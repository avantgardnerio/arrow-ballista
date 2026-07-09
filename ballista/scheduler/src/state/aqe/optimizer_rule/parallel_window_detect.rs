// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rule that wraps every parallelizable window's input in a
//! `UnorderedRangeRepartitionExec` marker. The marker is a pass-through today;
//! upcoming commits grow it into a T-Digest sampler + value-range router.
//!
//! The pitch is completion-under-memory-pressure via range partitioning.
//! Shipping the pass-through first proves the wrapper lands in the right plan
//! slot before touching execution semantics.
//!
//! # TODO — widen detection as the rewrite lands
//!   - **Multi-column `ORDER BY`.** Today we require a single sort key so
//!     the range-partition boundary is one-dimensional. Composite keys
//!     need a lexicographic bucketing strategy in the rewrite.
//!   - **Computed order-by expressions.** Today we require a physical
//!     `Column`. Widening this needs the rewrite to know how to sample
//!     the computed value at range-boundary time.
//!   - **`ROWS` bounds.** Today we only match `RANGE`. Bounded `ROWS`
//!     frames are also parallelizable via row-count halos (see Q6
//!     `avg(...) OVER (ORDER BY id3 ROWS BETWEEN 100 PRECEDING AND
//!     CURRENT ROW)`), but the halo unit is rows not order-key deltas so
//!     the coordinator is a different calculation.
//!   - **Non-Float64 routing keys.** Currently gated to `Float64` because
//!     DataFusion's built-in T-Digest is `f64`-only. Widening waits on a
//!     DIY generic-over-`Ord` KLL sketch we intend to build later.

use ballista_core::execution_plans::UnorderedRangeRepartitionExec;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use log::info;
use std::sync::Arc;

/// Physical optimizer pass that wraps every parallelizable window's input
/// in a `UnorderedRangeRepartitionExec` marker. That operator is a pass-through
/// today; upcoming commits grow it into a T-Digest sampler + value-range
/// router. Inserting the pass-through first lets us prove the rewrite lands
/// in the right plan slot before touching execution semantics.
#[derive(Default, Debug)]
pub struct ParallelWindowDetectRule;

impl PhysicalOptimizerRule for ParallelWindowDetectRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let out = plan.transform_down(|node| {
            let Some(bwag) = as_candidate(node.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            info!(
                "ParallelWindow: wrapping BWAG input — order-by=`{}`, frame=RANGE BETWEEN {} AND {}",
                bwag.order_key,
                fmt_bound(&bwag.start_bound),
                fmt_bound(&bwag.end_bound),
            );
            let children = node.children();
            let [bwag_input] = children.as_slice() else {
                return datafusion::common::internal_err!(
                    "BoundedWindowAggExec must have exactly one child, got {}",
                    children.len()
                );
            };
            let wrapped: Arc<dyn ExecutionPlan> =
                Arc::new(UnorderedRangeRepartitionExec::try_new(
                    (*bwag_input).clone(),
                    bwag.order_by.clone(),
                )?);
            let new_bwag = node.with_new_children(vec![wrapped])?;
            // Jump past the rewritten node's children — the BWAG we just
            // reconstructed still matches `as_candidate` by shape, and
            // recursing would re-wrap forever.
            Ok(Transformed::new(new_bwag, true, TreeNodeRecursion::Jump))
        })?;
        Ok(out.data)
    }

    fn name(&self) -> &str {
        "ParallelWindowDetect"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Fields extracted from a matching `BoundedWindowAggExec` — everything the
/// rewrite will eventually need to plant a `UnorderedRangeRepartitionExec`.
/// Carries the full lexicographic ORDER BY (not just the routing key name)
/// so the wrapping operator can accept it verbatim; today the sampler only
/// uses the first entry, but the API is shape-general.
#[derive(Debug, Clone)]
struct BwagCandidate {
    order_key: String,
    order_by: Vec<datafusion::physical_expr::PhysicalSortExpr>,
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
}

fn as_candidate(node: &dyn ExecutionPlan) -> Option<BwagCandidate> {
    let bwag = node.downcast_ref::<BoundedWindowAggExec>()?;
    // AQE re-runs the optimizer chain on every stage-boundary replan. If we
    // already wrapped this BWAG's subtree in an earlier pass, skip — otherwise
    // we nest another marker per replan and the plan grows one layer per
    // iteration, driving AQE into a livelock instead of converging.
    if subtree_has_marker(bwag.input().as_ref()) {
        return None;
    }
    // Shape gates as slice patterns: 0 or 2+ elements simply don't match.
    // No cross-line invariants for the reader to hold in their head.
    let [window_expr] = bwag.window_expr() else {
        return None;
    };
    let [] = window_expr.partition_by() else {
        return None;
    };
    let [order_by_expr] = window_expr.order_by() else {
        return None;
    };
    let order_column = order_by_expr.expr.downcast_ref::<Column>()?;
    // Rewrite only applies when the routing key is Float64 — that's what DF's
    // T-Digest speaks. Other scalar types wait for the DIY generic-KLL follow-up.
    let input_schema = bwag.input().schema();
    let DataType::Float64 = input_schema.field(order_column.index()).data_type() else {
        return None;
    };
    let frame = window_expr.get_window_frame();
    let WindowFrameUnits::Range = frame.units else {
        return None;
    };
    let (Some(start), Some(end)) =
        (as_finite(&frame.start_bound), as_finite(&frame.end_bound))
    else {
        return None;
    };
    Some(BwagCandidate {
        order_key: order_column.name().to_string(),
        order_by: window_expr.order_by().to_vec(),
        start_bound: start.clone(),
        end_bound: end.clone(),
    })
}

/// True when `plan` or any of its descendants is a
/// `UnorderedRangeRepartitionExec`. Cheap in-process walk; the plan tree is
/// small compared to the data volume it describes.
fn subtree_has_marker(plan: &dyn ExecutionPlan) -> bool {
    if plan
        .downcast_ref::<UnorderedRangeRepartitionExec>()
        .is_some()
    {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| subtree_has_marker(child.as_ref()))
}

/// Returns the bound unchanged when it's `CurrentRow` or a non-null scalar
/// offset. `UNBOUNDED PRECEDING/FOLLOWING` is represented as a typed-null
/// scalar and returns `None`.
fn as_finite(bound: &WindowFrameBound) -> Option<&WindowFrameBound> {
    match bound {
        WindowFrameBound::CurrentRow => Some(bound),
        WindowFrameBound::Preceding(scalar) | WindowFrameBound::Following(scalar)
            if !scalar.is_null() =>
        {
            Some(bound)
        }
        _ => None,
    }
}

fn fmt_bound(bound: &WindowFrameBound) -> String {
    match bound {
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::Preceding(scalar) => format!("{scalar} PRECEDING"),
        WindowFrameBound::Following(scalar) => format!("{scalar} FOLLOWING"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::execution_plans::UnorderedRangeRepartitionExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    async fn plan(sql: &str) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id1", DataType::Int64, false),
            Field::new("id2", DataType::Int64, false),
            Field::new("id3", DataType::Int64, false),
            Field::new("v2", DataType::Float64, false),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("large", Arc::new(EmptyTable::new(schema)))?;
        ctx.sql(sql).await?.create_physical_plan().await
    }

    /// Runs the rule on `sql` and returns the number of
    /// `UnorderedRangeRepartitionExec` nodes it inserted. `1` means the target
    /// shape matched and got wrapped; `0` means the rule left the plan alone.
    async fn count_wraps(sql: &str) -> datafusion::common::Result<usize> {
        let plan = plan(sql).await?;
        let rewritten = ParallelWindowDetectRule.optimize(plan, &ConfigOptions::new())?;
        let mut wraps = 0;
        rewritten.apply(|node| {
            if node
                .downcast_ref::<UnorderedRangeRepartitionExec>()
                .is_some()
            {
                wraps += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(wraps)
    }

    // Q8 shape — RANGE frame on a double order key with an Int64 bound.
    // The old Int64-only detection rejected this; this rule must wrap.
    #[tokio::test]
    async fn wraps_range_frame_double_orderby_int_bound() -> datafusion::common::Result<()>
    {
        let wraps = count_wraps(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert_eq!(wraps, 1);
        Ok(())
    }

    // ROWS frames are out of scope for this pass.
    #[tokio::test]
    async fn rejects_rows_frame() -> datafusion::common::Result<()> {
        let wraps = count_wraps(
            "SELECT avg(v2) OVER (ORDER BY id3 \
                ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert_eq!(wraps, 0);
        Ok(())
    }

    // PARTITION BY takes the query off the single-partition parallel path.
    #[tokio::test]
    async fn rejects_partition_by() -> datafusion::common::Result<()> {
        let wraps = count_wraps(
            "SELECT sum(v2) OVER (PARTITION BY id1 ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert_eq!(wraps, 0);
        Ok(())
    }

    // UNBOUNDED PRECEDING means the halo needs the whole prefix — that's the
    // CarryExec / prefix-scan path, not the RangeRepartition path.
    #[tokio::test]
    async fn rejects_unbounded_preceding() -> datafusion::common::Result<()> {
        let wraps = count_wraps(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert_eq!(wraps, 0);
        Ok(())
    }

    // No ORDER BY → no range partitioning to do.
    #[tokio::test]
    async fn rejects_no_order_by() -> datafusion::common::Result<()> {
        let wraps = count_wraps("SELECT sum(v2) OVER () FROM large").await?;
        assert_eq!(wraps, 0);
        Ok(())
    }

    // Rewrite only applies to Float64 routing keys (T-Digest limitation).
    // id3 is Int64 in the test schema, so this must not match even though
    // the frame + shape are otherwise valid.
    #[tokio::test]
    async fn rejects_non_float64_order_by() -> datafusion::common::Result<()> {
        let wraps = count_wraps(
            "SELECT sum(v2) OVER (ORDER BY id3 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert_eq!(wraps, 0);
        Ok(())
    }

    // AQE re-runs the optimizer chain on every stage boundary. If the rule
    // isn't idempotent, each replan nests another marker → the plan grows
    // one layer per iteration and AQE never converges (real regression we
    // hit against a live cluster before this test existed).
    #[tokio::test]
    async fn rule_is_idempotent_across_reruns() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let config = ConfigOptions::new();
        let once = ParallelWindowDetectRule.optimize(plan, &config)?;
        let count_after_once = count_markers(&once);
        let twice = ParallelWindowDetectRule.optimize(once, &config)?;
        let count_after_twice = count_markers(&twice);
        assert_eq!(
            count_after_once, 1,
            "first pass should insert exactly one marker"
        );
        assert_eq!(
            count_after_twice, 1,
            "second pass must not add another marker"
        );
        Ok(())
    }

    fn count_markers(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let mut count = 0;
        plan.apply(|node| {
            if node
                .downcast_ref::<UnorderedRangeRepartitionExec>()
                .is_some()
            {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("apply is infallible for pure counting closure");
        count
    }

    // Positional check: the wrap lands directly under BWAG, not somewhere
    // else in the tree.
    #[tokio::test]
    async fn wraps_land_directly_under_bwag() -> datafusion::common::Result<()> {
        use datafusion::physical_plan::windows::BoundedWindowAggExec;

        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let rewritten = ParallelWindowDetectRule.optimize(plan, &ConfigOptions::new())?;
        let mut found = false;
        rewritten.apply(|node| {
            if node.downcast_ref::<BoundedWindowAggExec>().is_some() {
                let children = node.children();
                let [bwag_input] = children.as_slice() else {
                    unreachable!("BWAG has exactly one child")
                };
                if bwag_input
                    .downcast_ref::<UnorderedRangeRepartitionExec>()
                    .is_some()
                {
                    found = true;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(
            found,
            "BWAG's direct child must be UnorderedRangeRepartitionExec"
        );
        Ok(())
    }
}
