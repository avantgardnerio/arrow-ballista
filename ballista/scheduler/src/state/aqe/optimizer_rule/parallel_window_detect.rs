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

//! Detection-only rule: log when the plan contains a bounded RANGE-frame
//! window that could be parallelized by inserting a `RangeRepartitionExec`
//! above the window's input.
//!
//! No plan mutation happens here — the rule returns the input plan
//! unchanged. The pitch is completion-under-memory-pressure via range
//! partitioning, so the detection logs are a scaffold for later wiring;
//! they answer *does this query shape reach us?* before we spend effort on
//! the `RangeRepartitionExec` port.
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
//!   - **Bound scalar types we can't natively bucket.** Detection accepts
//!     any non-null scalar today, but the eventual rewrite will need KLL
//!     sketches (or similar streaming quantile estimators) to compute
//!     range boundaries when the order key isn't natively rangeable at
//!     plan time.

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use log::info;
use std::sync::Arc;

/// Physical optimizer pass that logs every parallelizable window it sees
/// and returns the plan unchanged.
#[derive(Default, Debug)]
pub struct ParallelWindowDetectRule;

impl PhysicalOptimizerRule for ParallelWindowDetectRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        for shape in detect_candidates(&plan)? {
            info!(
                "ParallelWindow: candidate detected — order-by=`{}`, frame=RANGE BETWEEN {} AND {}",
                shape.order_key,
                fmt_bound(&shape.start_bound),
                fmt_bound(&shape.end_bound),
            );
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "ParallelWindowDetect"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Shape captured from a matching `BoundedWindowAggExec`. Everything the
/// rewrite will eventually need to plant a `RangeRepartitionExec` is here.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowCandidate {
    pub order_key: String,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

/// Walk `plan` and return one entry per `BoundedWindowAggExec` that matches
/// the parallel-window shape.
pub fn detect_candidates(
    plan: &Arc<dyn ExecutionPlan>,
) -> datafusion::common::Result<Vec<WindowCandidate>> {
    let mut hits = Vec::new();
    plan.apply(|node| {
        if let Some(shape) = as_candidate(node.as_ref()) {
            hits.push(shape);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(hits)
}

fn as_candidate(node: &dyn ExecutionPlan) -> Option<WindowCandidate> {
    let window = node.downcast_ref::<BoundedWindowAggExec>()?;
    // Shape gates as slice patterns: 0 or 2+ elements simply don't match.
    // No cross-line invariants for the reader to hold in their head.
    let [expr] = window.window_expr() else {
        return None;
    };
    let [] = expr.partition_by() else {
        return None;
    };
    let [order] = expr.order_by() else {
        return None;
    };
    let column = order.expr.downcast_ref::<Column>()?;
    let frame = expr.get_window_frame();
    let WindowFrameUnits::Range = frame.units else {
        return None;
    };
    let (Some(start), Some(end)) =
        (as_finite(&frame.start_bound), as_finite(&frame.end_bound))
    else {
        return None;
    };
    Some(WindowCandidate {
        order_key: column.name().to_string(),
        start_bound: start.clone(),
        end_bound: end.clone(),
    })
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

    // Q8 shape — RANGE frame on a double order key with an Int64 bound.
    // The old Int64-only detection rejected this; this rule must accept.
    #[tokio::test]
    async fn detects_range_frame_double_orderby_int_bound()
    -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        let candidates = detect_candidates(&plan)?;
        assert_eq!(
            candidates.len(),
            1,
            "expected one candidate, got {candidates:?}"
        );
        assert_eq!(candidates[0].order_key, "v2");
        Ok(())
    }

    // ROWS frames are out of scope for this pass.
    #[tokio::test]
    async fn rejects_rows_frame() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT avg(v2) OVER (ORDER BY id3 \
                ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert!(detect_candidates(&plan)?.is_empty());
        Ok(())
    }

    // PARTITION BY takes the query off the single-partition parallel path.
    #[tokio::test]
    async fn rejects_partition_by() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (PARTITION BY id1 ORDER BY v2 \
                RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert!(detect_candidates(&plan)?.is_empty());
        Ok(())
    }

    // UNBOUNDED PRECEDING means the halo needs the whole prefix — that's the
    // CarryExec / prefix-scan path, not the RangeRepartition path.
    #[tokio::test]
    async fn rejects_unbounded_preceding() -> datafusion::common::Result<()> {
        let plan = plan(
            "SELECT sum(v2) OVER (ORDER BY v2 \
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM large",
        )
        .await?;
        assert!(detect_candidates(&plan)?.is_empty());
        Ok(())
    }

    // No ORDER BY → no range partitioning to do.
    #[tokio::test]
    async fn rejects_no_order_by() -> datafusion::common::Result<()> {
        let plan = plan("SELECT sum(v2) OVER () FROM large").await?;
        assert!(detect_candidates(&plan)?.is_empty());
        Ok(())
    }
}
