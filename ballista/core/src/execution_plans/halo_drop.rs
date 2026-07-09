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

//! Drops "halo" rows after a window computation. Sits above BWAG at the
//! top of a parallel-window Stage 3, keeping only rows whose routing key
//! falls in this task's *primary* value range — the surrounding halo rows
//! were pulled in so BWAG's frame calculations saw them, but they belong
//! to another output partition and must not appear in the final output.
//!
//! # Why not `FilterExec`?
//!
//! The predicate has to be *invisible* to DataFusion's `PushDownFilter`
//! optimizer rule. A vanilla `FilterExec` (or a `FilterExec` + `__is_halo`
//! projection column) can be pushed down through Sort / SPM / BWAG — which
//! silently corrupts the query. Halo rows must reach BWAG so its frames
//! extend over them; dropping them before BWAG loses the whole reason we
//! fetched them in the first place.
//!
//! `HaloDropExec` is opaque by construction: it doesn't expose a
//! `PhysicalExpr` predicate accessor, doesn't implement any pushdown
//! trait, and doesn't participate in `FilterExec`'s optimizer surface.
//! The keep range lives as a plain data field the operator interprets
//! itself.
//!
//! # Modes
//!
//! - **[`KeepRange::Value`]** — value-based, half-open `[lo, hi_exclusive)`.
//!   Used for `RANGE`-frame windows. Only mode implemented today.
//! - **Rank** — row-index-based, `[lo_rank, hi_rank_exclusive)`. Needed
//!   for `ROWS`-frame windows. TODO; wires up once Barrier 2.5's row-count
//!   stats are plumbed in.
//!
//! # API surface
//!
//! Boundaries typed as [`ScalarValue`], not `f64` — the routing key type
//! is a widening axis. Impl downcasts to `Float64` internally today.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Float64Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{
    Result, ScalarValue, internal_datafusion_err, internal_err, plan_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::StreamExt;

/// Which rows a `HaloDropExec` keeps. Value-based for `RANGE` frames;
/// rank-based lands with ROWS-frame support.
#[derive(Debug, Clone)]
pub enum KeepRange {
    /// Half-open `[lo, hi_exclusive)` on the routing key's value. `lo` and
    /// `hi_exclusive` must have identical `DataType` matching what
    /// `routing_expr` evaluates to.
    Value {
        /// Inclusive lower bound.
        lo: ScalarValue,
        /// Exclusive upper bound. Must be strictly greater than `lo`.
        hi_exclusive: ScalarValue,
    },
    // TODO: Rank { lo: usize, hi_exclusive: usize } — for ROWS-frame
    // windows once Barrier 2.5's row-count stats are plumbed in.
}

/// Drops halo rows to keep only this task's primary output range.
/// See the module-level docs.
pub struct HaloDropExec {
    input: Arc<dyn ExecutionPlan>,
    routing_expr: Arc<dyn PhysicalExpr>,
    keep: KeepRange,
    properties: Arc<PlanProperties>,
}

impl HaloDropExec {
    /// Wrap `input`. `routing_expr` must evaluate to the same `DataType`
    /// as `keep`'s bounds (both `Float64` today). `keep`'s bounds must not
    /// be NULL and `hi_exclusive` must be strictly greater than `lo`.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        routing_expr: Arc<dyn PhysicalExpr>,
        keep: KeepRange,
    ) -> Result<Self> {
        let schema = input.schema();
        let routing_type = routing_expr.data_type(&schema)?;
        match &keep {
            KeepRange::Value { lo, hi_exclusive } => {
                if lo.is_null() || hi_exclusive.is_null() {
                    return internal_err!(
                        "HaloDropExec keep range bounds must not be NULL"
                    );
                }
                if lo.data_type() != hi_exclusive.data_type() {
                    return internal_err!(
                        "HaloDropExec keep range bounds must have the same DataType, \
                         got lo={:?}, hi_exclusive={:?}",
                        lo.data_type(),
                        hi_exclusive.data_type()
                    );
                }
                if lo.data_type() != routing_type {
                    return plan_err!(
                        "HaloDropExec keep range type {:?} does not match routing \
                         expression `{}`'s type {:?}",
                        lo.data_type(),
                        routing_expr,
                        routing_type
                    );
                }
                // Today's impl requires Float64. The API accepts any
                // ScalarValue so widening the impl doesn't break callers.
                if !matches!(routing_type, DataType::Float64) {
                    // TODO: support all continuous primitives
                    return internal_err!(
                        "HaloDropExec routing key `{}` must be Float64 for now, got {:?}",
                        routing_expr,
                        routing_type
                    );
                }
                let ScalarValue::Float64(Some(lo_f)) = lo else {
                    return internal_err!(
                        "HaloDropExec Float64 keep range but lo isn't Float64: {:?}",
                        lo
                    );
                };
                let ScalarValue::Float64(Some(hi_f)) = hi_exclusive else {
                    return internal_err!(
                        "HaloDropExec Float64 keep range but hi isn't Float64: {:?}",
                        hi_exclusive
                    );
                };
                if lo_f >= hi_f {
                    return internal_err!(
                        "HaloDropExec keep range must be non-empty: [lo={}, hi_exclusive={}) \
                         is empty or inverted",
                        lo_f,
                        hi_f
                    );
                }
            }
        }
        // Preserve input's PlanProperties: we're a row-filter, we don't
        // change the schema, the ordering, or the partitioning.
        let properties = Arc::clone(input.properties());
        Ok(Self {
            input,
            routing_expr,
            keep,
            properties,
        })
    }

    /// The expression whose value drives keep/drop decisions.
    pub fn routing_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.routing_expr
    }

    /// The keep range this operator applies.
    pub fn keep(&self) -> &KeepRange {
        &self.keep
    }
}

impl Debug for HaloDropExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("HaloDropExec")
            .field("routing_expr", &self.routing_expr)
            .field("keep", &self.keep)
            .finish()
    }
}

impl DisplayAs for HaloDropExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.keep {
            KeepRange::Value { lo, hi_exclusive } => write!(
                f,
                "HaloDropExec: keep={} ∈ [{}, {})",
                self.routing_expr, lo, hi_exclusive
            ),
        }
    }
}

impl ExecutionPlan for HaloDropExec {
    fn name(&self) -> &str {
        "HaloDropExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!(
                "HaloDropExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(HaloDropExec::try_new(
            input.clone(),
            self.routing_expr.clone(),
            self.keep.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, ctx)?;
        let routing_expr = self.routing_expr.clone();
        let keep = self.keep.clone();
        let schema = self.schema();
        let out = input_stream.map(move |batch_result| {
            let batch = batch_result?;
            apply_keep_range(&batch, &routing_expr, &keep)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Filter `batch` to just the rows whose routing key falls in `keep`.
///
/// TODO(perf): input is sorted (SPM upstream in Stage 3), so binary-search
/// for the `[lo, hi_exclusive)` slice boundaries within each batch and
/// return `batch.slice(start, len)` — zero-copy Arc bumps instead of the
/// per-row mask evaluation + `filter_record_batch` allocation. Same
/// optimisation family as the ordered scatter's TODO.
fn apply_keep_range(
    batch: &RecordBatch,
    routing_expr: &Arc<dyn PhysicalExpr>,
    keep: &KeepRange,
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let evaluated = routing_expr.evaluate(batch)?;
    let array = evaluated.into_array(batch.num_rows())?;
    let keys = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            internal_datafusion_err!(
                "HaloDropExec: routing expr produced {:?}, expected Float64",
                array.data_type()
            )
        })?;
    let KeepRange::Value { lo, hi_exclusive } = keep;
    // Constructor validated both are non-NULL Float64.
    let ScalarValue::Float64(Some(lo)) = lo else {
        return internal_err!(
            "HaloDropExec: keep.lo not a non-null Float64 at execute time"
        );
    };
    let ScalarValue::Float64(Some(hi)) = hi_exclusive else {
        return internal_err!(
            "HaloDropExec: keep.hi_exclusive not a non-null Float64 at execute time"
        );
    };
    // NULLs in the routing column can't satisfy the range comparison; they
    // land in the "drop" bucket via `is_null()` → false.
    let mask: BooleanArray = (0..batch.num_rows())
        .map(|row| {
            if keys.is_null(row) {
                Some(false)
            } else {
                let key = keys.value(row);
                Some(key >= *lo && key < *hi)
            }
        })
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    fn schema_v2_id() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v2", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn batch(schema: &Arc<Schema>, keys: Vec<Option<f64>>, ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(keys)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .unwrap()
    }

    fn mem_input(
        schema: &Arc<Schema>,
        rows: Vec<(Option<f64>, i64)>,
    ) -> Arc<dyn ExecutionPlan> {
        let (keys, ids): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        MemorySourceConfig::try_new_exec(
            &[vec![batch(schema, keys, ids)]],
            schema.clone(),
            None,
        )
        .unwrap()
    }

    fn session() -> Arc<SessionContext> {
        Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ))
    }

    async fn collect_ids(exec: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let ctx = session();
        let stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = <_ as futures::stream::StreamExt>::collect::<
            Vec<Result<RecordBatch>>,
        >(stream)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
        batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn value_range(lo: f64, hi: f64) -> KeepRange {
        KeepRange::Value {
            lo: ScalarValue::Float64(Some(lo)),
            hi_exclusive: ScalarValue::Float64(Some(hi)),
        }
    }

    // ---------- Constructor validation ----------------------------------

    fn empty_input(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap()
    }

    #[test]
    fn try_new_rejects_null_bound() {
        let schema = schema_v2_id();
        let err = HaloDropExec::try_new(
            empty_input(&schema),
            col("v2", schema.as_ref()).unwrap(),
            KeepRange::Value {
                lo: ScalarValue::Float64(None),
                hi_exclusive: ScalarValue::Float64(Some(10.0)),
            },
        )
        .expect_err("NULL lo bound must be rejected");
        assert!(err.to_string().contains("must not be NULL"), "got: {err}");
    }

    #[test]
    fn try_new_rejects_mismatched_bound_types() {
        let schema = schema_v2_id();
        let err = HaloDropExec::try_new(
            empty_input(&schema),
            col("v2", schema.as_ref()).unwrap(),
            KeepRange::Value {
                lo: ScalarValue::Float64(Some(0.0)),
                hi_exclusive: ScalarValue::Int64(Some(10)),
            },
        )
        .expect_err("mismatched bound types must be rejected");
        assert!(err.to_string().contains("same DataType"), "got: {err}");
    }

    #[test]
    fn try_new_rejects_bound_type_mismatch_with_routing() {
        let schema = schema_v2_id();
        // Routing on Int64 `id`, but bounds are Float64.
        let err = HaloDropExec::try_new(
            empty_input(&schema),
            col("id", schema.as_ref()).unwrap(),
            value_range(0.0, 10.0),
        )
        .expect_err("bound type must match routing expression type");
        assert!(
            err.to_string().contains("does not match routing"),
            "got: {err}"
        );
    }

    #[test]
    fn try_new_rejects_inverted_range() {
        let schema = schema_v2_id();
        let err = HaloDropExec::try_new(
            empty_input(&schema),
            col("v2", schema.as_ref()).unwrap(),
            value_range(10.0, 5.0),
        )
        .expect_err("inverted range must be rejected");
        assert!(err.to_string().contains("empty or inverted"), "got: {err}");
    }

    #[test]
    fn try_new_rejects_empty_range() {
        let schema = schema_v2_id();
        let err = HaloDropExec::try_new(
            empty_input(&schema),
            col("v2", schema.as_ref()).unwrap(),
            value_range(5.0, 5.0),
        )
        .expect_err("lo == hi_exclusive is empty");
        assert!(err.to_string().contains("empty or inverted"), "got: {err}");
    }

    // ---------- End-to-end filtering ------------------------------------

    #[tokio::test]
    async fn keeps_rows_in_range_drops_halo() {
        let schema = schema_v2_id();
        // Range [10, 20). Halo lo: 5, 8. Halo hi: 20, 25. Primary: 10, 15, 19.
        let input = mem_input(
            &schema,
            vec![
                (Some(5.0), 0),
                (Some(8.0), 1),
                (Some(10.0), 2),
                (Some(15.0), 3),
                (Some(19.0), 4),
                (Some(20.0), 5),
                (Some(25.0), 6),
            ],
        );
        let exec = Arc::new(
            HaloDropExec::try_new(
                input,
                col("v2", schema.as_ref()).unwrap(),
                value_range(10.0, 20.0),
            )
            .unwrap(),
        );
        let ids = collect_ids(exec).await;
        assert_eq!(ids, vec![2, 3, 4], "only primary-range ids should survive");
    }

    /// Half-open convention: `lo == 10` passes; `hi_exclusive == 20` drops.
    /// Matches DRR's boundary convention so an unbroken chain of half-open
    /// slices covers the whole line without gaps or overlaps.
    #[tokio::test]
    async fn boundary_is_half_open() {
        let schema = schema_v2_id();
        let input = mem_input(
            &schema,
            vec![
                (Some(9.999), 0),
                (Some(10.0), 1),
                (Some(19.999), 2),
                (Some(20.0), 3),
            ],
        );
        let exec = Arc::new(
            HaloDropExec::try_new(
                input,
                col("v2", schema.as_ref()).unwrap(),
                value_range(10.0, 20.0),
            )
            .unwrap(),
        );
        let ids = collect_ids(exec).await;
        assert_eq!(ids, vec![1, 2], "10.0 kept (inclusive), 20.0 dropped");
    }

    #[tokio::test]
    async fn null_routing_key_is_dropped() {
        let schema = schema_v2_id();
        let input = mem_input(
            &schema,
            vec![(None, 0), (Some(15.0), 1), (None, 2), (Some(50.0), 3)],
        );
        let exec = Arc::new(
            HaloDropExec::try_new(
                input,
                col("v2", schema.as_ref()).unwrap(),
                value_range(10.0, 20.0),
            )
            .unwrap(),
        );
        let ids = collect_ids(exec).await;
        assert_eq!(ids, vec![1], "NULLs and out-of-range dropped");
    }

    #[tokio::test]
    async fn empty_batch_survives() {
        let schema = schema_v2_id();
        let input = mem_input(&schema, vec![]);
        let exec = Arc::new(
            HaloDropExec::try_new(
                input,
                col("v2", schema.as_ref()).unwrap(),
                value_range(10.0, 20.0),
            )
            .unwrap(),
        );
        let ids = collect_ids(exec).await;
        assert!(ids.is_empty());
    }

    /// The operator's whole reason for existing: `PushDownFilter` must not
    /// crack it open. We don't test the optimizer directly here — that
    /// would drag in the whole planning pipeline — but we assert the
    /// structural property that makes us opaque: `HaloDropExec` doesn't
    /// downcast to `FilterExec` (which is what the pushdown rule looks
    /// for), and its keep range is stored as plain data, not a
    /// `PhysicalExpr` predicate accessor.
    #[test]
    fn opaque_to_filter_pushdown_by_construction() {
        use datafusion::physical_plan::filter::FilterExec;
        let schema = schema_v2_id();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            HaloDropExec::try_new(
                empty_input(&schema),
                col("v2", schema.as_ref()).unwrap(),
                value_range(10.0, 20.0),
            )
            .unwrap(),
        );
        assert!(
            exec.downcast_ref::<FilterExec>().is_none(),
            "HaloDropExec must not present as a FilterExec — that would \
             invite PushDownFilter to try moving the predicate downward"
        );
    }
}
