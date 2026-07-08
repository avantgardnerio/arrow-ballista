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

//! Generic flow-control operator that DataFusion doesn't have (yet).
//! Buffers upstream batches with a policy-configurable release trigger,
//! then drains buffered rows and passes remaining input through.
//!
//! Currently supports one mode:
//!
//! - **`BufferMode::Dam`** — buffer input while a `MemoryReservation` grows
//!   against DataFusion's `MemoryPool`. Break the dam when `try_grow`
//!   refuses further allocation (any memory pressure — this operator, or
//!   contention from elsewhere in the query). No hardcoded byte threshold:
//!   the pool decides pressure, same mechanism [`SortExec`] uses to
//!   trigger spill (`sort.rs:887-907`).
//!
//! Growth path (intentionally not built today; documented so the shape
//! and future mode names are visible to reviewers):
//!
//! - `BufferMode::PassThrough` — noop, present so upstream rules can drop
//!   the operator without restructuring the plan.
//! - `BufferMode::BufferAll` — buffer until end-of-stream, then drain.
//! - `BufferMode::SpillOnPressure` — same as `Dam`, but writes to disk
//!   when the pool refuses rather than releasing to downstream. Fills the
//!   "generic materialise/spill primitive" gap in DataFusion.
//! - `BufferMode::StageBoundary` — the buffer IS a shuffle write.
//!
//! Only `Dam` is implemented for now. The other variants are named so
//! that when they land, they slot in without renaming or restructuring.
//!
//! [`SortExec`]: datafusion::physical_plan::sorts::sort::SortExec

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, StreamExt, TryStreamExt};

/// How the operator decides when to release buffered input.
///
/// Only `Dam` is implemented today; the other variants are documented
/// here (rather than only in the module docstring) so that when they
/// land, the growth path is visible right at the enum definition.
///
/// ```ignore
/// // Aspirational; NOT implemented today. Each is a natural evolution
/// // of the shape we already have — same operator, different release
/// // policy.
/// pub enum BufferMode {
///     Dam,                          // ← implemented
///
///     /// Pass batches through unchanged. Present so a rewrite rule can
///     /// disable the operator without restructuring the plan tree.
///     PassThrough,
///
///     /// Buffer until end-of-stream, then drain. Equivalent to what
///     /// `SortExec` does today but without the sort — the "generic
///     /// materialise" primitive DataFusion doesn't have.
///     BufferAll,
///
///     /// Same trigger as `Dam` (memory-pool pressure), but on
///     /// pressure spill buffered batches to disk instead of releasing
///     /// downstream. Fills the "generic spill" primitive gap; would
///     /// share plumbing with `SortExec::spill` machinery.
///     SpillOnPressure,
///
///     /// Buffer until an explicit row/byte threshold OR pool pressure,
///     /// then spill. The two-trigger variant of the above for callers
///     /// who want a hard cap on in-memory buffering time.
///     BufferAndSpill { max_rows: usize },
///
///     /// The buffer IS the shuffle write — writes batches to per-partition
///     /// shuffle files as they arrive. Would let this operator replace
///     /// `ShuffleWriterExec` in the appropriate positions.
///     StageBoundary,
/// }
/// ```
///
/// Ultimately this operator should land upstream in DataFusion so every
/// blocking operator (`SortExec`, `HashAggregateExec`, joins, ...) can
/// share the same materialise/spill primitive instead of each rolling
/// its own.
#[derive(Debug, Clone)]
pub enum BufferMode {
    /// Buffer batches while a `MemoryReservation` grows against
    /// DataFusion's `MemoryPool`. The dam breaks when `MemoryPool::try_grow`
    /// refuses further allocation (pressure from anywhere in the query).
    /// After the dam breaks, buffered batches drain in order and the
    /// remaining input passes through unchanged; the reservation is released
    /// as the buffer drops.
    ///
    /// No user-tunable byte threshold — the pool decides pressure,
    /// matching `SortExec::reserve_memory_for_batch_and_maybe_spill`.
    Dam,
}

/// Generic flow-control operator. See module-level docs.
pub struct BufferExec {
    input: Arc<dyn ExecutionPlan>,
    mode: BufferMode,
    properties: Arc<PlanProperties>,
}

impl BufferExec {
    /// Wrap `input` with the given buffering `mode`.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, mode: BufferMode) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            mode,
            properties,
        })
    }

    /// The configured buffering policy.
    pub fn mode(&self) -> &BufferMode {
        &self.mode
    }
}

impl Debug for BufferExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferExec")
            .field("mode", &self.mode)
            .finish()
    }
}

impl DisplayAs for BufferExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.mode {
            BufferMode::Dam => write!(f, "BufferExec: mode=Dam"),
        }
    }
}

impl ExecutionPlan for BufferExec {
    fn name(&self) -> &str {
        "BufferExec"
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
                "BufferExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(BufferExec::try_new(
            input.clone(),
            self.mode.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input_stream = self.input.execute(partition, ctx.clone())?;
        match &self.mode {
            BufferMode::Dam => {
                // Register a per-partition consumer against the runtime's
                // memory pool. Same shape SortExec uses; participates in
                // whatever pool policy is configured (Greedy, FairSpill,
                // Unbounded, ...) without picking numbers ourselves.
                let reservation = MemoryConsumer::new(format!(
                    "BufferExec::Dam[partition={partition}]"
                ))
                .register(&ctx.runtime_env().memory_pool);
                let out = build_dam_stream(input_stream, reservation);
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
            }
        }
    }
}

/// Build the async stream that implements `BufferMode::Dam`.
///
/// Semantics (mirrors `SortExec::reserve_memory_for_batch_and_maybe_spill`):
/// 1. For each incoming batch, `reservation.try_grow(batch.get_array_memory_size())`.
///    - `Ok`: append to buffer.
///    - `Err` and buffer empty: propagate the OOM (nothing to release).
///    - `Err` and buffer non-empty: **dam breaks**; drain buffer, then
///      passthrough. Reservation is dropped as the buffer empties.
/// 2. On stream-end without dam-break: drain buffer, done.
fn build_dam_stream(
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    futures::stream::once(async move {
        let mut buffered: Vec<RecordBatch> = Vec::new();
        let mut dam_broken = false;

        // Fill phase: read and buffer until pool pressure or end-of-stream.
        while let Some(batch) = input.next().await {
            let batch = batch?;
            let size = batch.get_array_memory_size();
            match reservation.try_grow(size) {
                Ok(_) => buffered.push(batch),
                Err(err) => {
                    if buffered.is_empty() {
                        return Err(oom_context(err));
                    }
                    // Dam breaks: hand the (already-pulled) batch back so it
                    // rides through with the rest, then hand the rest of the
                    // input to the passthrough tail.
                    buffered.push(batch);
                    dam_broken = true;
                    break;
                }
            }
        }

        // Drain buffered → then passthrough remainder if dam broke early.
        // `stream::iter` yields owned batches, releasing reservation via
        // Drop once `buffered` is fully consumed. `.boxed()` unifies types.
        let drained =
            futures::stream::iter(buffered.into_iter().map(Ok::<_, DataFusionError>));
        let out: futures::stream::BoxStream<'static, Result<RecordBatch>> = if dam_broken
        {
            drained.chain(input).boxed()
        } else {
            drained.boxed()
        };
        // Drop the reservation explicitly at the end of the fill phase —
        // downstream can allocate freely from here on.
        drop(reservation);
        Ok::<_, DataFusionError>(out)
    })
    .try_flatten()
}

/// Wrap an OOM error with the same context message SortExec uses so the
/// operator surfaces uniform "tune your memory limit" advice.
fn oom_context(e: DataFusionError) -> DataFusionError {
    match e {
        DataFusionError::ResourcesExhausted(_) => e.context(
            "Not enough memory to buffer a single batch in BufferExec::Dam. \
             Consider increasing 'datafusion.runtime.memory_limit'.",
        ),
        _ => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;

    fn one_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batches(rows_per: usize, num_batches: usize) -> Vec<RecordBatch> {
        (0..num_batches)
            .map(|b| {
                let start = (b * rows_per) as i64;
                let array = Int64Array::from_iter_values(start..start + rows_per as i64);
                RecordBatch::try_new(one_col_schema(), vec![Arc::new(array)]).unwrap()
            })
            .collect()
    }

    fn memory_source(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = one_col_schema();
        let src =
            MemorySourceConfig::try_new(&[batches], schema, None).expect("mem source");
        Arc::new(DataSourceExec::new(Arc::new(src)))
    }

    /// Runtime context whose memory pool has a hard `pool_bytes` limit. The
    /// dam is triggered by pool refusal, so a tight pool is how we make
    /// tests exercise the dam-break branch without hardcoded thresholds.
    fn ctx_with_pool_limit(pool_bytes: usize) -> Arc<TaskContext> {
        let pool = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("runtime");
        let cfg = SessionConfig::new();
        let session = SessionContext::new_with_config_rt(cfg, runtime);
        session.task_ctx()
    }

    async fn collect_rows(
        plan: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<usize> {
        let stream = plan.execute(0, ctx)?;
        let out: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(out.iter().map(|b| b.num_rows()).sum())
    }

    /// Input fits within the pool → everything buffers, drains at
    /// end-of-stream. No rows lost.
    #[tokio::test]
    async fn dam_drains_full_buffer_when_pool_has_headroom() -> Result<()> {
        let src = memory_source(batches(100, 10));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        // 10 MB pool — trivially fits ~800 bytes per batch × 10.
        let ctx = ctx_with_pool_limit(10 * 1024 * 1024);
        assert_eq!(collect_rows(Arc::new(dam), ctx).await?, 1000);
        Ok(())
    }

    /// Pool tight enough that reservation growth fails partway through the
    /// stream → dam breaks, buffered rows plus the remainder all pass
    /// through. No rows lost, none duplicated.
    #[tokio::test]
    async fn dam_breaks_on_pool_pressure_and_passes_remainder() -> Result<()> {
        // 200 rows/batch of Int64 ≈ 1.6 KB payload per batch, but arrow's
        // reported memory footprint per batch here is ~1-2 KB. A ~5 KB pool
        // buffers 2-3 batches, then try_grow refuses.
        let src = memory_source(batches(200, 50));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        let ctx = ctx_with_pool_limit(5 * 1024);
        assert_eq!(collect_rows(Arc::new(dam), ctx).await?, 200 * 50);
        Ok(())
    }

    /// Pool so tight that even the first batch's reservation fails →
    /// operator surfaces OOM (mirrors SortExec behaviour when there's
    /// nothing already buffered that could be spilled/released).
    #[tokio::test]
    async fn dam_propagates_oom_when_first_batch_wont_fit() -> Result<()> {
        let src = memory_source(batches(1000, 5));
        let dam = BufferExec::try_new(src, BufferMode::Dam)?;
        // 1-byte pool — no batch can be reserved.
        let ctx = ctx_with_pool_limit(1);
        let result = collect_rows(Arc::new(dam), ctx).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("BufferExec::Dam") || e.to_string().contains("memory")),
            "expected OOM, got {result:?}"
        );
        Ok(())
    }
}
