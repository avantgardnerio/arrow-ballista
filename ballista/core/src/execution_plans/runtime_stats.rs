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

//! Passthrough operator that accumulates runtime statistics on the data
//! flowing through it. Two accessor families:
//!
//! - **Row count** — always tracked, per input partition. Backs Stage-2's
//!   exact-count-per-sub-partition slicing (SortExec doesn't expose runtime
//!   counts via `partition_statistics()` — that's still plan-time — so we
//!   compute the count ourselves as data streams past).
//! - **Quantile sketch** — optional (only when `order_by` is set at
//!   construction). Backs Stage-1's global-cut selection at the scheduler
//!   barrier.
//!
//! Timing is decoupled from correctness: both accessors are readable at any
//! point. Callers get whatever has flowed through so far — a mid-stream
//! snapshot for callers that decide the sample is accurate enough, a
//! post-drain snapshot for callers that want the full state (typical after
//! a blocking downstream like `SortExec` or `DamExec`). Per-partition
//! `Mutex`/`AtomicUsize` keeps writes off any shared lock and provides the
//! cross-thread memory-visibility barrier that reads need.
//!
//! The `order_by` accepts the full `Vec<PhysicalSortExpr>` so multi-key
//! `ORDER BY` survives serde (tie-breakers get preserved for downstream
//! `SortExec` / `BoundedWindowAggExec` even though only the first key drives
//! the sketch today).

use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_functions_aggregate_common::tdigest::TDigest;
use futures::stream::StreamExt;
use log::info;

/// T-Digest centroid budget. 100 is DataFusion's default and gives ~1%
/// quantile error, plenty of margin over the sub-partition counts (16-40 per
/// executor) we expect at bin-pack time. Bump if we start pushing sub-part
/// counts into the mid-hundreds.
const TDIGEST_MAX_SIZE: usize = 100;

/// Streaming runtime-stats operator on the parallel-window path. See
/// module-level docs.
pub struct RuntimeStatsExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY carried through from the wrapping window
    /// operator when the caller wants quantile sketching. `None` when only
    /// row counting is needed (e.g. Stage 2's use case).
    order_by: Option<Vec<PhysicalSortExpr>>,
    /// Always tracked, per input partition. Written via
    /// `AtomicUsize::fetch_add` on the hot path — no cross-partition
    /// contention, no lock overhead.
    row_counts: Arc<[AtomicUsize]>,
    /// Only allocated when `order_by` is `Some`. Sketches over the first
    /// ORDER BY expression's `Float64` values. `Mutex`-per-partition to
    /// keep writes off any shared lock.
    sketches: Option<Arc<[Mutex<TDigest>]>>,
    properties: Arc<PlanProperties>,
}

impl RuntimeStatsExec {
    /// Wrap `input`. If `order_by` is provided, its first entry drives the
    /// per-partition T-Digest; the full slice is preserved for serde and
    /// for downstream operators (`SortExec`, `BoundedWindowAggExec`) that
    /// need it. When `Some`, at least one expression is required — nothing
    /// to sketch on with an empty slice.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Option<Vec<PhysicalSortExpr>>,
    ) -> Result<Self> {
        if let Some(exprs) = &order_by {
            let [_first, ..] = exprs.as_slice() else {
                return internal_err!(
                    "RuntimeStatsExec: order_by is Some but empty; pass None to skip sketching"
                );
            };
        }
        let partition_count = input.output_partitioning().partition_count();
        let row_counts: Arc<[AtomicUsize]> = (0..partition_count)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into();
        let sketches: Option<Arc<[Mutex<TDigest>]>> = order_by.as_ref().map(|_| {
            (0..partition_count)
                .map(|_| Mutex::new(TDigest::new(TDIGEST_MAX_SIZE)))
                .collect::<Vec<_>>()
                .into()
        });
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            order_by,
            row_counts,
            sketches,
            properties,
        })
    }

    /// Full ORDER BY carried through, or `None` if the operator was built
    /// in row-count-only mode.
    pub fn order_by(&self) -> Option<&[PhysicalSortExpr]> {
        self.order_by.as_deref()
    }

    /// Rows observed on `partition` so far. Cheap `Relaxed` load — the
    /// value is a running counter, monotonically non-decreasing.
    ///
    /// Errors on out-of-range partition. Callers pass a partition id
    /// they've already used with `execute`.
    pub fn row_count(&self, partition: usize) -> Result<usize> {
        let counter = self.row_counts.get(partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                self.row_counts.len()
            )
        })?;
        Ok(counter.load(Ordering::Relaxed))
    }

    /// Rows observed across all partitions so far.
    pub fn total_row_count(&self) -> usize {
        self.row_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }

    /// Snapshot of one partition's running quantile sketch. Returns `None`
    /// when the operator was built in row-count-only mode (no `order_by`).
    /// Cheap clone (a `Vec<Centroid>` of size ≤ `TDIGEST_MAX_SIZE`).
    ///
    /// Errors if `partition` ≥ input's partition count — callers pass a
    /// partition id they've already used with `execute`.
    pub fn quantile_sketch(&self, partition: usize) -> Result<Option<TDigest>> {
        let Some(sketches) = &self.sketches else {
            return Ok(None);
        };
        let slot = sketches.get(partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                sketches.len()
            )
        })?;
        Ok(Some(
            slot.lock()
                .expect("RuntimeStats sketch mutex poisoned")
                .clone(),
        ))
    }

    /// All partitions merged into one sketch. `None` in row-count-only mode.
    pub fn merged_quantile_sketch(&self) -> Option<TDigest> {
        let sketches = self.sketches.as_ref()?;
        let snapshots: Vec<TDigest> = sketches
            .iter()
            .map(|m| {
                m.lock()
                    .expect("RuntimeStats sketch mutex poisoned")
                    .clone()
            })
            .collect();
        Some(TDigest::merge_digests(snapshots.iter()))
    }
}

impl Debug for RuntimeStatsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeStatsExec")
            .field("order_by", &self.order_by)
            .finish()
    }
}

impl DisplayAs for RuntimeStatsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.order_by {
            Some(exprs) => {
                let routing = &exprs[0];
                write!(
                    f,
                    "RuntimeStatsExec: rows + sketch(routing={} {})",
                    routing.expr,
                    if routing.options.descending {
                        "desc"
                    } else {
                        "asc"
                    }
                )
            }
            None => write!(f, "RuntimeStatsExec: rows"),
        }
    }
}

impl ExecutionPlan for RuntimeStatsExec {
    fn name(&self) -> &str {
        "RuntimeStatsExec"
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
                "RuntimeStatsExec expects exactly one child, got {}",
                children.len()
            );
        };
        // Fresh counters + sketches on rebuild — planning-time reshuffles
        // shouldn't carry stale sample state through the tree.
        Ok(Arc::new(RuntimeStatsExec::try_new(
            input.clone(),
            self.order_by.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.row_counts.len() {
            return internal_err!(
                "RuntimeStatsExec: partition {} out of range (have {})",
                partition,
                self.row_counts.len()
            );
        }
        let input_stream = self.input.execute(partition, ctx)?;
        let schema = self.schema();
        // Cloning `Arc`s so downstream operators observing the same state
        // share the counters/sketches. First ORDER BY expression, if any,
        // drives sketching.
        let row_counts = self.row_counts.clone();
        let sketches = self.sketches.clone();
        let routing_expr = self
            .order_by
            .as_ref()
            .and_then(|exprs| exprs.first())
            .map(|e| e.expr.clone());

        let state = StreamState {
            input: input_stream,
            row_counts,
            sketches,
            routing_expr,
            partition,
        };
        let out = futures::stream::unfold(state, |mut state| async move {
            let batch = state.input.next().await?;
            let forwarded = batch.and_then(|batch| {
                state.ingest(&batch)?;
                Ok(batch)
            });
            Some((forwarded, state))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Per-partition streaming state. Owns the input stream and the routing
/// expression; writes to its own row-count slot and (if sketching) its own
/// sketch slot.
struct StreamState {
    input: SendableRecordBatchStream,
    row_counts: Arc<[AtomicUsize]>,
    sketches: Option<Arc<[Mutex<TDigest>]>>,
    routing_expr: Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    partition: usize,
}

impl StreamState {
    /// Update per-partition counters and (if sketching) the digest.
    /// Returns `Err` on any failure path — evaluation error,
    /// materialisation error, or wrong result type — so the caller
    /// propagates to the output stream rather than emitting a batch the
    /// stats never observed.
    fn ingest(
        &mut self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<()> {
        // Row count is unconditional and cheap.
        let counter = self.row_counts.get(self.partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range (have {}) — \
                 execute() should have validated this",
                self.partition,
                self.row_counts.len()
            )
        })?;
        counter.fetch_add(batch.num_rows(), Ordering::Relaxed);

        // Sketch only when configured.
        let (Some(sketches), Some(routing_expr)) = (&self.sketches, &self.routing_expr)
        else {
            return Ok(());
        };
        let evaluated = routing_expr.evaluate(batch)?;
        let array = evaluated.into_array(batch.num_rows())?;
        let f64_arr = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "RuntimeStatsExec partition {}: routing expr produced {:?}, \
                 expected Float64 — detection rule should have refused this plan",
                    self.partition,
                    array.data_type()
                )
            })?;
        // NULL routing keys aren't sortable so they can't participate in
        // range partitioning; drop them from the sample set rather than
        // erroring.
        let values: Vec<f64> = f64_arr.iter().flatten().collect();
        if values.is_empty() {
            return Ok(());
        }
        let slot = sketches.get(self.partition).ok_or_else(|| {
            internal_datafusion_err!(
                "RuntimeStatsExec: partition {} out of range on sketch slot",
                self.partition
            )
        })?;
        let mut sketch = slot.lock().expect("RuntimeStats sketch mutex poisoned");
        *sketch = sketch.merge_unsorted_f64(values);
        Ok(())
    }
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // End-of-stream introspection until the operator learns to emit its
        // stats upstream. Log-and-skip if invariants somehow broke — panic
        // in Drop is a process-abort footgun.
        let Some(counter) = self.row_counts.get(self.partition) else {
            log::error!(
                "RuntimeStatsExec partition {} missing row-count slot on Drop \
                 (invariant broken by refactor?); skipping end-of-stream log",
                self.partition,
            );
            return;
        };
        let rows = counter.load(Ordering::Relaxed);
        if rows == 0 {
            return;
        }
        match self.sketches.as_ref().and_then(|s| s.get(self.partition)) {
            Some(slot) => {
                let sketch = slot.lock().expect("RuntimeStats sketch mutex poisoned");
                info!(
                    "RuntimeStatsExec partition {}: rows={} T-Digest count={} min={} max={}",
                    self.partition,
                    rows,
                    sketch.count(),
                    sketch.min(),
                    sketch.max(),
                );
            }
            None => {
                info!(
                    "RuntimeStatsExec partition {}: rows={}",
                    self.partition, rows
                );
            }
        }
    }
}
