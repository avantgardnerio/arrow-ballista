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

//! Quantile-sketch operator for the parallel-window path. Streams input
//! batches through unchanged while accumulating a quantile sketch (T-Digest
//! today, KLL later) over the first `ORDER BY` expression's `Float64`
//! values. The sketch is task-local and reachable via
//! [`QuantileSketchExec::quantile_sketch`]; downstream operators inside the
//! same Ballista task (typically a `DynamicRangeRepartitionExec` above a
//! blocking `SortExec`) call the accessor after their upstream has drained,
//! consuming a finalised distribution summary.
//!
//! Timing is decoupled from correctness: sketches are readable at any point.
//! Callers get whatever has flowed through so far — a mid-stream snapshot for
//! callers that decide the sample is accurate enough, a post-drain snapshot
//! for callers that want the whole distribution (typical after a blocking
//! `SortExec`). One `Mutex` per partition keeps writes off any shared lock
//! and provides the cross-thread memory-visibility barrier that reads need.
//!
//! The API accepts the full `Vec<PhysicalSortExpr>` so multi-key `ORDER BY`
//! survives serde (tie-breakers get preserved for downstream `SortExec` /
//! `BoundedWindowAggExec` even though only the first key drives the sketch).

use std::fmt::{self, Debug, Formatter};
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

/// Streaming quantile-sketch tap on the parallel-window path. See
/// module-level docs.
pub struct QuantileSketchExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY carried through from the wrapping window
    /// operator. `try_new` guarantees at least one element; only the first
    /// drives the sketch today.
    order_by: Vec<PhysicalSortExpr>,
    /// One sketch per input partition, indexed by partition id. Every
    /// batch write hits only its partition's mutex — no cross-partition
    /// contention on the hot path. Reads happen once (post-drain) and
    /// merge across partitions via T-Digest's associative combine.
    /// Task-scoped by construction: each Ballista task decodes its own
    /// operator instance, so this slice never leaks across tasks.
    sketches: Arc<[Mutex<TDigest>]>,
    properties: Arc<PlanProperties>,
}

impl QuantileSketchExec {
    /// Wrap `input` in a quantile-sketch tap over the first entry of
    /// `order_by`. `order_by` must contain at least one expression —
    /// nothing to sketch on otherwise.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let [_first, ..] = order_by.as_slice() else {
            return internal_err!(
                "QuantileSketchExec requires at least one ORDER BY expression"
            );
        };
        let partition_count = input.output_partitioning().partition_count();
        let sketches: Arc<[Mutex<TDigest>]> = (0..partition_count)
            .map(|_| Mutex::new(TDigest::new(TDIGEST_MAX_SIZE)))
            .collect::<Vec<_>>()
            .into();
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            order_by,
            sketches,
            properties,
        })
    }

    /// Full ORDER BY carried through from the wrapping window operator.
    pub fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    /// Snapshot of one partition's running quantile sketch. Cheap clone (a
    /// `Vec<Centroid>` of size ≤ `TDIGEST_MAX_SIZE`).
    ///
    /// Readable at any point in the stream's lifetime — modular by design.
    /// A mid-stream snapshot returns the sketch as it stands, biased toward
    /// whichever rows have flowed through so far; a post-drain snapshot is
    /// the full distribution for that partition. Callers decide what
    /// accuracy they need.
    ///
    /// Returns `TDigest` today; will migrate to `Box<dyn QuantileSketch>`
    /// when the sketch backend becomes pluggable (KLL, GK, q-digest).
    ///
    /// Errors if `partition` ≥ input's partition count — callers pass a
    /// partition id they've already used with `execute`, so out-of-range
    /// is a programming error surfaced explicitly.
    pub fn quantile_sketch(&self, partition: usize) -> Result<TDigest> {
        let slot = self.sketches.get(partition).ok_or_else(|| {
            internal_datafusion_err!(
                "QuantileSketchExec: partition {} out of range (have {})",
                partition,
                self.sketches.len()
            )
        })?;
        Ok(slot.lock().expect("QuantileSketch mutex poisoned").clone())
    }

    /// All partitions merged into one sketch via T-Digest's associative
    /// combine. Cheap: one mutex acquire per partition, then a linear
    /// merge over ≤ `TDIGEST_MAX_SIZE` × `partition_count` centroids.
    ///
    /// Same "readable at any point" semantic as [`Self::quantile_sketch`] —
    /// the caller decides when the sketches are ready enough to be useful.
    pub fn merged_quantile_sketch(&self) -> TDigest {
        let snapshots: Vec<TDigest> = self
            .sketches
            .iter()
            .map(|m| m.lock().expect("QuantileSketch mutex poisoned").clone())
            .collect();
        TDigest::merge_digests(snapshots.iter())
    }
}

impl Debug for QuantileSketchExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuantileSketchExec")
            .field("order_by", &self.order_by)
            .finish()
    }
}

impl DisplayAs for QuantileSketchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let routing = &self.order_by[0];
        write!(
            f,
            "QuantileSketchExec: routing={} {}",
            routing.expr,
            if routing.options.descending {
                "desc"
            } else {
                "asc"
            }
        )
    }
}

impl ExecutionPlan for QuantileSketchExec {
    fn name(&self) -> &str {
        "QuantileSketchExec"
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
                "QuantileSketchExec expects exactly one child, got {}",
                children.len()
            );
        };
        // Fresh sketch on rebuild — planning-time reshuffles shouldn't
        // carry stale sample state through the tree.
        Ok(Arc::new(QuantileSketchExec::try_new(
            input.clone(),
            self.order_by.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.sketches.len() {
            return internal_err!(
                "QuantileSketchExec: partition {} out of range (have {})",
                partition,
                self.sketches.len()
            );
        }
        let input_stream = self.input.execute(partition, ctx)?;
        let schema = self.schema();
        // Clone the `Arc` so the stream's writer and downstream readers
        // (via `quantile_sketch`) both see the same per-partition slots.
        let sketches = self.sketches.clone();
        // `try_new` guarantees `order_by[0]` exists.
        let routing_expr = self.order_by[0].expr.clone();

        let state = StreamState {
            input: input_stream,
            sketches,
            routing_expr,
            partition,
        };
        // Ingest is expected to succeed on every batch — an evaluation, cast,
        // or type failure means the plan is malformed (detection rule bug) or
        // the runtime state is broken; either way we surface the error rather
        // than silently emitting a batch whose values never reached the sketch.
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
/// expression; holds an `Arc` to the operator's per-partition sketch
/// slots and writes only to its own slot on the hot path.
struct StreamState {
    input: SendableRecordBatchStream,
    sketches: Arc<[Mutex<TDigest>]>,
    routing_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    partition: usize,
}

impl StreamState {
    /// Evaluate the routing expression on `batch` and merge its non-null
    /// `Float64` values into this partition's sketch. Returns `Err` on any
    /// failure path — evaluation error, materialisation error, or wrong
    /// result type; the caller propagates to the output stream so callers
    /// see the failure rather than downstream getting a batch the sketch
    /// never observed.
    fn ingest(
        &mut self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<()> {
        let evaluated = self.routing_expr.evaluate(batch)?;
        let array = evaluated.into_array(batch.num_rows())?;
        let f64_arr = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "QuantileSketchExec partition {}: routing expr produced {:?}, \
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
        // Only this partition's slot is touched — no cross-partition
        // contention. Reads from downstream (`quantile_sketch(partition)`)
        // are the only other users of this specific mutex. `execute`
        // validated `self.partition` before we got here; the `ok_or_else`
        // is defense in depth for a future refactor, and it stays on the
        // Result path rather than jumping to a panic.
        let slot = self.sketches.get(self.partition).ok_or_else(|| {
            internal_datafusion_err!(
                "QuantileSketchExec: partition {} out of range (have {}) — \
                 execute() should have validated this",
                self.partition,
                self.sketches.len()
            )
        })?;
        let mut sketch = slot.lock().expect("QuantileSketch mutex poisoned");
        *sketch = sketch.merge_unsorted_f64(values);
        Ok(())
    }
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // End-of-stream introspection until the operator learns to emit its
        // sketch upstream. Only worth logging if this partition contributed
        // rows — an empty input isn't a bug, just quiet.
        let Some(slot) = self.sketches.get(self.partition) else {
            log::error!(
                "QuantileSketchExec partition {} missing sketch slot on Drop \
                 (invariant broken by refactor?); skipping end-of-stream log",
                self.partition,
            );
            return;
        };
        let sketch = slot.lock().expect("QuantileSketch mutex poisoned");
        if sketch.count() > 0.0 {
            info!(
                "QuantileSketchExec partition {}: T-Digest \
                 count={} min={} max={}",
                self.partition,
                sketch.count(),
                sketch.min(),
                sketch.max(),
            );
        }
    }
}
