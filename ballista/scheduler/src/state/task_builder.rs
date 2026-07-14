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

//! Per-task plan rewriter.
//!
//! Before shipping a stage's plan to an executor, the scheduler restricts the
//! plan's leaves (Scan file groups, ShuffleReader partition locations) to the
//! task's assigned partition slice. The executor then just runs whatever it's
//! given — no operator needs to know its task's global identity, and any
//! within-stage operator's `output_partitioning()` naturally reflects the
//! restricted count.
//!
//! Contrast with the previous shape, where the executor called
//! `restrict_scan_to_partition(plan, partition_id)` at execution time — that
//! only worked for the "1 task = 1 partition" model, and forced the executor
//! to know which of the K it was. This module moves the restrict to the
//! scheduler and generalises to arbitrary partition slices (contiguous today,
//! but the shape supports halo overlap and other non-contiguous assignments
//! future stages will need).

use ballista_core::execution_plans::ShuffleReaderExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use log::warn;
use std::any::Any;
use std::sync::Arc;

/// Restrict `plan` so that its leaves only see the given `partitions`. Every
/// operator above the leaves picks up the restricted count naturally via
/// `output_partitioning()`.
pub fn restrict_plan_to_partitions(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<Arc<dyn ExecutionPlan>> {
    let out = plan.transform_down(|node| {
        if let Some(rewritten) = restrict_scan(&node, partitions) {
            return Ok(Transformed::yes(rewritten));
        }
        if let Some(rewritten) = restrict_shuffle_reader(&node, partitions) {
            return Ok(Transformed::yes(rewritten));
        }
        Ok(Transformed::no(node))
    })?;
    Ok(out.data)
}

/// Restrict a `DataSourceExec` (file-backed or in-memory) so only the
/// assigned `partitions` remain. `output_partitioning().partition_count()`
/// shrinks to `partitions.len()` — matching what `restrict_shuffle_reader`
/// already does — so position `i` in the restricted plan corresponds to the
/// task's `partition_slice[i]` globally.
fn restrict_scan(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = plan.downcast_ref::<DataSourceExec>()?;
    let source: &dyn Any = exec.data_source().as_ref();
    if let Some(config) = source.downcast_ref::<FileScanConfig>() {
        let file_groups: Vec<FileGroup> = partitions
            .iter()
            .filter_map(|&i| config.file_groups.get(i).cloned())
            .collect();
        let restricted = FileScanConfigBuilder::from(config.clone())
            .with_file_groups(file_groups)
            .build();
        return Some(DataSourceExec::from_data_source(restricted));
    }
    if let Some(config) = source.downcast_ref::<MemorySourceConfig>() {
        let kept: Vec<Vec<_>> = partitions
            .iter()
            .filter_map(|&i| config.partitions().get(i).cloned())
            .collect();
        let restricted = MemorySourceConfig::try_new(
            &kept,
            config.original_schema(),
            config.projection().clone(),
        )
        .ok()?;
        return Some(DataSourceExec::from_data_source(restricted));
    }
    warn!(
        "restrict_plan_to_partitions: unrecognised DataSourceExec source \
         left unrestricted; if it distributes work from a shared queue, \
         tasks would over-read"
    );
    None
}

/// Restrict a `ShuffleReaderExec` so only the assigned `partitions` remain in
/// its `partition` vec. Output_partitioning shrinks to `partitions.len()` but
/// **preserves the partitioning kind** — a `Hash([col], N)` reader becomes
/// `Hash([col], kept.len())`, not `UnknownPartitioning`. This matters for
/// operators like `InterleaveExec` above the reader that assert children
/// share a hash partitioning to fuse safely; downgrading to
/// `UnknownPartitioning` breaks that assertion.
fn restrict_shuffle_reader(
    plan: &Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Option<Arc<dyn ExecutionPlan>> {
    let reader = plan.downcast_ref::<ShuffleReaderExec>()?;
    // Broadcast readers serve everything from partition[0] regardless of slot;
    // don't restrict.
    if reader.broadcast {
        return None;
    }
    let kept: Vec<Vec<_>> = partitions
        .iter()
        .filter_map(|&p| reader.partition.get(p).cloned())
        .collect();
    let partitioning = match reader.properties().output_partitioning() {
        datafusion::physical_plan::Partitioning::Hash(exprs, _) => {
            datafusion::physical_plan::Partitioning::Hash(exprs.clone(), kept.len())
        }
        datafusion::physical_plan::Partitioning::RoundRobinBatch(_) => {
            datafusion::physical_plan::Partitioning::RoundRobinBatch(kept.len())
        }
        datafusion::physical_plan::Partitioning::UnknownPartitioning(_) => {
            datafusion::physical_plan::Partitioning::UnknownPartitioning(kept.len())
        }
    };
    let restricted =
        ShuffleReaderExec::try_new(reader.stage_id, kept, reader.schema(), partitioning)
            .ok()?;
    Some(Arc::new(restricted))
}
