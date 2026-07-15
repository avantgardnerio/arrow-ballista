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
//! Restriction is *scoped*: a leaf below a collapse (`CoalescePartitionsExec`,
//! `SortPreservingMergeExec`, or a `SinglePartition`-requiring join build side)
//! must read the entire upstream, not the task's slice — otherwise each of N
//! sibling tasks would collapse only 1/N of the input, producing partial
//! results downstream tries to merge (e.g. the wrong scalar threshold from a
//! HAVING subquery). The rewriter tracks these scopes on a stack so
//! leaves know which mode to apply.

use ballista_core::execution_plans::ShuffleReaderExec;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use log::warn;
use std::any::Any;
use std::sync::Arc;

/// Restrict `plan` so that its leaves only see the given `partitions`, unless
/// a leaf sits below a collapse operator (`CoalescePartitionsExec`,
/// `SortPreservingMergeExec`, or a `SinglePartition`-requiring join build
/// side) — those leaves keep the full upstream so the collapse sees every
/// partition.
pub fn restrict_plan_to_partitions(
    plan: Arc<dyn ExecutionPlan>,
    partitions: &[usize],
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut rewriter = TaskPlanRewriter {
        partitions,
        scopes: vec![],
    };
    Ok(plan.rewrite(&mut rewriter)?.data)
}

/// Descent-time scope for `TaskPlanRewriter`.
///
/// The rewriter walks the plan top-down. When it enters an operator that
/// requires its subtree to produce all partitions (a collapse), it pushes a
/// `Collect` frame. Each leaf pops the current scope: `Collect` means read the
/// entire upstream; `None` means restrict to the task's `partitions`.
#[derive(Debug, Clone, Copy)]
enum Scope {
    /// A collapse operator ancestor demands every upstream partition. Leaves
    /// under this scope skip restriction.
    Collect,
}

struct TaskPlanRewriter<'a> {
    partitions: &'a [usize],
    scopes: Vec<Scope>,
}

impl TreeNodeRewriter for TaskPlanRewriter<'_> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        // Leaves consume the current scope and restrict (or not) accordingly.
        if let Some(rewritten) = self.rewrite_shuffle_reader(&node) {
            return Ok(Transformed::yes(rewritten));
        }
        if let Some(rewritten) = self.rewrite_scan(&node) {
            return Ok(Transformed::yes(rewritten));
        }

        // Interior operators push a scope for their subtree when they require
        // all upstream partitions in one place.
        if node.is::<CoalescePartitionsExec>() || node.is::<SortPreservingMergeExec>() {
            // A nested Coalesce/SPM inside an existing `Collect` scope is a
            // no-op — the outer scope already forces "read everything".
            if !matches!(self.scopes.last(), Some(Scope::Collect)) {
                self.scopes.push(Scope::Collect);
            }
            return Ok(Transformed::no(node));
        }

        if node.is::<HashJoinExec>() || node.is::<NestedLoopJoinExec>() {
            // Push a `Collect` per input that requires a single partition
            // (typically a broadcast build side). Handles plans where the
            // build side collapses to 1 without an explicit
            // CoalescePartitionsExec above the reader.
            for dist in node.required_input_distribution() {
                if matches!(dist, Distribution::SinglePartition) {
                    self.scopes.push(Scope::Collect);
                }
            }
            return Ok(Transformed::no(node));
        }

        Ok(Transformed::no(node))
    }
}

impl TaskPlanRewriter<'_> {
    /// Restrict a `ShuffleReaderExec` so only the assigned `partitions` remain
    /// in its `partition` vec. Output_partitioning shrinks to
    /// `partitions.len()` but **preserves the partitioning kind** — a
    /// `Hash([col], N)` reader becomes `Hash([col], kept.len())`, not
    /// `UnknownPartitioning`. This matters for operators like `InterleaveExec`
    /// above the reader that assert children share a hash partitioning to fuse
    /// safely; downgrading to `UnknownPartitioning` breaks that assertion.
    fn rewrite_shuffle_reader(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let reader = plan.downcast_ref::<ShuffleReaderExec>()?;
        // Broadcast readers serve everything from partition[0] regardless of
        // task; leave them intact.
        if reader.broadcast {
            self.scopes.pop();
            return None;
        }
        let kept: Vec<Vec<_>> = match self.scopes.pop() {
            // Under a collapse (Coalesce / SPM / single-partition join
            // build): every task must see the full upstream so the collapse
            // aggregates the complete partial-result set.
            Some(Scope::Collect) => reader.partition.clone(),
            None => self
                .partitions
                .iter()
                .filter_map(|&p| reader.partition.get(p).cloned())
                .collect(),
        };
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
        let restricted = ShuffleReaderExec::try_new(
            reader.stage_id,
            kept,
            reader.schema(),
            partitioning,
        )
        .ok()?;
        Some(Arc::new(restricted))
    }

    /// Restrict a `DataSourceExec` (file-backed or in-memory) so only the
    /// assigned `partitions` remain, or take all groups if under a `Collect`
    /// scope. `output_partitioning().partition_count()` shrinks to
    /// `partitions.len()` — matching what `rewrite_shuffle_reader` does — so
    /// position `i` in the restricted plan corresponds to the task's
    /// `global_input_partition_ids[i]` globally.
    fn rewrite_scan(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let exec = plan.downcast_ref::<DataSourceExec>()?;
        let scope = self.scopes.pop();
        let source: &dyn Any = exec.data_source().as_ref();
        if let Some(config) = source.downcast_ref::<FileScanConfig>() {
            let file_groups: Vec<FileGroup> = match scope {
                Some(Scope::Collect) => return None,
                None => self
                    .partitions
                    .iter()
                    .filter_map(|&i| config.file_groups.get(i).cloned())
                    .collect(),
            };
            let restricted = FileScanConfigBuilder::from(config.clone())
                .with_file_groups(file_groups)
                .build();
            return Some(DataSourceExec::from_data_source(restricted));
        }
        if let Some(config) = source.downcast_ref::<MemorySourceConfig>() {
            let kept: Vec<Vec<_>> = match scope {
                Some(Scope::Collect) => return None,
                None => self
                    .partitions
                    .iter()
                    .filter_map(|&i| config.partitions().get(i).cloned())
                    .collect(),
            };
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
}
