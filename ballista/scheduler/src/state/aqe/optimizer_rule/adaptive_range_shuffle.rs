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

//! AQE rule that rewrites `Partial → Hash exchange → FinalPartitioned` into a
//! post-shuffle range-filter shape.
//!
//! # Slice 1a1 — detect and log only
//!
//! Today this rule walks the plan tree looking for the target pattern and
//! logs a `debug!` line for every match. It does **not** modify the plan.
//! The rewrite itself (URRE + Passthrough writer + BufferExec::Dam +
//! RuntimeStatsExec, and the paired stage-N+1 range filter on the read
//! side) is added in a follow-up.
//!
//! # Pattern
//!
//! ```text
//! AggregateExec { mode: FinalPartitioned }
//!   ExchangeExec { partitioning: Some(Hash(exprs, K)) }
//!     AggregateExec { mode: Partial }
//! ```
//!
//! Strict shape for now — we can generalize later. Full design in
//! `dev-notes/adaptive-range-shuffle/lineitem-agg.md`.
//!
//! # Gating
//!
//! `ballista.optimizer.adaptive_range_shuffle.enabled` (default `false`).
//! When off the rule short-circuits at the first line of `optimize()`.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use log::debug;

use crate::state::aqe::execution_plan::ExchangeExec;

/// AQE rule that (for now) detects and logs the `Partial → Hash → Final`
/// shape without modifying the plan. See module docs.
#[derive(Debug, Default)]
pub struct AdaptiveRangeShuffleRule;

impl PhysicalOptimizerRule for AdaptiveRangeShuffleRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let bc = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if !bc.adaptive_range_shuffle_enabled() {
            return Ok(plan);
        }

        debug!(
            "[adaptive-range-shuffle] rule fires; scanning plan for \
             Partial → Hash → FinalPartitioned"
        );

        // Walk the whole plan. For every node, check if it's the target
        // shape; log if so. Continue the walk in every direction — the
        // pattern may appear multiple times (e.g. Q20 at SF100+ has three
        // Hash exchanges, only some of which are inside a Partial→Final
        // aggregation subtree).
        let mut match_count: usize = 0;
        plan.apply(|node| {
            if let Some(m) = detect_match(node) {
                debug!(
                    "[adaptive-range-shuffle] would rewrite: plan_id={} \
                     Hash cols={:?} K={} partial_group_by_len={} \
                     final_group_by_len={}",
                    m.exchange_plan_id,
                    m.hash_exprs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>(),
                    m.k,
                    m.partial_group_by_len,
                    m.final_group_by_len,
                );
                match_count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        if match_count == 0 {
            debug!("[adaptive-range-shuffle] no matches in this plan");
        } else {
            debug!(
                "[adaptive-range-shuffle] {match_count} match(es) detected \
                 (rewrite not yet implemented; plan flows through unchanged)"
            );
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "AdaptiveRangeShuffleRule"
    }

    fn schema_check(&self) -> bool {
        // We don't modify the plan yet, so the schema can't change.
        true
    }
}

/// A detected instance of the target shape. Captured as data so we can log
/// it and (later) drive the rewrite.
struct Match {
    exchange_plan_id: usize,
    hash_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    k: usize,
    partial_group_by_len: usize,
    final_group_by_len: usize,
}

/// If `node` is an `AggregateExec::FinalPartitioned` whose sole child is an
/// `ExchangeExec` with `Partitioning::Hash(exprs, K)` whose sole child is
/// an `AggregateExec::Partial`, return the captured details. Otherwise
/// return `None`.
fn detect_match(node: &Arc<dyn ExecutionPlan>) -> Option<Match> {
    let final_agg = node.downcast_ref::<AggregateExec>()?;
    if *final_agg.mode() != AggregateMode::FinalPartitioned {
        return None;
    }

    let exchange = final_agg.input().downcast_ref::<ExchangeExec>()?;
    // Must be a Hash-partitioned exchange (the "badness" we want to remove).
    let (hash_exprs, k) = match &exchange.partitioning {
        Some(Partitioning::Hash(exprs, k)) => (exprs.clone(), k),
        _ => return None,
    };

    let partial_agg = exchange.input().downcast_ref::<AggregateExec>()?;
    if *partial_agg.mode() != AggregateMode::Partial {
        return None;
    }

    Some(Match {
        exchange_plan_id: exchange.plan_id,
        hash_exprs,
        k: *k,
        partial_group_by_len: partial_agg.group_expr().expr().len(),
        final_group_by_len: final_agg.group_expr().expr().len(),
    })
}
