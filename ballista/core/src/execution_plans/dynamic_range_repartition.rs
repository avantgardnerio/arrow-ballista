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

//! Marker operator for range partitioning inserted by `ParallelWindowDetect`.
//!
//! Currently a pass-through: it forwards its input unchanged, mirroring the
//! input's schema and partitioning. Ships as a no-op so the optimizer rule
//! that inserts it can be exercised end-to-end without touching execution
//! semantics. Follow-up commits will grow this operator into a T-Digest
//! sampler + value-range router.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// See the module-level docs for the eventual shape; today this is a
/// pass-through marker inserted by the parallel-window optimizer rule.
pub struct DynamicRangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DynamicRangeRepartitionExec {
    /// Wrap `input` in a pass-through range-repartition marker.
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self { input, properties })
    }
}

impl Debug for DynamicRangeRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicRangeRepartitionExec").finish()
    }
}

impl DisplayAs for DynamicRangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DynamicRangeRepartitionExec: passthrough")
    }
}

impl ExecutionPlan for DynamicRangeRepartitionExec {
    fn name(&self) -> &str {
        "DynamicRangeRepartitionExec"
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
            return datafusion::common::internal_err!(
                "DynamicRangeRepartitionExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(DynamicRangeRepartitionExec::try_new(
            input.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, ctx)
    }
}
