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

//! Range-partition marker for the parallel-window path. Passes batches
//! through unchanged today; upcoming commits grow it into a value-range
//! router that consumes a `T-Digest` from an upstream
//! [`SamplerExec`](crate::execution_plans::SamplerExec) via a child-tree
//! walk (see the design doc under
//! `docs/source/contributors-guide/parallel-window-kll-adaptive.md`).
//!
//! The operator's `try_new` API accepts the full `Vec<PhysicalSortExpr>`
//! from the wrapping window so multi-key `ORDER BY` and non-column
//! expressions survive plan round-trips.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// Range-partition marker. See the module-level docs.
pub struct DynamicRangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Lexicographic ORDER BY carried through from the wrapping window
    /// operator. `try_new` guarantees at least one element.
    order_by: Vec<PhysicalSortExpr>,
    properties: Arc<PlanProperties>,
}

impl DynamicRangeRepartitionExec {
    /// Wrap `input` in a range-partition marker. `order_by` must contain at
    /// least one expression — nothing to route on otherwise.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        order_by: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let [_first, ..] = order_by.as_slice() else {
            return internal_err!(
                "DynamicRangeRepartitionExec requires at least one ORDER BY expression"
            );
        };
        let properties = Arc::new(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            order_by,
            properties,
        })
    }

    /// Full ORDER BY carried through from the wrapping window operator.
    pub fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }
}

impl Debug for DynamicRangeRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicRangeRepartitionExec")
            .field("order_by", &self.order_by)
            .finish()
    }
}

impl DisplayAs for DynamicRangeRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let routing = &self.order_by[0];
        write!(
            f,
            "DynamicRangeRepartitionExec: routing={} {}",
            routing.expr,
            if routing.options.descending {
                "desc"
            } else {
                "asc"
            }
        )
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
            return internal_err!(
                "DynamicRangeRepartitionExec expects exactly one child, got {}",
                children.len()
            );
        };
        Ok(Arc::new(DynamicRangeRepartitionExec::try_new(
            input.clone(),
            self.order_by.clone(),
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
