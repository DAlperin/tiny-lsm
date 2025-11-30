use crate::lsm::LsmTree;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

/// A DataFusion TableProvider that wraps an LsmTree.
/// Schema: (key TEXT, value TEXT)
pub struct LsmTableProvider {
    tree: Arc<RwLock<LsmTree>>,
    schema: SchemaRef,
}

impl Debug for LsmTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTableProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

impl LsmTableProvider {
    pub fn new(tree: Arc<RwLock<LsmTree>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self { tree, schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for LsmTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract key bounds from filters
        let (lower_bound, upper_bound) = extract_key_bounds(filters);

        let projected_schema = project_schema(&self.schema, projection)?;

        Ok(Arc::new(LsmExecutionPlan::new(
            self.tree.clone(),
            self.schema.clone(),
            projected_schema,
            projection.cloned(),
            lower_bound,
            upper_bound,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let support = filters
            .iter()
            .map(|f| {
                if is_key_filter(f) {
                    // We can handle key filters exactly
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(support)
    }
}

/// Checks if an expression is a filter on the "key" column that we can push down.
fn is_key_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            let is_key_col = matches!(binary.left.as_ref(), Expr::Column(col) if col.name() == "key")
                || matches!(binary.right.as_ref(), Expr::Column(col) if col.name() == "key");

            let is_supported_op = matches!(
                binary.op,
                Operator::Eq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            );

            is_key_col && is_supported_op
        }
        _ => false,
    }
}

/// Extracts lower and upper bounds for the key from filter expressions.
/// Returns (lower_bound, upper_bound) where None means unbounded.
fn extract_key_bounds(filters: &[Expr]) -> (Option<String>, Option<String>) {
    let mut lower_bound: Option<String> = None;
    let mut upper_bound: Option<String> = None;

    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            let (col_on_left, col_name, literal) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(lit)) => (true, col.name(), lit),
                (Expr::Literal(lit), Expr::Column(col)) => (false, col.name(), lit),
                _ => continue,
            };

            if col_name != "key" {
                continue;
            }

            let value = match literal {
                ScalarValue::Utf8(Some(s)) => s.clone(),
                _ => continue,
            };

            // Normalize operator based on whether column was on left or right
            let op = if col_on_left {
                binary.op.clone()
            } else {
                // Flip the operator if column was on the right
                match binary.op {
                    Operator::Lt => Operator::Gt,
                    Operator::LtEq => Operator::GtEq,
                    Operator::Gt => Operator::Lt,
                    Operator::GtEq => Operator::LtEq,
                    other => other,
                }
            };

            match op {
                Operator::Eq => {
                    // key = 'value' means both bounds are the same
                    lower_bound = Some(value.clone());
                    upper_bound = Some(value);
                }
                Operator::Gt | Operator::GtEq => {
                    // key >= 'value' or key > 'value'
                    let new_lower = value;
                    lower_bound = match lower_bound {
                        None => Some(new_lower),
                        Some(existing) if new_lower > existing => Some(new_lower),
                        existing => existing,
                    };
                }
                Operator::Lt | Operator::LtEq => {
                    // key <= 'value' or key < 'value'
                    let new_upper = value;
                    upper_bound = match upper_bound {
                        None => Some(new_upper),
                        Some(existing) if new_upper < existing => Some(new_upper),
                        existing => existing,
                    };
                }
                _ => {}
            }
        }
    }

    (lower_bound, upper_bound)
}

/// ExecutionPlan for scanning the LSM tree.
#[derive(Debug)]
struct LsmExecutionPlan {
    tree: Arc<RwLock<LsmTree>>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    lower_bound: Option<String>,
    upper_bound: Option<String>,
    properties: PlanProperties,
}

impl LsmExecutionPlan {
    fn new(
        tree: Arc<RwLock<LsmTree>>,
        schema: SchemaRef,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        lower_bound: Option<String>,
        upper_bound: Option<String>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            tree,
            schema,
            projected_schema,
            projection,
            lower_bound,
            upper_bound,
            properties,
        }
    }
}

impl DisplayAs for LsmExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LsmExecutionPlan: ")?;
                if let Some(ref lb) = self.lower_bound {
                    write!(f, "lower_bound={}", lb)?;
                }
                if let Some(ref ub) = self.upper_bound {
                    write!(f, " upper_bound={}", ub)?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for LsmExecutionPlan {
    fn name(&self) -> &str {
        "LsmExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let tree = self.tree.read().unwrap();

        // Perform the scan with bounds
        let items: Vec<(String, String)> = tree
            .scan(
                self.lower_bound.as_deref(),
                self.upper_bound.as_deref(),
            )
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        // Build arrays
        let keys: Vec<&str> = items.iter().map(|(k, _)| k.as_str()).collect();
        let values: Vec<&str> = items.iter().map(|(_, v)| v.as_str()).collect();

        let key_array = Arc::new(StringArray::from(keys));
        let value_array = Arc::new(StringArray::from(values));

        // Apply projection
        let batch = match &self.projection {
            Some(indices) => {
                let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = indices
                    .iter()
                    .map(|&i| {
                        if i == 0 {
                            key_array.clone() as Arc<dyn datafusion::arrow::array::Array>
                        } else {
                            value_array.clone() as Arc<dyn datafusion::arrow::array::Array>
                        }
                    })
                    .collect();
                RecordBatch::try_new(self.projected_schema.clone(), columns)?
            }
            None => {
                RecordBatch::try_new(self.schema.clone(), vec![key_array, value_array])?
            }
        };

        let stream = futures::stream::once(async move { Ok(batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_basic_query() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));
        {
            let mut t = tree.write().unwrap();
            t.insert("a".to_string(), "1".to_string());
            t.insert("b".to_string(), "2".to_string());
            t.insert("c".to_string(), "3".to_string());
        }

        let ctx = SessionContext::new();
        ctx.register_table("kv", Arc::new(LsmTableProvider::new(tree.clone())))
            .unwrap();

        let df = ctx.sql("SELECT * FROM kv ORDER BY key").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "a");
        assert_eq!(keys.value(1), "b");
        assert_eq!(keys.value(2), "c");
    }

    #[tokio::test]
    async fn test_filter_pushdown() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));
        {
            let mut t = tree.write().unwrap();
            t.insert("a".to_string(), "1".to_string());
            t.insert("b".to_string(), "2".to_string());
            t.insert("c".to_string(), "3".to_string());
            t.insert("d".to_string(), "4".to_string());
        }

        let ctx = SessionContext::new();
        ctx.register_table("kv", Arc::new(LsmTableProvider::new(tree.clone())))
            .unwrap();

        // Test equality filter
        let df = ctx
            .sql("SELECT * FROM kv WHERE key = 'b'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);

        // Test range filter
        let df = ctx
            .sql("SELECT * FROM kv WHERE key >= 'b' AND key <= 'c'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_projection() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));
        {
            let mut t = tree.write().unwrap();
            t.insert("a".to_string(), "1".to_string());
        }

        let ctx = SessionContext::new();
        ctx.register_table("kv", Arc::new(LsmTableProvider::new(tree.clone())))
            .unwrap();

        let df = ctx.sql("SELECT value FROM kv").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_columns(), 1);
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "1");
    }
}
