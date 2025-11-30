use crate::codec::{decode_key, decode_row, decode_row_projected, encode_key, encode_row};
use crate::lsm::LsmTree;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

/// A DataFusion TableProvider that wraps an LsmTree with an arbitrary schema.
///
/// The table uses:
/// - Order-preserving key encoding for primary key columns
/// - MessagePack serialization for row values
pub struct LsmTableProvider {
    tree: Arc<RwLock<LsmTree>>,
    schema: SchemaRef,
    /// The table ID used as a key prefix (allows multiple tables in one LSM tree)
    table_id: u32,
    /// Indices of columns that form the primary key
    pk_indices: Vec<usize>,
}

impl Debug for LsmTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsmTableProvider")
            .field("schema", &self.schema)
            .field("table_id", &self.table_id)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl LsmTableProvider {
    /// Creates a new LsmTableProvider.
    ///
    /// # Arguments
    /// * `tree` - The underlying LSM tree storage
    /// * `schema` - The table schema
    /// * `table_id` - Unique identifier for this table (used as key prefix)
    /// * `pk_indices` - Indices of columns that form the primary key
    pub fn new(
        tree: Arc<RwLock<LsmTree>>,
        schema: SchemaRef,
        table_id: u32,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            tree,
            schema,
            table_id,
            pk_indices,
        }
    }

    /// Creates a simple key-value table provider with schema (key TEXT, value TEXT).
    #[cfg(test)]
    pub fn new_kv(tree: Arc<RwLock<LsmTree>>) -> Self {
        use datafusion::arrow::datatypes::Field;

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self::new(tree, schema, 0, vec![0])
    }

    /// Inserts a row into the table.
    pub fn insert(&self, values: &[ScalarValue]) -> Result<()> {
        // Extract primary key values
        let pk_values: Vec<ScalarValue> =
            self.pk_indices.iter().map(|&i| values[i].clone()).collect();

        // Encode key and value
        let key = encode_key(self.table_id, &pk_values);
        let value = encode_row(values);

        // Insert into LSM tree
        let mut tree = self.tree.write().unwrap();
        tree.insert(key, value);

        Ok(())
    }

    /// Prints the underlying LSM tree structure with decoded row values.
    pub fn debug_dump(&self) {
        println!("{}", self.format_debug());
    }

    /// Returns a formatted string showing the LSM tree with decoded row values.
    pub fn format_debug(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();

        let tree = self.tree.read().unwrap();
        let table_prefix = self.table_id.to_be_bytes();

        writeln!(output, "=== LsmTableProvider Debug ===").unwrap();
        writeln!(output, "Table ID: {}", self.table_id).unwrap();
        writeln!(
            output,
            "Schema: {:?}",
            self.schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        )
        .unwrap();
        writeln!(
            output,
            "Primary Key Columns: {:?}",
            self.pk_indices
                .iter()
                .map(|&i| self.schema.field(i).name())
                .collect::<Vec<_>>()
        )
        .unwrap();
        writeln!(output).unwrap();

        // Format function that decodes MessagePack values
        let schema = self.schema.clone();
        let format_row = |key: &[u8], value: &[u8]| -> String {
            // Check if this key belongs to our table
            if key.len() < 4 || key[..4] != table_prefix {
                return format!("[other table] key={:02x?}", key);
            }

            // Decode the key (skip table prefix, show remaining bytes)
            let key_rest = &key[4..];
            let pk_types: Vec<&DataType> = self
                .pk_indices
                .iter()
                .map(|&i| self.schema.field(i).data_type())
                .collect();
            let key_display = format_key_bytes(key_rest, &pk_types);

            // Decode the MessagePack value
            let row_display = match decode_row_to_strings(value, &schema) {
                Ok(values) => {
                    let pairs: Vec<String> = schema
                        .fields()
                        .iter()
                        .zip(values.iter())
                        .map(|(f, v)| format!("{}: {}", f.name(), v))
                        .collect();
                    format!("{{ {} }}", pairs.join(", "))
                }
                Err(e) => format!("[decode error: {}]", e),
            };

            format!("{} => {}", key_display, row_display)
        };

        // Print tree structure
        writeln!(output, "Memtable ({} entries):", tree.memtable_len()).unwrap();

        // Use the tree's display_with_formatter through Debug
        struct FormattedTree<'a, F> {
            tree: &'a LsmTree,
            formatter: F,
        }
        impl<'a, F: Fn(&[u8], &[u8]) -> String> std::fmt::Debug for FormattedTree<'a, F> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.tree.display_with_formatter(f, &self.formatter)
            }
        }

        write!(
            output,
            "{:?}",
            FormattedTree {
                tree: &tree,
                formatter: format_row
            }
        )
        .unwrap();

        output
    }
}

/// Formats key bytes for display using the known PK column types.
fn format_key_bytes(bytes: &[u8], pk_types: &[&DataType]) -> String {
    let values = decode_key(bytes, pk_types);
    let formatted: Vec<String> = values.iter().map(format_scalar_value).collect();
    format!("key({})", formatted.join(", "))
}

fn format_scalar_value(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "NULL".to_string(),
        ScalarValue::Boolean(b) => format_option(b, |x| x.to_string()),
        ScalarValue::Int8(i) => format_option(i, |x| x.to_string()),
        ScalarValue::Int16(i) => format_option(i, |x| x.to_string()),
        ScalarValue::Int32(i) => format_option(i, |x| x.to_string()),
        ScalarValue::Int64(i) => format_option(i, |x| x.to_string()),
        ScalarValue::UInt8(i) => format_option(i, |x| x.to_string()),
        ScalarValue::UInt16(i) => format_option(i, |x| x.to_string()),
        ScalarValue::UInt32(i) => format_option(i, |x| x.to_string()),
        ScalarValue::UInt64(i) => format_option(i, |x| x.to_string()),
        ScalarValue::Float32(f) => format_option(f, |x| format!("{:.2}", x)),
        ScalarValue::Float64(f) => format_option(f, |x| format!("{:.2}", x)),
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
            format_option(s, |x| format!("\"{}\"", x))
        }
        ScalarValue::Binary(b) | ScalarValue::LargeBinary(b) => format_option(b, |x| {
            format!(
                "0x{}",
                x.iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>()
            )
        }),
        other => format!("{:?}", other),
    }
}

fn format_option<T, F>(opt: &Option<T>, formatter: F) -> String
where
    F: FnOnce(&T) -> String,
{
    opt.as_ref()
        .map(formatter)
        .unwrap_or_else(|| "NULL".to_string())
}

/// Decodes a MessagePack-encoded row to string representations.
fn decode_row_to_strings(bytes: &[u8], schema: &Schema) -> Result<Vec<String>, String> {
    use crate::codec::format_msgpack_value;

    let value = rmpv::decode::read_value(&mut &bytes[..]).map_err(|e| e.to_string())?;

    match value {
        rmpv::Value::Array(values) => Ok(values
            .iter()
            .zip(schema.fields())
            .map(|(v, _field)| format_msgpack_value(v))
            .collect()),
        _ => Err("Expected array in MessagePack data".to_string()),
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
        // Extract key bounds from filters on primary key columns
        let (lower_bound, upper_bound) =
            extract_pk_bounds(filters, &self.schema, &self.pk_indices, self.table_id);

        let projected_schema = project_schema(&self.schema, projection)?;

        Ok(Arc::new(LsmExecutionPlan::new(
            self.tree.clone(),
            self.schema.clone(),
            projected_schema,
            projection.cloned(),
            self.table_id,
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
                if is_pk_filter(f, &self.schema, &self.pk_indices) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(support)
    }
}

/// Checks if an expression is a filter on a primary key column.
fn is_pk_filter(expr: &Expr, schema: &Schema, pk_indices: &[usize]) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            let col_name = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(_)) => Some(col.name()),
                (Expr::Literal(_), Expr::Column(col)) => Some(col.name()),
                _ => None,
            };

            if let Some(name) = col_name {
                // Check if this column is part of the primary key
                if let Ok(idx) = schema.index_of(name) {
                    if pk_indices.contains(&idx) {
                        return matches!(
                            binary.op,
                            Operator::Eq
                                | Operator::Lt
                                | Operator::LtEq
                                | Operator::Gt
                                | Operator::GtEq
                        );
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Extracts lower and upper key bounds from filter expressions on primary key columns.
fn extract_pk_bounds(
    filters: &[Expr],
    schema: &Schema,
    pk_indices: &[usize],
    table_id: u32,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    // For simplicity, we only handle filters on the first PK column for now
    // A complete implementation would handle composite PKs
    if pk_indices.is_empty() {
        return (None, None);
    }

    let first_pk_idx = pk_indices[0];
    let first_pk_name = schema.field(first_pk_idx).name();

    let mut lower_bound: Option<ScalarValue> = None;
    let mut upper_bound: Option<ScalarValue> = None;

    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            let (col_on_left, col_name, literal) =
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(col), Expr::Literal(lit)) => (true, col.name(), lit),
                    (Expr::Literal(lit), Expr::Column(col)) => (false, col.name(), lit),
                    _ => continue,
                };

            if col_name != first_pk_name {
                continue;
            }

            // Normalize operator based on whether column was on left or right
            let op = if col_on_left {
                binary.op.clone()
            } else {
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
                    lower_bound = Some(literal.clone());
                    upper_bound = Some(literal.clone());
                }
                Operator::Gt | Operator::GtEq => {
                    lower_bound = Some(literal.clone());
                }
                Operator::Lt | Operator::LtEq => {
                    upper_bound = Some(literal.clone());
                }
                _ => {}
            }
        }
    }

    let lower = lower_bound.map(|v| encode_key(table_id, &[v]));
    let upper = upper_bound.map(|v| encode_key(table_id, &[v]));

    (lower, upper)
}

/// ExecutionPlan for scanning the LSM tree with arbitrary schema.
#[derive(Debug)]
struct LsmExecutionPlan {
    tree: Arc<RwLock<LsmTree>>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    table_id: u32,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    properties: PlanProperties,
}

impl LsmExecutionPlan {
    fn new(
        tree: Arc<RwLock<LsmTree>>,
        schema: SchemaRef,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        table_id: u32,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
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
            table_id,
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
                write!(f, "LsmExecutionPlan: table_id={}", self.table_id)?;
                if self.lower_bound.is_some() {
                    write!(f, " lower_bound=<encoded>")?;
                }
                if self.upper_bound.is_some() {
                    write!(f, " upper_bound=<encoded>")?;
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
        vec![]
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

        // Create table prefix for filtering
        let table_prefix = self.table_id.to_be_bytes();

        // Perform the scan with bounds
        let items: Vec<Vec<ScalarValue>> = tree
            .scan(self.lower_bound.as_deref(), self.upper_bound.as_deref())
            .filter(|(k, _)| {
                // Filter to only this table's keys
                k.len() >= 4 && k[..4] == table_prefix
            })
            .map(|(_, v)| {
                if let Some(ref indices) = self.projection {
                    decode_row_projected(v, &self.schema, indices)
                } else {
                    decode_row(v, &self.schema)
                }
            })
            .collect();

        // Build Arrow arrays from the decoded values
        let batch = build_record_batch(&self.projected_schema, items)?;

        let schema = self.projected_schema.clone();
        let stream = futures::stream::once(async move { Ok(batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Builds a RecordBatch from a vector of rows (each row is a Vec<ScalarValue>).
fn build_record_batch(schema: &SchemaRef, rows: Vec<Vec<ScalarValue>>) -> Result<RecordBatch> {
    use datafusion::arrow::array::*;

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let num_cols = schema.fields().len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = schema.field(col_idx);
        let values: Vec<&ScalarValue> = rows.iter().map(|row| &row[col_idx]).collect();

        let array: ArrayRef = match field.data_type() {
            DataType::Boolean => {
                let arr: BooleanArray = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Boolean(b) => *b,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Int8 => {
                let arr: Int8Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Int8(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Int16 => {
                let arr: Int16Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Int16(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Int32 => {
                let arr: Int32Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Int32(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Int64 => {
                let arr: Int64Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Int64(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::UInt8 => {
                let arr: UInt8Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::UInt8(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::UInt16 => {
                let arr: UInt16Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::UInt16(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::UInt32 => {
                let arr: UInt32Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::UInt32(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::UInt64 => {
                let arr: UInt64Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::UInt64(i) => *i,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Float32 => {
                let arr: Float32Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Float32(f) => *f,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Float64 => {
                let arr: Float64Array = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Float64(f) => *f,
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Utf8 => {
                let arr: StringArray = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Utf8(s) => s.as_deref(),
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::LargeUtf8 => {
                let arr: LargeStringArray = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::LargeUtf8(s) => s.as_deref(),
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::Binary => {
                let arr: BinaryArray = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Binary(b) => b.as_deref(),
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            DataType::LargeBinary => {
                let arr: LargeBinaryArray = values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::LargeBinary(b) => b.as_deref(),
                        _ => None,
                    })
                    .collect();
                Arc::new(arr)
            }
            _ => {
                // Fallback: convert to string
                let arr: StringArray = values.iter().map(|v| Some(format!("{:?}", v))).collect();
                Arc::new(arr)
            }
        };

        arrays.push(array);
    }

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_basic_kv_query() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));
        let provider = Arc::new(LsmTableProvider::new_kv(tree.clone()));

        // Insert some data
        provider
            .insert(&[
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("1".to_string())),
            ])
            .unwrap();
        provider
            .insert(&[
                ScalarValue::Utf8(Some("b".to_string())),
                ScalarValue::Utf8(Some("2".to_string())),
            ])
            .unwrap();
        provider
            .insert(&[
                ScalarValue::Utf8(Some("c".to_string())),
                ScalarValue::Utf8(Some("3".to_string())),
            ])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("kv", provider).unwrap();

        let df = ctx.sql("SELECT * FROM kv ORDER BY key").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_custom_schema() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let provider = Arc::new(LsmTableProvider::new(tree.clone(), schema, 1, vec![0]));

        // Insert some data
        provider
            .insert(&[
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("Alice".to_string())),
                ScalarValue::Float64(Some(95.5)),
            ])
            .unwrap();
        provider
            .insert(&[
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("Bob".to_string())),
                ScalarValue::Float64(Some(87.0)),
            ])
            .unwrap();
        provider
            .insert(&[
                ScalarValue::Int64(Some(3)),
                ScalarValue::Utf8(Some("Charlie".to_string())),
                ScalarValue::Float64(None),
            ])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("students", provider).unwrap();

        // Test basic query
        let df = ctx.sql("SELECT * FROM students ORDER BY id").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 3);

        // Test projection
        let df = ctx
            .sql("SELECT name, score FROM students ORDER BY id")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_columns(), 2);

        // Test filter
        let df = ctx
            .sql("SELECT * FROM students WHERE id >= 2")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_multiple_tables() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));

        // Create two tables with different schemas
        let users_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("username", DataType::Utf8, false),
        ]));
        let users = Arc::new(LsmTableProvider::new(
            tree.clone(),
            users_schema,
            1,
            vec![0],
        ));

        let orders_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let orders = Arc::new(LsmTableProvider::new(
            tree.clone(),
            orders_schema,
            2,
            vec![0],
        ));

        // Insert data into both tables
        users
            .insert(&[
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("alice".to_string())),
            ])
            .unwrap();
        users
            .insert(&[
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("bob".to_string())),
            ])
            .unwrap();

        orders
            .insert(&[
                ScalarValue::Int64(Some(100)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Float64(Some(50.0)),
            ])
            .unwrap();
        orders
            .insert(&[
                ScalarValue::Int64(Some(101)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Float64(Some(75.0)),
            ])
            .unwrap();
        orders
            .insert(&[
                ScalarValue::Int64(Some(102)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Float64(Some(25.0)),
            ])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("users", users).unwrap();
        ctx.register_table("orders", orders).unwrap();

        // Query users
        let df = ctx
            .sql("SELECT * FROM users ORDER BY user_id")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 2);

        // Query orders
        let df = ctx
            .sql("SELECT * FROM orders ORDER BY order_id")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 3);

        // Join query
        let df = ctx
            .sql(
                "SELECT u.username, o.amount
                 FROM users u
                 JOIN orders o ON u.user_id = o.user_id
                 ORDER BY o.order_id",
            )
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_update_row() {
        let tree = Arc::new(RwLock::new(LsmTree::new(10)));
        let provider = Arc::new(LsmTableProvider::new_kv(tree.clone()));

        // Insert initial value
        provider
            .insert(&[
                ScalarValue::Utf8(Some("key1".to_string())),
                ScalarValue::Utf8(Some("value1".to_string())),
            ])
            .unwrap();

        // Update the value
        provider
            .insert(&[
                ScalarValue::Utf8(Some("key1".to_string())),
                ScalarValue::Utf8(Some("value2".to_string())),
            ])
            .unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("kv", provider).unwrap();

        let df = ctx
            .sql("SELECT value FROM kv WHERE key = 'key1'")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        use datafusion::arrow::array::StringArray;
        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "value2");
    }
}
