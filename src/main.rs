mod codec;
mod datafusion;
mod lsm;

use std::sync::{Arc, RwLock};

use ::datafusion::arrow::datatypes::{DataType, Field, Schema};
use ::datafusion::prelude::*;
use ::datafusion::scalar::ScalarValue;

use crate::datafusion::LsmTableProvider;
use crate::lsm::LsmTree;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Basic LSM Tree (no datafusion)
    let mut tree = LsmTree::new(3);

    println!("=== Basic LSM Tree Operations ===\n");
    tree.insert(b"key1".to_vec(), b"value1".to_vec());
    tree.insert(b"key2".to_vec(), b"value2".to_vec());
    tree.insert(b"key3".to_vec(), b"value3".to_vec());
    tree.insert(b"key4".to_vec(), b"value4".to_vec());
    tree.insert(b"key5".to_vec(), b"value5".to_vec());
    println!("After inserts:");
    println!("{:?}", tree);

    // Overwrite key2
    tree.insert(b"key2".to_vec(), b"value2_updated".to_vec());
    println!("\nAfter updating key2:");
    println!(
        "{:?}",
        String::from_utf8(tree.get(b"key2").unwrap()).unwrap()
    );

    println!("=== DataFusion Integration ===\n");

    // Create a single LSM tree that will store multiple tables
    let tree = Arc::new(RwLock::new(LsmTree::new(3)));

    // === Create Users Table ===
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let users = Arc::new(LsmTableProvider::new(
        tree.clone(),
        users_schema,
        1,       // table_id
        vec![0], // primary key: user_id
    ));

    // === Create Orders Table ===
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let orders = Arc::new(LsmTableProvider::new(
        tree.clone(),
        orders_schema,
        2,       // table_id
        vec![0], // primary key: order_id
    ));

    // === Insert Sample Data ===
    println!("Inserting sample data...\n");

    // Users
    users.insert(&[
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("alice".to_string())),
        ScalarValue::Utf8(Some("alice@example.com".to_string())),
    ])?;
    users.insert(&[
        ScalarValue::Int64(Some(2)),
        ScalarValue::Utf8(Some("bob".to_string())),
        ScalarValue::Utf8(Some("bob@example.com".to_string())),
    ])?;
    users.insert(&[
        ScalarValue::Int64(Some(3)),
        ScalarValue::Utf8(Some("charlie".to_string())),
        ScalarValue::Utf8(None), // No email
    ])?;

    // Orders
    orders.insert(&[
        ScalarValue::Int64(Some(100)),
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("Widget".to_string())),
        ScalarValue::Float64(Some(29.99)),
    ])?;
    orders.insert(&[
        ScalarValue::Int64(Some(101)),
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("Gadget".to_string())),
        ScalarValue::Float64(Some(49.99)),
    ])?;
    orders.insert(&[
        ScalarValue::Int64(Some(102)),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Utf8(Some("Widget".to_string())),
        ScalarValue::Float64(Some(29.99)),
    ])?;
    orders.insert(&[
        ScalarValue::Int64(Some(103)),
        ScalarValue::Int64(Some(3)),
        ScalarValue::Utf8(Some("Thingamajig".to_string())),
        ScalarValue::Float64(Some(99.99)),
    ])?;

    // === Register Tables with DataFusion ===
    let ctx = SessionContext::new();
    ctx.register_table("users", users)?;
    ctx.register_table("orders", orders)?;

    // === Run Queries ===

    println!("=== All Users ===");
    let df = ctx.sql("SELECT * FROM users ORDER BY user_id").await?;
    df.show().await?;

    println!("\n=== All Orders ===");
    let df = ctx.sql("SELECT * FROM orders ORDER BY order_id").await?;
    df.show().await?;

    println!("\n=== Orders with User Info (JOIN) ===");
    let df = ctx
        .sql(
            "SELECT u.username, o.product, o.amount
             FROM users u
             JOIN orders o ON u.user_id = o.user_id
             ORDER BY u.username, o.order_id",
        )
        .await?;
    df.show().await?;

    println!("\n=== Total Spending per User ===");
    let df = ctx
        .sql(
            "SELECT u.username, SUM(o.amount) as total_spent
             FROM users u
             JOIN orders o ON u.user_id = o.user_id
             GROUP BY u.username
             ORDER BY total_spent DESC",
        )
        .await?;
    df.show().await?;

    println!("\n=== Users with ID >= 2 (Filter Pushdown) ===");
    let df = ctx
        .sql("SELECT * FROM users WHERE user_id >= 2 ORDER BY user_id")
        .await?;
    df.show().await?;

    println!("\n=== Update Demo: Changing alice's email ===");
    // Get the users provider back to insert an update
    let users_table = ctx.table_provider("users").await?;
    let users_provider = users_table
        .as_any()
        .downcast_ref::<LsmTableProvider>()
        .expect("Should be LsmTableProvider");

    users_provider.insert(&[
        ScalarValue::Int64(Some(1)),
        ScalarValue::Utf8(Some("alice".to_string())),
        ScalarValue::Utf8(Some("alice.new@example.com".to_string())),
    ])?;

    let df = ctx.sql("SELECT * FROM users WHERE user_id = 1").await?;
    df.show().await?;

    // === Debug: Show LSM Tree Structure ===
    println!("\n=== LSM Tree Debug (Raw) ===");
    println!("{}", tree.read().unwrap());

    println!("\n=== LSM Tree Debug (Detailed) ===");
    println!("{:?}", tree.read().unwrap());

    println!("\n=== Users Table Debug (with decoded MessagePack) ===");
    users_provider.debug_dump();

    Ok(())
}
