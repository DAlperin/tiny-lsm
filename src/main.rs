mod lsm;

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

    Ok(())
}
