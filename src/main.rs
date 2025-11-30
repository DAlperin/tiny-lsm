mod datafusion;
mod lsm;

use lsm::LsmTree;

fn main() {
    let mut lsm = LsmTree::new(2);
    lsm.insert("a".to_string(), "1".to_string());
    lsm.insert("b".to_string(), "2".to_string());
    // Should have flushed now. Memtable empty, Level 0 has 2 items.
    lsm.insert("a".to_string(), "10".to_string());

    let a_value = lsm.get("a");

    println!("{:?}", a_value);

    lsm.insert("c".to_string(), "3".to_string());
    lsm.insert("d".to_string(), "4".to_string());
    lsm.insert("e".to_string(), "5".to_string());
    lsm.insert("f".to_string(), "6".to_string());

    let scan_result: Vec<_> = lsm.scan(Some("a"), Some("d")).collect();

    println!("{:?}", scan_result);

    lsm.insert("d".to_string(), "40".to_string());

    println!("{:?}", lsm.get("d"));
}
