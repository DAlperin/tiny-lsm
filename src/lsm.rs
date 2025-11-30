use std::collections::BTreeMap;

#[derive(Debug)]
pub struct LsmTree {
    // In-memory sorted data structure to hold recent writes
    memtable: BTreeMap<String, String>,
    levels: Vec<Option<LsmLevel>>,

    // Threshold for flushing memtable to disk
    memtable_threshold: usize,
}

impl LsmTree {
    pub fn new(memtable_threshold: usize) -> Self {
        LsmTree {
            memtable: BTreeMap::new(),
            levels: Vec::new(),
            memtable_threshold,
        }
    }

    fn level_capacity(&self, level: usize) -> usize {
        self.memtable_threshold << level // Each level can hold 2^level times the memtable threshold
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.memtable.insert(key, value);
        if self.memtable.len() >= self.memtable_threshold {
            self.flush_memtable();
        }
    }

    fn flush_memtable(&mut self) {
        let mut new_level_data = Vec::new();
        for (key, value) in std::mem::take(&mut self.memtable) {
            new_level_data.push((key, value));
        }
        self.merge_into_level(0, new_level_data);
    }

    fn merge_into_level(&mut self, level: usize, new_data: Vec<(String, String)>) {
        if level >= self.levels.len() {
            let stats = compute_stats(&new_data);
            self.levels.push(Some(LsmLevel {
                data: new_data,
                stats,
            }));
            return;
        }

        // Merge new data with existing level data
        let existing_data = self.levels[level]
            .take()
            .map(|l| l.data)
            .unwrap_or_default();
        let sst = merge_sorted(existing_data, new_data);

        // Check if the merged data exceeds the level capacity
        if sst.len() > self.level_capacity(level) {
            // For simplicity, we just bubble the whole level up
            self.merge_into_level(level + 1, sst);
        } else {
            // Update the current level with the merged data
            self.levels[level] = Some(LsmLevel {
                stats: compute_stats(&sst),
                data: sst,
            });
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        // Check memtable first
        if let Some(value) = self.memtable.get(key) {
            return Some(value.clone());
        }

        // Check levels from newest to oldest
        for level in &self.levels {
            if let Some(level) = level {
                if key < level.stats.lower.as_str() || key > level.stats.upper.as_str() {
                    continue; // Key is out of bounds for this level
                }
                // Perform binary search in the level's data
                if let Ok(pos) = level.data.binary_search_by(|(k, _)| k.as_str().cmp(key)) {
                    return Some(level.data[pos].1.clone());
                }
            }
        }

        None // Key not found
    }

    /// Scans for all key-value pairs in the range [start, end].
    /// Returns an iterator that yields (key, value) pairs in sorted order.
    pub fn scan<'a, 'b: 'a>(
        &'a self,
        start: Option<&'b str>,
        end: Option<&'b str>,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        let mut iterators: Vec<Box<dyn Iterator<Item = (&'a str, &'a str)> + 'a>> = vec![];

        let lower_bound = if let Some(start) = start {
            std::ops::Bound::Included(start)
        } else {
            std::ops::Bound::Unbounded
        };

        let upper_bound = if let Some(end) = end {
            std::ops::Bound::Included(end)
        } else {
            std::ops::Bound::Unbounded
        };

        let memtable_iter = self
            .memtable
            .range::<str, _>((lower_bound, upper_bound))
            .map(|(k, v)| (k.as_str(), v.as_str()));

        iterators.push(Box::new(memtable_iter));

        for level in self.levels.iter().filter_map(|l| l.as_ref()) {
            if let Some(start) = start {
                if start > level.stats.upper.as_str() {
                    continue;
                }
            }
            if let Some(end) = end {
                if end < level.stats.lower.as_str() {
                    continue;
                }
            }

            let level_iter = level
                .data
                .iter()
                .filter(move |(k, _)| {
                    let key = k.as_str();
                    start.map_or(true, |s| key >= s) && end.map_or(true, |e| key <= e)
                })
                .map(|(k, v)| (k.as_str(), v.as_str()));

            iterators.push(Box::new(level_iter));
        }

        ScanIterator::new(iterators)
    }
}

fn compute_stats(data: &[(String, String)]) -> LevelStats {
    let lower = data.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let upper = data.last().map(|(k, _)| k.clone()).unwrap_or_default();
    LevelStats { lower, upper }
}

fn merge_sorted(
    existing: Vec<(String, String)>,
    new_data: Vec<(String, String)>,
) -> Vec<(String, String)> {
    // When merging, we want to keep the most recent value for each key
    let mut merged = vec![];
    // Use two pointers to merge the sorted lists
    // i points to existing, j points to new_data
    let mut i = 0;
    let mut j = 0;

    while i < existing.len() && j < new_data.len() {
        match existing[i].0.cmp(&new_data[j].0) {
            std::cmp::Ordering::Less => {
                merged.push(existing[i].clone());
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(new_data[j].clone());
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                // If keys are equal, take the value from new_data (most recent)
                merged.push(new_data[j].clone());
                i += 1;
                j += 1;
            }
        }
    }

    // Add remaining items from existing
    while i < existing.len() {
        merged.push(existing[i].clone());
        i += 1;
    }

    // Add remaining items from new_data
    while j < new_data.len() {
        merged.push(new_data[j].clone());
        j += 1;
    }

    merged
}

#[derive(Debug)]
struct LsmLevel {
    // For simplicity, we will just store the data directly in the level
    data: Vec<(String, String)>,
    stats: LevelStats,
}

#[derive(Debug)]
struct LevelStats {
    lower: String,
    upper: String,
}

#[derive(Debug, PartialEq, Eq)]
struct HeapMergeItem<'a> {
    /// The key of the item being merged
    key: &'a str,
    /// The value of the item being merged
    value: &'a str,
    /// Which iterator this item came from
    index: usize,
}

impl<'a> Ord for HeapMergeItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match other.key.cmp(&self.key) {
            // If keys are equal, we want the most recent value (lower index)
            std::cmp::Ordering::Equal => other.index.cmp(&self.index),
            other => other,
        }
    }
}

impl<'a> PartialOrd for HeapMergeItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Merges multiple sorted iterators, giving precedence to earlier iterators.
// Implemented as a basic k-way merge using a binary heap.
struct ScanIterator<'a> {
    iterators: Vec<Box<dyn Iterator<Item = (&'a str, &'a str)> + 'a>>,
    heap: std::collections::BinaryHeap<HeapMergeItem<'a>>,
    last_key: Option<String>,
}

impl<'a> ScanIterator<'a> {
    fn new(mut iterators: Vec<Box<dyn Iterator<Item = (&'a str, &'a str)> + 'a>>) -> Self {
        let mut heap = std::collections::BinaryHeap::new();
        for (index, iter) in iterators.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(HeapMergeItem { key, value, index });
            }
        }
        ScanIterator {
            iterators,
            heap,
            last_key: None,
        }
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.heap.pop()?;

            if let Some(next_val) = self.iterators[item.index].next() {
                self.heap.push(HeapMergeItem {
                    key: next_val.0,
                    value: next_val.1,
                    index: item.index,
                });
            }

            if let Some(last) = &self.last_key {
                if *last == item.key {
                    continue;
                }
            }

            self.last_key = Some(item.key.to_string());
            return Some((item.key, item.value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_and_get() {
        let mut lsm = LsmTree::new(10);
        lsm.insert("key1".to_string(), "value1".to_string());
        lsm.insert("key2".to_string(), "value2".to_string());

        assert_eq!(lsm.get("key1"), Some("value1".to_string()));
        assert_eq!(lsm.get("key2"), Some("value2".to_string()));
        assert_eq!(lsm.get("key3"), None);
    }

    #[test]
    fn test_memtable_flush() {
        let mut lsm = LsmTree::new(2);

        lsm.insert("k1".to_string(), "v1".to_string());
        lsm.insert("k2".to_string(), "v2".to_string());

        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.levels.len(), 1);
        assert!(lsm.levels[0].is_some());
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        assert_eq!(lsm.get("k1"), Some("v1".to_string()));
        assert_eq!(lsm.get("k2"), Some("v2".to_string()));
    }

    #[test]
    fn test_level_merge() {
        // Threshold 2.
        // Level 0 capacity: 2 * 2^0 = 2.
        // Level 1 capacity: 2 * 2^1 = 4.
        let mut lsm = LsmTree::new(2);

        // 1. Insert 2 items -> Flush to L0. L0 size 2.
        lsm.insert("a".to_string(), "1".to_string());
        lsm.insert("b".to_string(), "2".to_string());

        println!("After first flush: {:#?}", lsm);

        assert_eq!(lsm.levels.len(), 1);
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        // 2. Insert 2 items -> Flush to L0.
        // Merge (L0 existing) + (New) = 4 items.
        // 4 > L0 capacity (2). So push to L1.
        lsm.insert("c".to_string(), "3".to_string());
        lsm.insert("d".to_string(), "4".to_string());

        println!("After second flush: {:#?}", lsm);

        assert_eq!(lsm.levels.len(), 2); // Should have created L1
        assert!(lsm.levels[0].is_none()); // L0 data moved up
        assert!(lsm.levels[1].is_some()); // L1 has the data
        assert_eq!(lsm.levels[1].as_ref().unwrap().data.len(), 4);

        assert_eq!(lsm.get("a"), Some("1".to_string()));
        assert_eq!(lsm.get("d"), Some("4".to_string()));
    }

    #[test]
    fn test_overwrite() {
        let mut lsm = LsmTree::new(2);
        lsm.insert("key1".to_string(), "val1".to_string());
        lsm.insert("key1".to_string(), "val2".to_string()); // Overwrite in memtable

        assert_eq!(lsm.get("key1"), Some("val2".to_string()));

        // Flush
        lsm.insert("key2".to_string(), "val3".to_string()); // Trigger flush (size 2)

        // Now key1 is in L0 with val2.
        assert_eq!(lsm.get("key1"), Some("val2".to_string()));

        // Overwrite again in memtable
        lsm.insert("key1".to_string(), "val3".to_string());
        assert_eq!(lsm.get("key1"), Some("val3".to_string()));
    }

    #[test]
    fn test_scan() {
        let mut lsm = LsmTree::new(2);

        // Insert "a", "b" -> Flush to L0
        lsm.insert("a".to_string(), "1".to_string());
        lsm.insert("b".to_string(), "2".to_string());

        // Insert "c", "d" -> Flush to L0 -> Merge with L0 -> L1
        lsm.insert("c".to_string(), "3".to_string());
        lsm.insert("d".to_string(), "4".to_string());

        // Insert "e" -> Memtable
        lsm.insert("e".to_string(), "5".to_string());

        // Overwrite "b" in memtable
        lsm.insert("b".to_string(), "20".to_string());

        // Current state:
        // Memtable: "b" -> "20", "e" -> "5"
        // L1: "a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4"
        // L0: None

        // Scan from "a" to "z"
        let items: Vec<_> = lsm.scan(Some("a"), Some("z")).collect();

        // Verify we got the latest values and all keys
        let mut sorted_items = items.clone();
        sorted_items.sort();

        assert_eq!(
            sorted_items,
            vec![
                ("a", "1"),
                ("b", "20"), // Latest value
                ("c", "3"),
                ("d", "4"),
                ("e", "5"),
            ]
        );
    }

    #[test]
    fn test_full_scan() {
        let mut lsm = LsmTree::new(2);

        lsm.insert("a".to_string(), "1".to_string());
        lsm.insert("b".to_string(), "2".to_string());
        lsm.insert("c".to_string(), "3".to_string());
        lsm.insert("d".to_string(), "4".to_string());
        lsm.insert("e".to_string(), "5".to_string());

        let items: Vec<_> = lsm.scan(None, None).collect();

        let mut sorted_items = items.clone();
        sorted_items.sort();

        assert_eq!(
            sorted_items,
            vec![("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5"),]
        );
    }

    #[test]
    fn test_scan_iterator_direct() {
        // Create 3 iterators with overlapping keys to test merge logic and precedence
        // Iter 0 (Highest priority): ("a", "10"), ("c", "30")
        // Iter 1 (Medium priority):  ("a", "1"),  ("b", "2")
        // Iter 2 (Lowest priority):  ("b", "20"), ("d", "4")

        let iter0 = vec![("a", "10"), ("c", "30")].into_iter();

        let iter1 = vec![("a", "1"), ("b", "2")].into_iter();

        let iter2 = vec![("b", "20"), ("d", "4")].into_iter();

        let iterators: Vec<Box<dyn Iterator<Item = (&str, &str)>>> =
            vec![Box::new(iter0), Box::new(iter1), Box::new(iter2)];

        let scan_iter = ScanIterator::new(iterators);
        let result: Vec<_> = scan_iter.collect();

        assert_eq!(
            result,
            vec![
                ("a", "10"), // From iter0
                ("b", "2"),  // From iter1 (iter0 didn't have 'b')
                ("c", "30"), // From iter0
                ("d", "4"),  // From iter2
            ]
        );
    }
}
