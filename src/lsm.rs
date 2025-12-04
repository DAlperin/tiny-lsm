use std::collections::BTreeMap;
use std::fmt;

pub struct LsmTree {
    // In-memory sorted data structure to hold recent writes
    memtable: BTreeMap<Vec<u8>, Vec<u8>>,
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

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // TODO: Implement insert
        // Insert into memtable, flush to disk if threshold reached
        // Useful: self.memtable.insert(), self.memtable.len(), self.flush_memtable()
        todo!("Implement insert")
    }

    fn flush_memtable(&mut self) {
        let mut new_level_data = Vec::new();
        for (key, value) in std::mem::take(&mut self.memtable) {
            new_level_data.push((key, value));
        }
        self.merge_into_level(0, new_level_data);
    }

    fn merge_into_level(&mut self, level: usize, new_data: Vec<(Vec<u8>, Vec<u8>)>) {
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

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // TODO: Implement get
        // Check memtable first, then search through levels
        // Useful: self.memtable.get(), level.stats.{lower,upper}
        todo!("Implement get")
    }

    /// Scans for all key-value pairs in the range [start, end].
    /// Returns an iterator that yields (key, value) pairs in sorted order.
    pub fn scan<'a, 'b: 'a>(
        &'a self,
        start: Option<&'b [u8]>,
        end: Option<&'b [u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        let mut iterators: Vec<Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a>> = vec![];

        let lower_bound = if let Some(start) = start {
            std::ops::Bound::Included(start.to_vec())
        } else {
            std::ops::Bound::Unbounded
        };

        let upper_bound = if let Some(end) = end {
            std::ops::Bound::Included(end.to_vec())
        } else {
            std::ops::Bound::Unbounded
        };

        let memtable_iter = self
            .memtable
            .range::<Vec<u8>, _>((lower_bound, upper_bound))
            .map(|(k, v)| (k.as_slice(), v.as_slice()));

        iterators.push(Box::new(memtable_iter));

        for level in self.levels.iter().filter_map(|l| l.as_ref()) {
            if let Some(start) = start {
                if start > level.stats.upper.as_slice() {
                    continue;
                }
            }
            if let Some(end) = end {
                if end < level.stats.lower.as_slice() {
                    continue;
                }
            }

            let level_iter = level
                .data
                .iter()
                .filter(move |(k, _)| {
                    let key = k.as_slice();
                    start.map_or(true, |s| key >= s) && end.map_or(true, |e| key <= e)
                })
                .map(|(k, v)| (k.as_slice(), v.as_slice()));

            iterators.push(Box::new(level_iter));
        }

        ScanIterator::new(iterators)
    }

    /// Returns the number of entries in the memtable.
    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    /// Pretty-prints the tree structure with a custom value formatter.
    /// The formatter receives (key_bytes, value_bytes) and returns a string representation.
    pub fn display_with_formatter<F>(&self, f: &mut fmt::Formatter<'_>, value_fmt: F) -> fmt::Result
    where
        F: Fn(&[u8], &[u8]) -> String,
    {
        writeln!(f, "LsmTree {{")?;
        writeln!(f, "  memtable_threshold: {}", self.memtable_threshold)?;
        writeln!(f, "  memtable: ({} entries) {{", self.memtable.len())?;
        for (key, value) in &self.memtable {
            writeln!(f, "    {}", value_fmt(key, value))?;
        }
        writeln!(f, "  }}")?;

        writeln!(f, "  levels: ({} levels) [", self.levels.len())?;
        for (i, level) in self.levels.iter().enumerate() {
            let capacity = self.level_capacity(i);
            match level {
                Some(l) => {
                    writeln!(
                        f,
                        "    L{} ({}/{} entries, keys: {:?}..{:?}) {{",
                        i,
                        l.data.len(),
                        capacity,
                        format_key_short(&l.stats.lower),
                        format_key_short(&l.stats.upper),
                    )?;
                    for (key, value) in &l.data {
                        writeln!(f, "      {}", value_fmt(key, value))?;
                    }
                    writeln!(f, "    }}")?;
                }
                None => {
                    writeln!(f, "    L{} (empty, capacity: {})", i, capacity)?;
                }
            }
        }
        writeln!(f, "  ]")?;
        writeln!(f, "}}")
    }
}

/// Formats a key for display, showing as string if valid UTF-8, otherwise as hex.
fn format_key_short(key: &[u8]) -> String {
    if key.is_empty() {
        return "[]".to_string();
    }
    // Skip table prefix (first 4 bytes) if present and show the rest
    let display_part = if key.len() > 4 { &key[4..] } else { key };
    match std::str::from_utf8(display_part) {
        Ok(s) if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') => {
            format!("\"{}\"", s)
        }
        _ => format!("{:02x?}", &key[..key.len().min(8)]),
    }
}

/// Default formatter that shows keys/values as strings or hex.
fn default_kv_formatter(key: &[u8], value: &[u8]) -> String {
    let key_str = format_bytes_smart(key);
    let value_str = format_bytes_smart(value);
    format!("{} => {}", key_str, value_str)
}

/// Formats bytes as a string if valid UTF-8, otherwise as hex.
fn format_bytes_smart(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') => {
            format!("\"{}\"", s)
        }
        _ if bytes.len() <= 32 => {
            format!("0x{}", hex_encode(bytes))
        }
        _ => {
            format!("0x{}... ({} bytes)", hex_encode(&bytes[..16]), bytes.len())
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

impl fmt::Debug for LsmTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.display_with_formatter(f, default_kv_formatter)
    }
}

impl fmt::Display for LsmTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "LsmTree: {} memtable entries, {} levels",
            self.memtable.len(),
            self.levels.len()
        )?;
        for (i, level) in self.levels.iter().enumerate() {
            match level {
                Some(l) => {
                    writeln!(
                        f,
                        "  L{}: {} entries (capacity: {})",
                        i,
                        l.data.len(),
                        self.level_capacity(i)
                    )?;
                }
                None => {
                    writeln!(f, "  L{}: empty", i)?;
                }
            }
        }
        Ok(())
    }
}

fn compute_stats(data: &[(Vec<u8>, Vec<u8>)]) -> LevelStats {
    let lower = data.first().map(|(k, _)| k.clone()).unwrap_or_default();
    let upper = data.last().map(|(k, _)| k.clone()).unwrap_or_default();
    LevelStats { lower, upper }
}

fn merge_sorted(
    existing: Vec<(Vec<u8>, Vec<u8>)>,
    new_data: Vec<(Vec<u8>, Vec<u8>)>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    // TODO: Implement merge_sorted
    // Merge two sorted lists; newer data wins on duplicate keys
    todo!("Implement merge_sorted")
}

#[derive(Debug)]
struct LsmLevel {
    // For simplicity, we will just store the data directly in the level
    data: Vec<(Vec<u8>, Vec<u8>)>,
    stats: LevelStats,
}

#[derive(Debug)]
struct LevelStats {
    lower: Vec<u8>,
    upper: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
struct HeapMergeItem<'a> {
    /// The key of the item being merged
    key: &'a [u8],
    /// The value of the item being merged
    value: &'a [u8],
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
    iterators: Vec<Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a>>,
    heap: std::collections::BinaryHeap<HeapMergeItem<'a>>,
    last_key: Option<Vec<u8>>,
}

impl<'a> ScanIterator<'a> {
    fn new(mut iterators: Vec<Box<dyn Iterator<Item = (&'a [u8], &'a [u8])> + 'a>>) -> Self {
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
    type Item = (&'a [u8], &'a [u8]);

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
                if last.as_slice() == item.key {
                    continue;
                }
            }

            self.last_key = Some(item.key.to_vec());
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
        lsm.insert(b"key1".to_vec(), b"value1".to_vec());
        lsm.insert(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(lsm.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(lsm.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(lsm.get(b"key3"), None);
    }

    #[test]
    fn test_memtable_flush() {
        let mut lsm = LsmTree::new(2);

        lsm.insert(b"k1".to_vec(), b"v1".to_vec());
        lsm.insert(b"k2".to_vec(), b"v2".to_vec());

        assert_eq!(lsm.memtable.len(), 0);
        assert_eq!(lsm.levels.len(), 1);
        assert!(lsm.levels[0].is_some());
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        assert_eq!(lsm.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(lsm.get(b"k2"), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_level_merge() {
        // Threshold 2.
        // Level 0 capacity: 2 * 2^0 = 2.
        // Level 1 capacity: 2 * 2^1 = 4.
        let mut lsm = LsmTree::new(2);

        // 1. Insert 2 items -> Flush to L0. L0 size 2.
        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());

        println!("After first flush: {:#?}", lsm);

        assert_eq!(lsm.levels.len(), 1);
        assert_eq!(lsm.levels[0].as_ref().unwrap().data.len(), 2);

        // 2. Insert 2 items -> Flush to L0.
        // Merge (L0 existing) + (New) = 4 items.
        // 4 > L0 capacity (2). So push to L1.
        lsm.insert(b"c".to_vec(), b"3".to_vec());
        lsm.insert(b"d".to_vec(), b"4".to_vec());

        println!("After second flush: {:#?}", lsm);

        assert_eq!(lsm.levels.len(), 2); // Should have created L1
        assert!(lsm.levels[0].is_none()); // L0 data moved up
        assert!(lsm.levels[1].is_some()); // L1 has the data
        assert_eq!(lsm.levels[1].as_ref().unwrap().data.len(), 4);

        assert_eq!(lsm.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(lsm.get(b"d"), Some(b"4".to_vec()));
    }

    #[test]
    fn test_overwrite() {
        let mut lsm = LsmTree::new(2);
        lsm.insert(b"key1".to_vec(), b"val1".to_vec());
        lsm.insert(b"key1".to_vec(), b"val2".to_vec()); // Overwrite in memtable

        assert_eq!(lsm.get(b"key1"), Some(b"val2".to_vec()));

        // Flush
        lsm.insert(b"key2".to_vec(), b"val3".to_vec()); // Trigger flush (size 2)

        // Now key1 is in L0 with val2.
        assert_eq!(lsm.get(b"key1"), Some(b"val2".to_vec()));

        // Overwrite again in memtable
        lsm.insert(b"key1".to_vec(), b"val3".to_vec());
        assert_eq!(lsm.get(b"key1"), Some(b"val3".to_vec()));
    }

    #[test]
    fn test_scan() {
        let mut lsm = LsmTree::new(2);

        // Insert "a", "b" -> Flush to L0
        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());

        // Insert "c", "d" -> Flush to L0 -> Merge with L0 -> L1
        lsm.insert(b"c".to_vec(), b"3".to_vec());
        lsm.insert(b"d".to_vec(), b"4".to_vec());

        // Insert "e" -> Memtable
        lsm.insert(b"e".to_vec(), b"5".to_vec());

        // Overwrite "b" in memtable
        lsm.insert(b"b".to_vec(), b"20".to_vec());

        // Current state:
        // Memtable: "b" -> "20", "e" -> "5"
        // L1: "a" -> "1", "b" -> "2", "c" -> "3", "d" -> "4"
        // L0: None

        // Scan from "a" to "z"
        let items: Vec<_> = lsm.scan(Some(b"a"), Some(b"z")).collect();

        // Verify we got the latest values and all keys
        let mut sorted_items = items.clone();
        sorted_items.sort();

        assert_eq!(
            sorted_items,
            vec![
                (b"a".as_slice(), b"1".as_slice()),
                (b"b".as_slice(), b"20".as_slice()), // Latest value
                (b"c".as_slice(), b"3".as_slice()),
                (b"d".as_slice(), b"4".as_slice()),
                (b"e".as_slice(), b"5".as_slice()),
            ]
        );
    }

    #[test]
    fn test_full_scan() {
        let mut lsm = LsmTree::new(2);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());
        lsm.insert(b"c".to_vec(), b"3".to_vec());
        lsm.insert(b"d".to_vec(), b"4".to_vec());
        lsm.insert(b"e".to_vec(), b"5".to_vec());

        let items: Vec<_> = lsm.scan(None, None).collect();

        let mut sorted_items = items.clone();
        sorted_items.sort();

        assert_eq!(
            sorted_items,
            vec![
                (b"a".as_slice(), b"1".as_slice()),
                (b"b".as_slice(), b"2".as_slice()),
                (b"c".as_slice(), b"3".as_slice()),
                (b"d".as_slice(), b"4".as_slice()),
                (b"e".as_slice(), b"5".as_slice()),
            ]
        );
    }

    #[test]
    fn test_scan_iterator_direct() {
        // Create 3 iterators with overlapping keys to test merge logic and precedence
        // Iter 0 (Highest priority): ("a", "10"), ("c", "30")
        // Iter 1 (Medium priority):  ("a", "1"),  ("b", "2")
        // Iter 2 (Lowest priority):  ("b", "20"), ("d", "4")

        let data0: Vec<(&[u8], &[u8])> = vec![(b"a", b"10"), (b"c", b"30")];
        let data1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2")];
        let data2: Vec<(&[u8], &[u8])> = vec![(b"b", b"20"), (b"d", b"4")];

        let iter0 = data0.into_iter();
        let iter1 = data1.into_iter();
        let iter2 = data2.into_iter();

        let iterators: Vec<Box<dyn Iterator<Item = (&[u8], &[u8])>>> =
            vec![Box::new(iter0), Box::new(iter1), Box::new(iter2)];

        let scan_iter = ScanIterator::new(iterators);
        let result: Vec<_> = scan_iter.collect();

        assert_eq!(
            result,
            vec![
                (b"a".as_slice(), b"10".as_slice()), // From iter0
                (b"b".as_slice(), b"2".as_slice()),  // From iter1 (iter0 didn't have 'b')
                (b"c".as_slice(), b"30".as_slice()), // From iter0
                (b"d".as_slice(), b"4".as_slice()),  // From iter2
            ]
        );
    }

    #[test]
    fn test_merge_sorted_basic() {
        let existing = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        let new_data = vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        let result = merge_sorted(existing, new_data);

        assert_eq!(
            result,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
            ]
        );
    }

    #[test]
    fn test_merge_sorted_with_duplicates() {
        // When keys are equal, new_data should win
        let existing = vec![
            (b"a".to_vec(), b"old".to_vec()),
            (b"b".to_vec(), b"old".to_vec()),
        ];
        let new_data = vec![
            (b"a".to_vec(), b"new".to_vec()),
            (b"c".to_vec(), b"new".to_vec()),
        ];

        let result = merge_sorted(existing, new_data);

        assert_eq!(
            result,
            vec![
                (b"a".to_vec(), b"new".to_vec()), // new_data wins
                (b"b".to_vec(), b"old".to_vec()),
                (b"c".to_vec(), b"new".to_vec()),
            ]
        );
    }

    #[test]
    fn test_merge_sorted_empty_inputs() {
        let empty: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let data = vec![(b"a".to_vec(), b"1".to_vec())];

        // Empty existing
        assert_eq!(merge_sorted(empty.clone(), data.clone()), data);

        // Empty new_data
        assert_eq!(merge_sorted(data.clone(), empty.clone()), data);

        // Both empty
        assert_eq!(merge_sorted(empty.clone(), empty), vec![]);
    }

    #[test]
    fn test_compute_stats_basic() {
        let data = vec![
            (b"apple".to_vec(), b"1".to_vec()),
            (b"banana".to_vec(), b"2".to_vec()),
            (b"cherry".to_vec(), b"3".to_vec()),
        ];

        let stats = compute_stats(&data);

        assert_eq!(stats.lower, b"apple".to_vec());
        assert_eq!(stats.upper, b"cherry".to_vec());
    }

    #[test]
    fn test_compute_stats_single_element() {
        let data = vec![(b"only".to_vec(), b"1".to_vec())];

        let stats = compute_stats(&data);

        assert_eq!(stats.lower, b"only".to_vec());
        assert_eq!(stats.upper, b"only".to_vec());
    }

    #[test]
    fn test_compute_stats_empty() {
        let data: Vec<(Vec<u8>, Vec<u8>)> = vec![];

        let stats = compute_stats(&data);

        assert_eq!(stats.lower, Vec::<u8>::new());
        assert_eq!(stats.upper, Vec::<u8>::new());
    }

    #[test]
    fn test_heap_merge_item_ordering() {
        // HeapMergeItem uses reverse ordering for min-heap behavior
        let item_a = HeapMergeItem {
            key: b"a",
            value: b"1",
            index: 0,
        };
        let item_b = HeapMergeItem {
            key: b"b",
            value: b"2",
            index: 0,
        };

        // "a" < "b", so item_a should be "greater" in heap ordering (comes out first)
        assert!(item_a > item_b);
    }

    #[test]
    fn test_heap_merge_item_same_key_different_index() {
        // When keys are equal, lower index should have higher priority
        let item_idx0 = HeapMergeItem {
            key: b"same",
            value: b"1",
            index: 0,
        };
        let item_idx1 = HeapMergeItem {
            key: b"same",
            value: b"2",
            index: 1,
        };

        // Lower index should be "greater" (higher priority in max-heap)
        assert!(item_idx0 > item_idx1);
    }

    #[test]
    fn test_level_capacity() {
        let lsm = LsmTree::new(4);

        // Level 0: 4 * 2^0 = 4
        assert_eq!(lsm.level_capacity(0), 4);
        // Level 1: 4 * 2^1 = 8
        assert_eq!(lsm.level_capacity(1), 8);
        // Level 2: 4 * 2^2 = 16
        assert_eq!(lsm.level_capacity(2), 16);
        // Level 3: 4 * 2^3 = 32
        assert_eq!(lsm.level_capacity(3), 32);
    }

    #[test]
    fn test_scan_partial_range() {
        let mut lsm = LsmTree::new(10);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());
        lsm.insert(b"c".to_vec(), b"3".to_vec());
        lsm.insert(b"d".to_vec(), b"4".to_vec());
        lsm.insert(b"e".to_vec(), b"5".to_vec());

        // Scan only b to d
        let items: Vec<_> = lsm.scan(Some(b"b"), Some(b"d")).collect();

        assert_eq!(
            items,
            vec![
                (b"b".as_slice(), b"2".as_slice()),
                (b"c".as_slice(), b"3".as_slice()),
                (b"d".as_slice(), b"4".as_slice()),
            ]
        );
    }

    #[test]
    fn test_scan_start_only() {
        let mut lsm = LsmTree::new(10);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());
        lsm.insert(b"c".to_vec(), b"3".to_vec());

        // Scan from b to end
        let items: Vec<_> = lsm.scan(Some(b"b"), None).collect();

        assert_eq!(
            items,
            vec![
                (b"b".as_slice(), b"2".as_slice()),
                (b"c".as_slice(), b"3".as_slice()),
            ]
        );
    }

    #[test]
    fn test_scan_end_only() {
        let mut lsm = LsmTree::new(10);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"b".to_vec(), b"2".to_vec());
        lsm.insert(b"c".to_vec(), b"3".to_vec());

        // Scan from beginning to b
        let items: Vec<_> = lsm.scan(None, Some(b"b")).collect();

        assert_eq!(
            items,
            vec![
                (b"a".as_slice(), b"1".as_slice()),
                (b"b".as_slice(), b"2".as_slice()),
            ]
        );
    }

    #[test]
    fn test_get_uses_stats_filtering() {
        let mut lsm = LsmTree::new(2);

        // Insert keys that will flush to L0
        lsm.insert(b"m".to_vec(), b"1".to_vec());
        lsm.insert(b"n".to_vec(), b"2".to_vec());

        // L0 now has keys "m" to "n"
        // Querying for "a" (below range) or "z" (above range) should skip the level
        assert_eq!(lsm.get(b"a"), None);
        assert_eq!(lsm.get(b"z"), None);

        // Keys in range should still work
        assert_eq!(lsm.get(b"m"), Some(b"1".to_vec()));
        assert_eq!(lsm.get(b"n"), Some(b"2".to_vec()));
    }

    #[test]
    fn test_memtable_len() {
        let mut lsm = LsmTree::new(10);

        assert_eq!(lsm.memtable_len(), 0);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        assert_eq!(lsm.memtable_len(), 1);

        lsm.insert(b"b".to_vec(), b"2".to_vec());
        assert_eq!(lsm.memtable_len(), 2);

        // Overwriting doesn't increase count
        lsm.insert(b"a".to_vec(), b"updated".to_vec());
        assert_eq!(lsm.memtable_len(), 2);
    }

    #[test]
    fn test_multiple_level_compactions() {
        // Test that data correctly cascades through multiple levels
        let mut lsm = LsmTree::new(2);

        // Insert enough data to trigger multiple level compactions
        // With threshold 2:
        // - L0 capacity: 2
        // - L1 capacity: 4
        // - L2 capacity: 8

        // Insert 8 unique keys to fill up through L2
        for i in 0..8 {
            let key = format!("key{:02}", i).into_bytes();
            let value = format!("val{:02}", i).into_bytes();
            lsm.insert(key, value);
        }

        // Verify all keys are retrievable
        for i in 0..8 {
            let key = format!("key{:02}", i).into_bytes();
            let expected = format!("val{:02}", i).into_bytes();
            assert_eq!(lsm.get(&key), Some(expected), "Failed for key{:02}", i);
        }
    }

    #[test]
    fn test_scan_empty_tree() {
        let lsm = LsmTree::new(10);

        let items: Vec<_> = lsm.scan(None, None).collect();
        assert!(items.is_empty());

        let items: Vec<_> = lsm.scan(Some(b"a"), Some(b"z")).collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_scan_no_matches_in_range() {
        let mut lsm = LsmTree::new(10);

        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"z".to_vec(), b"2".to_vec());

        // Scan for range with no keys
        let items: Vec<_> = lsm.scan(Some(b"m"), Some(b"n")).collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_get_not_found_in_level() {
        let mut lsm = LsmTree::new(2);

        // Insert and flush to create a level
        lsm.insert(b"a".to_vec(), b"1".to_vec());
        lsm.insert(b"c".to_vec(), b"2".to_vec());

        // "b" is within stats range [a, c] but doesn't exist
        assert_eq!(lsm.get(b"b"), None);
    }
}
