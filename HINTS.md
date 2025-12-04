# LSM Tree Implementation Hints

## `insert`

1. Insert the key-value pair into `self.memtable` (it's a `BTreeMap`)
2. Check if memtable size has reached `self.memtable_threshold`
3. If threshold reached, call `self.flush_memtable()`

## `get`

1. Check the memtable first (most recent data lives here)
2. If not found, iterate through `self.levels` from newest to oldest
3. For each level, you can use `level.stats` (lower/upper bounds) to skip levels where the key is out of range
4. Within a level, use `binary_search_by` on `level.data` to find the key efficiently
5. Return `None` if key not found anywhere

## `merge_sorted`

This is a classic two-pointer merge algorithm:

1. Both inputs are already sorted by key
2. Use two pointers (`i` for existing, `j` for new_data)
3. Compare keys at each pointer position:
   - If `existing[i].key < new_data[j].key`: push `existing[i]`, advance `i`
   - If `existing[i].key > new_data[j].key`: push `new_data[j]`, advance `j`
   - If keys are equal: push `new_data[j]` (newer wins!), advance both
4. After the main loop, append any remaining items from both vectors
5. Return the merged result
