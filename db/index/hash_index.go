package index

import (
	"mini-rdbms/db/types"
)

// HashIndex implements a simple hash map for indexing unique values.
// It maps a Value (Value.Val) to a Primary Key.
// Since we only support Unique / Primary Key indices in this requirement scope,
// 1-to-1 mapping is sufficient.
type HashIndex struct {
	// Map from index key value to Primary Key of the row
	// Key is the raw value (int or string)
	Data map[interface{}]interface{}
}

// NewHashIndex creates an empty index.
func NewHashIndex() *HashIndex {
	return &HashIndex{
		Data: make(map[interface{}]interface{}),
	}
}

// Get returns the Primary Key associated with the value.
func (idx *HashIndex) Get(val types.Value) (interface{}, bool) {
	pk, ok := idx.Data[val.Val]
	return pk, ok
}

// Set inserts or updates the key-pk pair.
func (idx *HashIndex) Set(val types.Value, pk interface{}) {
	idx.Data[val.Val] = pk
}

// Delete removes the key.
func (idx *HashIndex) Delete(val types.Value) {
	delete(idx.Data, val.Val)
}

// Rebuild clears and rebuilds the index (placeholder if needed).
func (idx *HashIndex) Clear() {
	idx.Data = make(map[interface{}]interface{})
}
