package storage

import (
	"fmt"
	"mini-rdbms/db/index"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/types"
	"sync"
)

// Table represents a database table in memory.
// Thread-safe.
type Table struct {
	mu      sync.RWMutex
	Def     schema.TableDef
	Rows    map[interface{}]Row         // PK -> Row
	Indices map[string]*index.HashIndex // Column Name -> Index
}

// NewTable creates a new empty table.
func NewTable(def schema.TableDef) *Table {
	t := &Table{
		Def:     def,
		Rows:    make(map[interface{}]Row),
		Indices: make(map[string]*index.HashIndex),
	}

	// Create indices for Primary Key and Unique columns
	for _, col := range def.Columns {
		if col.IsPrimary || col.IsUnique {
			t.Indices[col.Name] = index.NewHashIndex()
		}
	}
	return t
}

// Insert adds a row to the table. Enforces constraints.
func (t *Table) Insert(values []types.Value) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(values) != len(t.Def.Columns) {
		return fmt.Errorf("column count mismatch: expected %d, got %d", len(t.Def.Columns), len(values))
	}

	// Validate types
	for i, val := range values {
		if val.Type != t.Def.Columns[i].Type {
			return fmt.Errorf("type mismatch for column %s: expected %s, got %s", t.Def.Columns[i].Name, t.Def.Columns[i].Type, val.Type)
		}
	}

	// Check constraints and gather keys
	var pk interface{}

	// 1. Check Primary Key
	pkCol, ok := t.Def.GetPrimaryKey()
	if !ok {
		return fmt.Errorf("table %s has no primary key", t.Def.Name)
	}
	pkIdx := t.Def.GetColumnIndex(pkCol.Name)
	pk = values[pkIdx].Val

	if _, exists := t.Rows[pk]; exists {
		return fmt.Errorf("duplicate primary key: %v", pk)
	}

	// 2. Check Unique Constraints
	for _, col := range t.Def.Columns {
		if col.IsUnique && !col.IsPrimary {
			colIdx := t.Def.GetColumnIndex(col.Name)
			val := values[colIdx]
			idx, hasIdx := t.Indices[col.Name]
			if hasIdx {
				if _, exists := idx.Get(val); exists {
					return fmt.Errorf("duplicate unique value for column %s: %v", col.Name, val.Val)
				}
			}
		}
	}

	// 3. Do Insert
	t.Rows[pk] = Row{Values: values}

	// 4. Update Indices
	for _, col := range t.Def.Columns {
		if col.IsPrimary || col.IsUnique {
			idx, hasIdx := t.Indices[col.Name]
			if hasIdx {
				colIdx := t.Def.GetColumnIndex(col.Name)
				idx.Set(values[colIdx], pk)
			}
		}
	}

	return nil
}

// Delete removes a row by Primary Key.
func (t *Table) Delete(pk types.Value) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	row, exists := t.Rows[pk.Val]
	if !exists {
		return fmt.Errorf("row not found for pk: %v", pk.Val)
	}

	// Remove from indices
	for _, col := range t.Def.Columns {
		if col.IsPrimary || col.IsUnique {
			idx, hasIdx := t.Indices[col.Name]
			if hasIdx {
				colIdx := t.Def.GetColumnIndex(col.Name)
				idx.Delete(row.Values[colIdx])
			}
		}
	}

	// Remove from rows
	delete(t.Rows, pk.Val)
	return nil
}

// Update modifies a row. Limitation: Updating PK is not supported.
func (t *Table) Update(pk types.Value, newValues []types.Value) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if row exists
	oldRow, exists := t.Rows[pk.Val]
	if !exists {
		return fmt.Errorf("row not found")
	}

	// Validate Count
	if len(newValues) != len(t.Def.Columns) {
		return fmt.Errorf("column count mismatch")
	}

	// Check if PK is changing
	pkCol, _ := t.Def.GetPrimaryKey()
	pkIdx := t.Def.GetColumnIndex(pkCol.Name)
	if newValues[pkIdx].Val != oldRow.Values[pkIdx].Val {
		return fmt.Errorf("updating primary key is not supported")
	}

	// Check Unique Constraints for changed values
	for i, col := range t.Def.Columns {
		if col.IsUnique && !col.IsPrimary {
			newVal := newValues[i]
			oldVal := oldRow.Values[i]
			if newVal.Val != oldVal.Val {
				idx := t.Indices[col.Name]
				if _, exists := idx.Get(newVal); exists {
					return fmt.Errorf("duplicate unique value for %s", col.Name)
				}
			}
		}
	}

	// Update Indices (Remove old, Add new)
	for i, col := range t.Def.Columns {
		if col.IsUnique && !col.IsPrimary {
			newVal := newValues[i]
			oldVal := oldRow.Values[i]
			if newVal.Val != oldVal.Val {
				idx := t.Indices[col.Name]
				idx.Delete(oldVal)
				idx.Set(newVal, pk.Val)
			}
		}
	}

	// Update Row
	t.Rows[pk.Val] = Row{Values: newValues}
	return nil
}

// GetRow returns a copy of the row for the given PK. Safe for concurrency.
func (t *Table) GetRow(pk interface{}) (Row, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	r, ok := t.Rows[pk]
	return r, ok
}

// Scan iterates over all rows safely. Stops if yield returns false.
func (t *Table) Scan(yield func(pk interface{}, row Row) bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for k, v := range t.Rows {
		if !yield(k, v) {
			break
		}
	}
}

// IndexLookup returns PK for a given indexed value.
func (t *Table) IndexLookup(colName string, val types.Value) (interface{}, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx, ok := t.Indices[colName]
	if !ok {
		return nil, false
	}
	return idx.Get(val)
}

// GetSnapshot returns all rows. Expensive but safe.
func (t *Table) GetSnapshot() []Row {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var rows []Row
	for _, r := range t.Rows {
		rows = append(rows, r)
	}
	return rows
}
