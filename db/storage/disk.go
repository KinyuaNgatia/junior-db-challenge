package storage

import (
	"encoding/json"
	"fmt"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/types"
	"os"
	"path/filepath"
)

// storageDir usually would be configured. We'll use "data".
const DataDir = "data"

// SerializableTable is a helper struct for JSON encoding.
type SerializableTable struct {
	Name    string
	Columns []schema.ColumnDef
	Rows    []Row // We convert map to slice for saving
}

// EnsureDataDir makes sure the data directory exists.
func EnsureDataDir() error {
	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		return os.Mkdir(DataDir, 0755)
	}
	return nil
}

// SaveTable persists the table to disk atomically.
func SaveTable(t *Table) error {
	if err := EnsureDataDir(); err != nil {
		return err
	}

	// Get a snapshot of data to write while holding the lock
	rows := t.GetSnapshot()

	sTable := SerializableTable{
		Name:    t.Def.Name,
		Columns: t.Def.Columns,
		Rows:    rows,
	}

	finalFilename := filepath.Join(DataDir, t.Def.Name+".json")
	// Write to temp file first
	tempFile, err := os.CreateTemp(DataDir, "tmp-*.json")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempName := tempFile.Name()
	defer os.Remove(tempName) // Cleanup if we fail

	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(sTable); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to encode table: %w", err)
	}
	// Must close before renaming on Windows
	tempFile.Close()

	// Atomic Rename
	if err := os.Rename(tempName, finalFilename); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// LoadTable reads a table from disk.
func LoadTable(tableName string) (*Table, error) {
	filename := filepath.Join(DataDir, tableName+".json")
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("table not found: %s", tableName)
		}
		return nil, err
	}
	defer file.Close()

	var sTable SerializableTable
	if err := json.NewDecoder(file).Decode(&sTable); err != nil {
		return nil, err
	}

	// Reconstruct Table
	def := schema.TableDef{Name: sTable.Name, Columns: sTable.Columns}
	t := NewTable(def)

	// Since JSON unmarshalling of interface{} converts numbers to float64,
	// we need to fix the types based on schema.
	for _, row := range sTable.Rows {
		// Convert values
		fixedValues := make([]types.Value, len(row.Values))
		for i, val := range row.Values {
			colType := def.Columns[i].Type
			fixedValues[i] = types.Value{Type: colType, Val: val.Val}

			// Fix float64 to int if necessary
			if colType == types.TypeInt {
				if f, ok := val.Val.(float64); ok {
					fixedValues[i].Val = int(f)
				} else if iVal, ok := val.Val.(int); ok {
					fixedValues[i].Val = iVal
				}
			}
		}

		// Insert directly (bypassing redundant checks optionally, but safer to use Insert or manual set)
		// Manual set to avoid re-checking constraints if trusted valid data,
		// but we do need to rebuild indices.

		// Let's use internal logic to populate Rows and Indices

		pkCol, _ := def.GetPrimaryKey()
		pkIdx := def.GetColumnIndex(pkCol.Name)
		pk := fixedValues[pkIdx].Val

		t.Rows[pk] = Row{Values: fixedValues}

		// Rebuild indices
		for idxName, idx := range t.Indices {
			colIdx := def.GetColumnIndex(idxName)
			idx.Set(fixedValues[colIdx], pk)
		}
	}

	return t, nil
}
