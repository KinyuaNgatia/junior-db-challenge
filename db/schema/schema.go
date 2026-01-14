package schema

import "mini-rdbms/db/types"

// ColumnDef defines a single column in a table.
type ColumnDef struct {
	Name      string
	Type      types.DataType
	IsPrimary bool
	IsUnique  bool
}

// TableDef defines the schema of a table.
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

// GetColumn finds a column definition by name.
func (t *TableDef) GetColumn(name string) (ColumnDef, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return ColumnDef{}, false
}

// GetPrimaryKey returns the primary key column definition.
func (t *TableDef) GetPrimaryKey() (ColumnDef, bool) {
	for _, c := range t.Columns {
		if c.IsPrimary {
			return c, true
		}
	}
	return ColumnDef{}, false
}

// GetColumnIndex returns the index of the column in the row.
func (t *TableDef) GetColumnIndex(name string) int {
	for i, c := range t.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}
