package schema

import "mini-rdbms/db/types"

// ColumnDef defines a single column in a table.
type ColumnDef struct {
	Name      string
	Type      types.DataType
	IsPrimary bool
	IsUnique  bool
}

// ForeignKeyDef defines a foreign key constraint.
// Example: orders.user_id REFERENCES users(id)
type ForeignKeyDef struct {
	Column    string // Column in this table (e.g., "user_id")
	RefTable  string // Referenced table name (e.g., "users")
	RefColumn string // Referenced column (e.g., "id")
}

// TableDef defines the schema of a table.
type TableDef struct {
	Name        string
	Columns     []ColumnDef
	ForeignKeys []ForeignKeyDef // FK constraints for this table
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

// GetForeignKey returns the FK constraint for a column, if it exists.
func (t *TableDef) GetForeignKey(column string) (ForeignKeyDef, bool) {
	for _, fk := range t.ForeignKeys {
		if fk.Column == column {
			return fk, true
		}
	}
	return ForeignKeyDef{}, false
}
