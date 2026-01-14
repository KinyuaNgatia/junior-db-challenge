package storage

import "mini-rdbms/db/types"

// Row represents a single record in the table.
// We use a slice of values corresponding to the column order in the schema.
type Row struct {
	Values []types.Value
}
