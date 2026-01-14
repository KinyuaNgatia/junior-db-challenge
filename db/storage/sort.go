package storage

import (
	"mini-rdbms/db/types"
	"sort"
)

// sortPrimaryKeys sorts a slice of primary keys based on their type.
// Supports INT and TEXT types for deterministic ordering.
func sortPrimaryKeys(pks []interface{}, pkType types.DataType) {
	sort.Slice(pks, func(i, j int) bool {
		if pkType == types.TypeInt {
			// Type assert to int
			a, aOk := pks[i].(int)
			b, bOk := pks[j].(int)
			if aOk && bOk {
				return a < b
			}
			return false
		} else if pkType == types.TypeText {
			// Type assert to string
			a, aOk := pks[i].(string)
			b, bOk := pks[j].(string)
			if aOk && bOk {
				return a < b
			}
			return false
		}
		return false
	})
}
