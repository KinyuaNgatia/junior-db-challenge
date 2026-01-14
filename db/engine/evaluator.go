package engine

import (
	"mini-rdbms/db/parser"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/storage"
)

// Evaluate returns true if the row satisfies the expression.
func Evaluate(expr parser.Expression, row storage.Row, def schema.TableDef) bool {
	if expr == nil {
		return true
	}

	switch e := expr.(type) {
	case *parser.ComparisonExpression:
		idx := def.GetColumnIndex(e.Column)
		if idx == -1 {
			return false
		} // Error?
		val := row.Values[idx]

		switch e.Operator {
		case "=":
			cmp, _ := val.Compare(e.Value)
			return cmp == 0
		// Add >, < later
		default:
			return false
		}

	case *parser.InfixExpression:
		left := Evaluate(e.Left, row, def)
		right := Evaluate(e.Right, row, def)

		switch e.Operator {
		case "AND":
			return left && right
		case "OR":
			return left || right
		default:
			return false
		}
	}
	return false
}
