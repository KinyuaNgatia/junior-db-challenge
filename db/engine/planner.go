package engine

import (
	"context"
	"fmt"
	"mini-rdbms/db/parser"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/storage"
	"mini-rdbms/db/types"
	"strings"
)

// PlanNode interface for execution plan steps.
type PlanNode interface {
	// Execute runs the node and returns rows.
	// For simplicity, we return fully materialized rows.
	Execute(ctx context.Context) ([]storage.Row, error)
	Schema() schema.TableDef
}

// Planner converts AST to Plan.
type Planner struct {
	Tables map[string]*storage.Table
}

func NewPlanner(tables map[string]*storage.Table) *Planner {
	return &Planner{Tables: tables}
}

func (p *Planner) CreatePlan(stmt parser.Statement) (PlanNode, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		node, err := p.planSelect(s)
		if err != nil {
			return nil, err
		}

		if s.Limit > 0 {
			node = &LimitNode{Input: node, Limit: s.Limit}
		}
		return node, nil
	default:
		return nil, fmt.Errorf("planning not implemented for this statement")
	}
}

// --- Plan Nodes ---

// LimitNode limits the number of rows returned.
type LimitNode struct {
	Input PlanNode
	Limit int
}

func (n *LimitNode) Execute(ctx context.Context) ([]storage.Row, error) {
	rows, err := n.Input.Execute(ctx)
	if err != nil {
		return nil, err
	}
	if len(rows) > n.Limit {
		return rows[:n.Limit], nil
	}
	return rows, nil
}
func (n *LimitNode) Schema() schema.TableDef { return n.Input.Schema() }

// ScanNode represents a full table scan or index lookup (if Range is set - simplified).
type ScanNode struct {
	Table     *storage.Table
	Predicate func(storage.Row) bool
}

func (n *ScanNode) Execute(ctx context.Context) ([]storage.Row, error) {
	var results []storage.Row
	// Use Safe Scan
	n.Table.Scan(func(pk interface{}, row storage.Row) bool {
		// Build-in cancellation check?
		// Table.Scan doesn't support it yet, so check here.
		select {
		case <-ctx.Done():
			return false // Stop scan
		default:
		}

		// Apply predicate
		if n.Predicate != nil {
			if !n.Predicate(row) {
				return true // Continue
			}
		}
		results = append(results, row)
		return true // Continue
	})

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return results, nil
}
func (n *ScanNode) Schema() schema.TableDef { return n.Table.Def }

// IndexScanNode represents an index lookup (O(1)).
type IndexScanNode struct {
	Table     *storage.Table
	IndexName string
	Value     types.Value
}

func (n *IndexScanNode) Execute(ctx context.Context) ([]storage.Row, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	pk, found := n.Table.IndexLookup(n.IndexName, n.Value)
	if !found {
		return []storage.Row{}, nil
	}
	row, ok := n.Table.GetRow(pk)
	if !ok {
		// Inconsistency?
		return []storage.Row{}, nil
	}
	return []storage.Row{row}, nil
}
func (n *IndexScanNode) Schema() schema.TableDef { return n.Table.Def }

// JoinNode represents a nested loop join.
type JoinNode struct {
	Left  PlanNode
	Right PlanNode // Often a TableScan or IndexScan
	// Condition: LeftCol = RightCol
	LeftCol  string
	RightCol string
}

func (n *JoinNode) Execute(ctx context.Context) ([]storage.Row, error) {
	leftRows, err := n.Left.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Optimized: If Right is IndexScanNode, we can iterate Left and lookup Right?
	// For simplicity: Materialize Right side and Nested Loop.

	rightRows, err := n.Right.Execute(ctx)
	if err != nil {
		return nil, err
	}

	var results []storage.Row
	lSchema := n.Left.Schema()
	rSchema := n.Right.Schema()

	lIdx := lSchema.GetColumnIndex(n.LeftCol)
	rIdx := rSchema.GetColumnIndex(n.RightCol)

	if lIdx == -1 || rIdx == -1 {
		return nil, fmt.Errorf("join columns not found: %s, %s", n.LeftCol, n.RightCol)
	}

	for _, lRow := range leftRows {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, rRow := range rightRows {
			// Equality Check
			cmp, err := lRow.Values[lIdx].Compare(rRow.Values[rIdx])
			if err == nil && cmp == 0 {
				// Combine Rows
				combined := storage.Row{Values: append(lRow.Values, rRow.Values...)}
				results = append(results, combined)
			}
		}
	}
	return results, nil
}

func (n *JoinNode) Schema() schema.TableDef {
	l := n.Left.Schema()
	r := n.Right.Schema()
	return schema.TableDef{
		Name:    l.Name + "_" + r.Name, // Virtual name
		Columns: append(l.Columns, r.Columns...),
	}
}

// --- Planning Logic ---

func (p *Planner) planSelect(stmt *parser.SelectStmt) (PlanNode, error) {
	t, ok := p.Tables[stmt.TableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", stmt.TableName)
	}

	var node PlanNode

	// 1. Where Clause Optimization (Index Lookup)
	useIndex := false
	if stmt.Where != nil {
		// Only optimize simple "col = val" for now
		if comp, ok := stmt.Where.Expr.(*parser.ComparisonExpression); ok {
			if comp.Operator == "=" {
				colDef, ok := t.Def.GetColumn(comp.Column)
				if ok && (colDef.IsPrimary || colDef.IsUnique) {
					node = &IndexScanNode{
						Table:     t,
						IndexName: comp.Column,
						Value:     comp.Value,
					}
					useIndex = true
				}
			}
		}
	}

	if !useIndex {
		// Full Scan with Predicate
		node = &ScanNode{
			Table: t,
			Predicate: func(r storage.Row) bool {
				if stmt.Where == nil {
					return true
				}
				return Evaluate(stmt.Where.Expr, r, t.Def)
			},
		}
	}

	// 2. Join
	if stmt.Join != nil {
		rightTable, ok := p.Tables[stmt.Join.Table]
		if !ok {
			return nil, fmt.Errorf("join table not found: %s", stmt.Join.Table)
		}

		// Right Node (Scan for now)
		rightNode := &ScanNode{Table: rightTable}

		// Join Node
		joinNode := &JoinNode{
			Left:     node,
			Right:    rightNode,
			LeftCol:  stmt.Join.OnLeft, // e.g. "users.id" -> need to match column name in schema "id"
			RightCol: stmt.Join.OnRight,
		}

		// Fix column names
		joinNode.LeftCol = stripTablePrefix(joinNode.LeftCol)
		joinNode.RightCol = stripTablePrefix(joinNode.RightCol)

		node = joinNode
	}

	return node, nil
}

func stripTablePrefix(s string) string {
	if idx := strings.Index(s, "."); idx != -1 {
		return s[idx+1:]
	}
	return s
}
