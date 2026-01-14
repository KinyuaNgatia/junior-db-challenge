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

// JoinNode implements INNER JOIN using the Nested Loop Join algorithm.
//
// RELATIONAL ALGEBRA SEMANTICS:
// Given two relations R (Left) and S (Right), and a join condition θ (theta),
// the INNER JOIN produces a new relation containing all combinations of rows
// from R and S where θ evaluates to true.
//
// Formally: R ⋈_θ S = { r ∪ s | r ∈ R ∧ s ∈ S ∧ θ(r,s) }
//
// IMPLEMENTATION DETAILS:
// - Algorithm: Nested Loop Join (simple but correct for small datasets)
// - Join Type: INNER JOIN (only matching rows are included)
// - Join Condition: Equality predicate on specified columns (LeftCol = RightCol)
// - Non-matching rows: Excluded from result (INNER JOIN guarantee)
//
// EXAMPLE:
// Given:
//
//	users:  {id: 1, name: "Alice"}, {id: 2, name: "Bob"}
//	orders: {id: 100, user_id: 1, amount: 50}, {id: 101, user_id: 3, amount: 75}
//
// JOIN users ON orders.user_id = users.id produces:
//
//	{id: 1, name: "Alice", id: 100, user_id: 1, amount: 50}
//
// Note: Order 101 (user_id: 3) is EXCLUDED because user 3 doesn't exist.
// This enforces referential integrity at query time.
type JoinNode struct {
	Left  PlanNode // Left relation (e.g., orders table)
	Right PlanNode // Right relation (e.g., users table)

	// Join condition: LeftCol = RightCol
	// Example: "user_id" = "id" for orders.user_id = users.id
	LeftCol  string
	RightCol string
}

// Execute performs the INNER JOIN operation.
//
// ALGORITHM: Nested Loop Join
//  1. Materialize left relation (all rows from Left table)
//  2. Materialize right relation (all rows from Right table)
//  3. For each row in Left:
//     For each row in Right:
//     If Left[LeftCol] == Right[RightCol]:
//     Combine rows and add to result
//
// TIME COMPLEXITY: O(|R| * |S|) where |R| = left rows, |S| = right rows
// SPACE COMPLEXITY: O(|R| + |S| + |Result|)
//
// DETERMINISM GUARANTEE:
// Results are deterministic because:
// - Input rows are sorted by primary key (via GetSnapshot)
// - Iteration order is stable (slice iteration, not map)
// - Join condition is deterministic (equality check)
func (n *JoinNode) Execute(ctx context.Context) ([]storage.Row, error) {
	// Step 1: Materialize left relation
	leftRows, err := n.Left.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Step 2: Materialize right relation
	// Note: For optimization, if Right is an IndexScanNode, we could
	// iterate Left and perform index lookups instead of full materialization.
	// Current implementation prioritizes simplicity and correctness.
	rightRows, err := n.Right.Execute(ctx)
	if err != nil {
		return nil, err
	}

	// Prepare result accumulator
	var results []storage.Row

	// Get schemas to locate join columns
	lSchema := n.Left.Schema()
	rSchema := n.Right.Schema()

	// Find column indices for join condition
	lIdx := lSchema.GetColumnIndex(n.LeftCol)
	rIdx := rSchema.GetColumnIndex(n.RightCol)

	if lIdx == -1 || rIdx == -1 {
		return nil, fmt.Errorf("join columns not found: %s, %s", n.LeftCol, n.RightCol)
	}

	// Step 3: Nested loop join
	// Outer loop: iterate through left relation
	for _, lRow := range leftRows {
		// Check for cancellation (allows query timeout/cancellation)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Inner loop: iterate through right relation
		for _, rRow := range rightRows {
			// Evaluate join condition: Left[LeftCol] == Right[RightCol]
			// Uses type-safe comparison from types.Value
			cmp, err := lRow.Values[lIdx].Compare(rRow.Values[rIdx])

			// If comparison succeeds and values are equal (cmp == 0)
			if err == nil && cmp == 0 {
				// INNER JOIN: Combine matching rows
				// Result schema: [Left columns..., Right columns...]
				combined := storage.Row{
					Values: append(lRow.Values, rRow.Values...),
				}
				results = append(results, combined)
			}
			// If values don't match (cmp != 0), skip this combination
			// This is the INNER JOIN semantics: only matching rows included
		}
	}

	// Return all matching row combinations
	// If no matches found, returns empty slice (not an error)
	return results, nil
}

// Schema returns the combined schema of the joined tables.
//
// SCHEMA COMPOSITION:
// Given Left schema: [col1, col2, ...] and Right schema: [colA, colB, ...]
// Result schema: [col1, col2, ..., colA, colB, ...]
//
// Note: Column names are preserved from both tables. In case of name conflicts,
// the projection layer should use qualified names (e.g., "users.id", "orders.id").
func (n *JoinNode) Schema() schema.TableDef {
	l := n.Left.Schema()
	r := n.Right.Schema()
	return schema.TableDef{
		Name:    l.Name + "_" + r.Name, // Virtual name for joined relation
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
