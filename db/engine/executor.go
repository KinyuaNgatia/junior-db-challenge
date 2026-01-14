package engine

import (
	"context"
	"fmt"
	"mini-rdbms/db/parser"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/storage"
	"mini-rdbms/db/types"
)

// ResultSet holds the result of a query.
type ResultSet struct {
	Columns []string
	Rows    []storage.Row
	Message string // For INSERT/UPDATE/DELETE/CREATE
}

type Engine struct {
	Tables map[string]*storage.Table
}

func NewEngine() *Engine {
	// Load tables from disk? Or empty?
	// For now, empty, but we might want `Init()` to load from data dir.
	e := &Engine{
		Tables: make(map[string]*storage.Table),
	}
	// Load existing?
	return e
}

func (e *Engine) Execute(ctx context.Context, sql string) (*ResultSet, error) {
	// 1. Tokenize
	tokenizer := parser.NewTokenizer(sql)

	// 2. Parse
	p := parser.NewParser(tokenizer)
	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	// 3. Update/DDL Execution (Immediate)
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.execCreate(s)
	case *parser.InsertStmt:
		return e.execInsert(s)
	case *parser.UpdateStmt:
		return e.execUpdate(s)
	case *parser.DeleteStmt:
		return e.execDelete(s)
	case *parser.SelectStmt:
		// 4. Query Planning & Execution
		planner := NewPlanner(e.Tables)
		plan, err := planner.CreatePlan(s)
		if err != nil {
			return nil, err
		}

		rows, err := plan.Execute(ctx)
		if err != nil {
			return nil, err
		}

		// 5. Projection (Filter Columns)
		return e.projectResult(rows, plan.Schema(), s.Fields)
	}

	return nil, fmt.Errorf("unknown statement type")
}

func (e *Engine) execCreate(stmt *parser.CreateTableStmt) (*ResultSet, error) {
	if _, exists := e.Tables[stmt.TableName]; exists {
		return nil, fmt.Errorf("table already exists: %s", stmt.TableName)
	}

	// Create def
	def := schema.TableDef{
		Name:    stmt.TableName,
		Columns: stmt.Columns,
	}

	// Validate (Must have primary key)
	if _, ok := def.GetPrimaryKey(); !ok {
		return nil, fmt.Errorf("table must have a primary key")
	}

	table := storage.NewTable(def)
	e.Tables[stmt.TableName] = table

	// Save immediately
	if err := storage.SaveTable(table); err != nil {
		return nil, err
	}

	return &ResultSet{Message: fmt.Sprintf("Table %s created", stmt.TableName)}, nil
}

func (e *Engine) execInsert(stmt *parser.InsertStmt) (*ResultSet, error) {
	table, ok := e.Tables[stmt.TableName]
	if !ok {
		// Try load?
		var err error
		table, err = storage.LoadTable(stmt.TableName)
		if err != nil {
			return nil, fmt.Errorf("table not found: %s", stmt.TableName)
		}
		e.Tables[stmt.TableName] = table
	}

	if err := table.Insert(stmt.Values); err != nil {
		return nil, err
	}

	if err := storage.SaveTable(table); err != nil {
		return nil, err
	}

	return &ResultSet{Message: "Insert successful"}, nil
}

func (e *Engine) execUpdate(stmt *parser.UpdateStmt) (*ResultSet, error) {
	table, ok := e.Tables[stmt.TableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}

	// Find rows to update.
	// Use Planner for finding rows?
	// Reuse ScanNode logic or duplicate for now.

	count := 0
	// Simplified: Iterate all rows safely using Scan to gather keys first.
	// Since we support Index in WHERE, we should use it.

	// Check if Where uses PK
	var pkTarget interface{}
	useIndex := false

	if stmt.Where != nil {
		if comp, ok := stmt.Where.Expr.(*parser.ComparisonExpression); ok {
			if comp.Operator == "=" {
				if col, ok := table.Def.GetColumn(comp.Column); ok && col.IsPrimary {
					useIndex = true
					pkTarget = comp.Value.Val
				}
			}
		}
	}

	if useIndex {
		row, exists := table.GetRow(pkTarget)
		if exists {
			// Apply Update
			if err := e.applyUpdate(table, row, stmt.Set, pkTarget); err != nil {
				return nil, err
			}
			count++
		}
	} else {
		// Scan
		// Collect keys to update to avoid issues during iteration (though Table.Scan is safe for read,
		// updating inside a Scan might block constraint checks depending on impl, best to collect IDs first).
		var keysToUpdate []interface{}
		// idx := table.Def.GetColumnIndex(stmt.Where.Column) -- Not needed for generic Evaluate

		table.Scan(func(pk interface{}, row storage.Row) bool {
			// Check Where
			if stmt.Where == nil || Evaluate(stmt.Where.Expr, row, table.Def) {
				keysToUpdate = append(keysToUpdate, pk)
			}
			return true
		})

		for _, pk := range keysToUpdate {
			// Re-fetch to be safe or update directly?
			// Need the row to check values.
			row, ok := table.GetRow(pk)
			if !ok {
				continue
			}
			if err := e.applyUpdate(table, row, stmt.Set, pk); err != nil {
				return nil, err
			}
			count++
		}
	}

	storage.SaveTable(table)
	return &ResultSet{Message: fmt.Sprintf("Updated %d rows", count)}, nil
}

func (e *Engine) applyUpdate(t *storage.Table, row storage.Row, setMap map[string]types.Value, pk interface{}) error {
	newValues := make([]types.Value, len(row.Values))
	copy(newValues, row.Values)

	for colName, newVal := range setMap {
		idx := t.Def.GetColumnIndex(colName)
		if idx == -1 {
			return fmt.Errorf("column not found: %s", colName)
		}
		newValues[idx] = newVal
	}

	// We can just construct a value.
	// We know PK column type.
	pkCol, _ := t.Def.GetPrimaryKey()
	pkValue := types.Value{Type: pkCol.Type, Val: pk}

	return t.Update(pkValue, newValues)
}

func (e *Engine) execDelete(stmt *parser.DeleteStmt) (*ResultSet, error) {
	table, ok := e.Tables[stmt.TableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}

	count := 0
	var keysToDelete []interface{}

	// Optimization: PK Lookup
	useIndex := false
	var pkTarget interface{}
	if stmt.Where != nil {
		if comp, ok := stmt.Where.Expr.(*parser.ComparisonExpression); ok {
			if comp.Operator == "=" {
				if col, ok := table.Def.GetColumn(comp.Column); ok && col.IsPrimary {
					useIndex = true
					pkTarget = comp.Value.Val
				}
			}
		}
	}

	if useIndex {
		keysToDelete = append(keysToDelete, pkTarget)
	} else {
		// Scan for keys
		// idx := table.Def.GetColumnIndex(stmt.Where.Column)

		table.Scan(func(pk interface{}, row storage.Row) bool {
			if stmt.Where == nil || Evaluate(stmt.Where.Expr, row, table.Def) {
				keysToDelete = append(keysToDelete, pk)
			}
			return true
		})
	}

	pkCol, _ := table.Def.GetPrimaryKey()

	for _, pk := range keysToDelete {
		pkValue := types.Value{Type: pkCol.Type, Val: pk}
		if err := table.Delete(pkValue); err == nil {
			count++
		}
	}

	storage.SaveTable(table)
	return &ResultSet{Message: fmt.Sprintf("Deleted %d rows", count)}, nil
}

func (e *Engine) projectResult(rows []storage.Row, schema schema.TableDef, fields []string) (*ResultSet, error) {
	// If fields contains "*", return all
	showAll := false
	for _, f := range fields {
		if f == "*" {
			showAll = true
		}
	}

	if showAll {
		// Return all columns
		colNames := make([]string, len(schema.Columns))
		for i, c := range schema.Columns {
			colNames[i] = c.Name
		}
		return &ResultSet{Columns: colNames, Rows: rows}, nil
	}

	// Filter columns
	var resultIndices []int
	var resultNames []string

	for _, f := range fields {
		// Remove prefix
		fieldName := stripTablePrefix(f)
		found := false
		for i, col := range schema.Columns {
			if col.Name == fieldName {
				resultIndices = append(resultIndices, i)
				resultNames = append(resultNames, f) // Keep original requested name? Or cleaned?
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column not found in result: %s", f)
		}
	}

	// Construct new rows
	newRows := make([]storage.Row, len(rows))
	for i, r := range rows {
		newVals := make([]types.Value, len(resultIndices))
		for j, idx := range resultIndices {
			newVals[j] = r.Values[idx]
		}
		newRows[i] = storage.Row{Values: newVals}
	}

	return &ResultSet{Columns: resultNames, Rows: newRows}, nil
}
