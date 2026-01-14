package parser

import (
	"fmt"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/types"
)

// ASTRoot interfaces
type Statement interface {
	statementNode()
}

type CreateTableStmt struct {
	TableName string
	Columns   []schema.ColumnDef
}

func (s *CreateTableStmt) statementNode() {}

type InsertStmt struct {
	TableName string
	Values    []types.Value
}

func (s *InsertStmt) statementNode() {}

type SelectStmt struct {
	Fields    []string // empty/asterisk means all
	TableName string
	Join      *JoinClause
	Where     *WhereClause
	Limit     int
}

func (s *SelectStmt) statementNode() {}

type UpdateStmt struct {
	TableName string
	Set       map[string]types.Value
	Where     *WhereClause
}

func (s *UpdateStmt) statementNode() {}

type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStmt) statementNode() {}

// Clauses

// Expressions

type Expression interface {
	String() string
}

type InfixExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (e *InfixExpression) String() string {
	return "(" + e.Left.String() + " " + e.Operator + " " + e.Right.String() + ")"
}

type ComparisonExpression struct {
	Column   string // For now, left side is always column
	Operator string // =
	Value    types.Value
}

func (e *ComparisonExpression) String() string {
	return fmt.Sprintf("%s %s %v", e.Column, e.Operator, e.Value)
}

type WhereClause struct {
	Expr Expression
}

type JoinClause struct {
	Table   string
	OnLeft  string // table.col
	OnRight string // table.col
}
