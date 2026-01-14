package parser

import (
	"fmt"
	"mini-rdbms/db/schema"
	"mini-rdbms/db/types"
	"strconv"
)

type Parser struct {
	l         *Tokenizer
	curToken  Token
	peekToken Token
	errors    []string
}

func NewParser(l *Tokenizer) *Parser {
	p := &Parser{l: l}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %d, got %d ('%s') instead", t, p.peekToken.Type, p.peekToken.Literal)
	p.errors = append(p.errors, msg)
}

func (p *Parser) ParseStatement() (Statement, error) {
	switch p.curToken.Type {
	case TokenCreate:
		return p.parseCreate()
	case TokenInsert:
		return p.parseInsert()
	case TokenSelect:
		return p.parseSelect()
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.curToken.Literal)
	}
}

// CREATE TABLE name (col type [options], ...)
func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	if !p.expectPeek(TokenTable) {
		return nil, fmt.Errorf(p.errors[len(p.errors)-1])
	}

	// Optional IF NOT EXISTS
	if p.peekTokenIs(TokenIf) {
		p.nextToken() // IF
		if !p.expectPeek(TokenNot) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		if !p.expectPeek(TokenExists) {
			return nil, fmt.Errorf("expected EXISTS after NOT")
		}
	}

	if !p.expectPeek(TokenIdent) {
		return nil, fmt.Errorf(p.errors[len(p.errors)-1])
	}

	stmt := &CreateTableStmt{TableName: p.curToken.Literal}

	if !p.expectPeek(TokenLParen) {
		return nil, fmt.Errorf(p.errors[len(p.errors)-1])
	}

	for !p.curTokenIs(TokenRParen) {
		p.nextToken() // skip ( or ,
		if p.curTokenIs(TokenRParen) {
			break
		}

		// Column Name
		if p.curToken.Type != TokenIdent {
			return nil, fmt.Errorf("expected column name")
		}
		colName := p.curToken.Literal

		// Column Type
		p.nextToken()
		var colType types.DataType
		switch p.curToken.Type {
		case TokenIntType:
			colType = types.TypeInt
		case TokenTextType:
			colType = types.TypeText
		default:
			return nil, fmt.Errorf("invalid column type: %s", p.curToken.Literal)
		}

		col := schema.ColumnDef{Name: colName, Type: colType}

		// Options (PRIMARY KEY, UNIQUE)
		if p.peekTokenIs(TokenPrimary) {
			p.nextToken() // PRIMARY
			if !p.expectPeek(TokenKey) {
				return nil, fmt.Errorf("expected KEY after PRIMARY")
			}
			col.IsPrimary = true
		} else if p.peekTokenIs(TokenUnique) {
			p.nextToken()
			col.IsUnique = true
		}

		stmt.Columns = append(stmt.Columns, col)

		if !p.peekTokenIs(TokenComma) && !p.peekTokenIs(TokenRParen) {
			return nil, fmt.Errorf("expected comma or rparen, got %s", p.peekToken.Literal)
		}
		if p.peekTokenIs(TokenComma) {
			p.nextToken()
		}
	}

	return stmt, nil
}

// INSERT INTO table VALUES (val, ...)
func (p *Parser) parseInsert() (*InsertStmt, error) {
	if !p.expectPeek(TokenInto) {
		return nil, p.lastError()
	}
	if !p.expectPeek(TokenIdent) {
		return nil, p.lastError()
	}

	stmt := &InsertStmt{TableName: p.curToken.Literal}

	if !p.expectPeek(TokenValues) {
		return nil, p.lastError()
	}
	if !p.expectPeek(TokenLParen) {
		return nil, p.lastError()
	}

	for !p.curTokenIs(TokenRParen) {
		p.nextToken() // skip ( or ,
		if p.curTokenIs(TokenRParen) {
			break
		}

		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, val)

		if p.peekTokenIs(TokenComma) {
			p.nextToken()
		}
	}
	return stmt, nil
}

// SELECT col1, col2 FROM table [JOIN table2 ON c1=c2] [WHERE col=val]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}
	// Fields
	p.nextToken() // skip SELECT
	for {
		if p.curTokenIs(TokenAsterisk) {
			// * mean all fields, represented as nil or empty? Let's use empty slice implies all, or specific convention.
			// But we need to support specific fields.
			// Let's store "*" as a field name for now or handle in executor.
			stmt.Fields = append(stmt.Fields, "*")
		} else if p.curToken.Type == TokenIdent {
			stmt.Fields = append(stmt.Fields, p.curToken.Literal)
			// Handle table.column? Tokenizer splits `.`? No, tokenizer `readIdentifier` only alpha+digits+_.
			// If we want table.col, we need `TokenDot`. For now requirement uses `users.name` which contains dot.
			// My Tokenizer `readIdentifier` doesn't include dot.
			// Oops. I need to fix Tokenizer or handle qualified names.
			// "users.name" -> IDENT DOT IDENT.
			// Or simpler: include dot in identifier for now.
		} else {
			return nil, fmt.Errorf("expected field name, got %s", p.curToken.Literal)
		}

		if p.peekTokenIs(TokenComma) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}

	if !p.expectPeek(TokenFrom) {
		return nil, p.lastError()
	}
	if !p.expectPeek(TokenIdent) {
		return nil, p.lastError()
	}
	stmt.TableName = p.curToken.Literal

	// JOIN
	if p.peekTokenIs(TokenJoin) {
		p.nextToken() // JOIN
		if !p.expectPeek(TokenIdent) {
			return nil, p.lastError()
		}
		joinTable := p.curToken.Literal

		if !p.expectPeek(TokenOn) {
			return nil, p.lastError()
		}

		// ON left = right
		p.nextToken()
		left := p.curToken.Literal // Assuming simple identifier, maybe qualified
		if !p.expectPeek(TokenEqual) {
			return nil, p.lastError()
		}
		p.nextToken()
		right := p.curToken.Literal

		stmt.Join = &JoinClause{
			Table:   joinTable,
			OnLeft:  left,
			OnRight: right,
		}
	}

	// WHERE
	if p.peekTokenIs(TokenWhere) {
		p.nextToken()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// LIMIT
	if p.peekTokenIs(TokenLimit) {
		p.nextToken()
		if !p.expectPeek(TokenNumber) {
			return nil, p.lastError()
		}
		limit, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	// UPDATE table SET col = val WHERE ...
	if !p.expectPeek(TokenIdent) {
		return nil, p.lastError()
	}
	stmt := &UpdateStmt{TableName: p.curToken.Literal, Set: make(map[string]types.Value)}

	if !p.expectPeek(TokenSet) {
		return nil, p.lastError()
	}

	// col = val. Only one for now? "UPDATE users SET name = 'Bob' WHERE id = 1"
	p.nextToken() // SET
	if p.curToken.Type != TokenIdent {
		return nil, fmt.Errorf("expected col name")
	}
	col := p.curToken.Literal

	if !p.expectPeek(TokenEqual) {
		return nil, p.lastError()
	}
	p.nextToken()

	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	stmt.Set[col] = val

	// Check for comma for multiple sets? Requirements say "UPDATE users SET name = 'Bob'..." (singular).
	// Let's stick to singular or loop.

	if !p.expectPeek(TokenWhere) {
		return nil, fmt.Errorf("UPDATE requires WHERE")
	}
	where, err := p.parseWhere()
	if err != nil {
		return nil, err
	}
	stmt.Where = where

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	if !p.expectPeek(TokenFrom) {
		return nil, p.lastError()
	}
	if !p.expectPeek(TokenIdent) {
		return nil, p.lastError()
	}
	stmt := &DeleteStmt{TableName: p.curToken.Literal}

	if !p.expectPeek(TokenWhere) {
		return nil, fmt.Errorf("DELETE requires WHERE")
	}
	where, err := p.parseWhere()
	if err != nil {
		return nil, err
	}
	stmt.Where = where

	return stmt, nil
}

const (
	_ int = iota
	LOWEST
	SUM     // +
	PRODUCT // * -- not supporting math yet but standard precedence
	EQUALS  // =
	ANDOR   // AND OR -- usually AND > OR, but for simplified we can group or use levels
)

// Precedence table?
// For now, simple:
func (p *Parser) parseWhere() (*WhereClause, error) {
	p.nextToken() // WHERE

	expr, err := p.parseExpression(LOWEST)
	if err != nil {
		return nil, err
	}

	return &WhereClause{Expr: expr}, nil
}

func (p *Parser) parseExpression(precedence int) (Expression, error) {
	// Prefix ? We don't have prefix ops like - or ! yet.
	// We expect Identifier (Column)

	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	// Infix
	for p.peekTokenIs(TokenAnd) || p.peekTokenIs(TokenEqual) { // Equal is handled in comparison?
		// Wait, "col = val" IS the comparison.
		// "col = val AND col = val"
		// parseComparison parses "col = val" fully.
		// So we look for AND / OR.

		// If we see AND
		if p.peekTokenIs(TokenAnd) {
			p.nextToken()
			op := p.curToken.Literal // AND

			// Recursively parse right
			right, err := p.parseExpression(EQUALS) // Tightness?
			if err != nil {
				return nil, err
			}

			left = &InfixExpression{Left: left, Operator: op, Right: right}
			continue
		}
		break
	}

	return left, nil
}

func (p *Parser) parseComparison() (Expression, error) {
	// Expect: IDENT = VALUE
	if p.curToken.Type != TokenIdent {
		return nil, fmt.Errorf("expected column name, got %s", p.curToken.Literal)
	}
	col := p.curToken.Literal

	if !p.expectPeek(TokenEqual) {
		return nil, p.lastError()
	}
	// curToken is now =
	op := "="

	p.nextToken()
	val, err := p.parseValue()
	if err != nil {
		return nil, err
	}

	return &ComparisonExpression{Column: col, Operator: op, Value: val}, nil
}

func (p *Parser) parseValue() (types.Value, error) {
	// Current token should be the value
	switch p.curToken.Type {
	case TokenNumber:
		i, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			return types.Value{}, err
		}
		return types.Value{Type: types.TypeInt, Val: i}, nil
	case TokenString:
		return types.Value{Type: types.TypeText, Val: p.curToken.Literal}, nil
	default:
		return types.Value{}, fmt.Errorf("unexpected value type: %s", p.curToken.Literal)
	}
}

func (p *Parser) lastError() error {
	if len(p.errors) > 0 {
		return fmt.Errorf(p.errors[len(p.errors)-1])
	}
	return fmt.Errorf("unknown parse error")
}
