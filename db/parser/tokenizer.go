package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenIllegal TokenType = iota
	TokenEOF
	TokenWS

	// Literals
	TokenIdent
	TokenString // 'value'
	TokenNumber // 123

	// Keywords
	TokenSelect
	TokenFrom
	TokenWhere
	TokenInsert
	TokenInto
	TokenValues
	TokenUpdate
	TokenSet
	TokenDelete
	TokenCreate
	TokenTable
	TokenPrimary
	TokenKey
	TokenUnique
	TokenJoin
	TokenOn
	TokenIntType
	TokenTextType
	TokenAnd // Minimal support if needed, though requirements only show simple conditions

	// Symbols
	TokenAsterisk // *
	TokenComma    // ,
	TokenLParen   // (
	TokenRParen   // )
	TokenEqual    // =
	TokenLimit
	TokenIf
	TokenNot
	TokenExists
)

type Token struct {
	Type    TokenType
	Literal string
}

func (t Token) String() string {
	return fmt.Sprintf("Token(%d, %q)", t.Type, t.Literal)
}

// Tokenizer scans a SQL string.
type Tokenizer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewTokenizer(input string) *Tokenizer {
	t := &Tokenizer{input: input}
	t.readChar()
	return t
}

func (t *Tokenizer) readChar() {
	if t.readPosition >= len(t.input) {
		t.ch = 0
	} else {
		t.ch = t.input[t.readPosition]
	}
	t.position = t.readPosition
	t.readPosition++
}

func (t *Tokenizer) skipWhitespace() {
	for unicode.IsSpace(rune(t.ch)) {
		t.readChar()
	}
}

func (t *Tokenizer) NextToken() Token {
	t.skipWhitespace()

	var tok Token

	switch t.ch {
	case 0:
		tok.Literal = ""
		tok.Type = TokenEOF
	case '*':
		tok = newToken(TokenAsterisk, t.ch)
	case ',':
		tok = newToken(TokenComma, t.ch)
	case '(':
		tok = newToken(TokenLParen, t.ch)
	case ')':
		tok = newToken(TokenRParen, t.ch)
	case '=':
		tok = newToken(TokenEqual, t.ch)
	case '\'':
		// String literal
		tok.Type = TokenString
		tok.Literal = t.readString()
		return tok // readString advances past quotes
	default:
		if isLetter(t.ch) {
			tok.Literal = t.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			return tok
		} else if isDigit(t.ch) {
			tok.Type = TokenNumber
			tok.Literal = t.readNumber()
			return tok
		} else {
			tok = newToken(TokenIllegal, t.ch)
		}
	}

	t.readChar()
	return tok
}

func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}

func (t *Tokenizer) readString() string {
	// skip opening quote
	t.readChar()
	position := t.position
	for t.ch != '\'' && t.ch != 0 {
		t.readChar()
	}
	// simple string reading, no escapes for now
	out := t.input[position:t.position]
	// skip closing quote
	if t.ch == '\'' {
		t.readChar()
	}
	return out
}

func (t *Tokenizer) readIdentifier() string {
	position := t.position
	for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
		t.readChar()
	}
	return t.input[position:t.position]
}

func (t *Tokenizer) readNumber() string {
	position := t.position
	for isDigit(t.ch) {
		t.readChar()
	}
	return t.input[position:t.position]
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '.'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

var keywords = map[string]TokenType{
	"SELECT":  TokenSelect,
	"FROM":    TokenFrom,
	"WHERE":   TokenWhere,
	"INSERT":  TokenInsert,
	"INTO":    TokenInto,
	"VALUES":  TokenValues,
	"UPDATE":  TokenUpdate,
	"SET":     TokenSet,
	"DELETE":  TokenDelete,
	"CREATE":  TokenCreate,
	"TABLE":   TokenTable,
	"PRIMARY": TokenPrimary,
	"KEY":     TokenKey,
	"UNIQUE":  TokenUnique,
	"JOIN":    TokenJoin,
	"ON":      TokenOn,
	"INT":     TokenIntType,
	"TEXT":    TokenTextType,
	"AND":     TokenAnd,
	"LIMIT":   TokenLimit,
	"IF":      TokenIf,
	"NOT":     TokenNot,
	"EXISTS":  TokenExists,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TokenIdent
}
