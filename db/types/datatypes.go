package types

import (
	"fmt"
)

// DataType represents the supported SQL types.
type DataType string

const (
	TypeInt  DataType = "INT"
	TypeText DataType = "TEXT"
)

// Value holds the dynamic data for a cell.
// In a real DB we might use a custom tagging/serialization,
// but for this mini-RDBMS `interface{}` is sufficient and idiomatic enough for the scope.
type Value struct {
	Type DataType
	Val  interface{}
}

// Check verifies if the internal Val matches the Type.
func (v Value) Check() error {
	switch v.Type {
	case TypeInt:
		if _, ok := v.Val.(int); !ok {
			// Try to cast if it's strictly a float or other number type that fits?
			// For now, strict int.
			return fmt.Errorf("expected INT, got type %T", v.Val)
		}
	case TypeText:
		if _, ok := v.Val.(string); !ok {
			return fmt.Errorf("expected TEXT, got type %T", v.Val)
		}
	default:
		return fmt.Errorf("unknown type: %s", v.Type)
	}
	return nil
}

// String returns a string representation of the value.
func (v Value) String() string {
	if v.Val == nil {
		return "NULL"
	}
	switch v.Type {
	case TypeInt:
		return fmt.Sprintf("%d", v.Val)
	case TypeText:
		return fmt.Sprintf("%s", v.Val)
	}
	return fmt.Sprintf("%v", v.Val)
}

// AsInt attempts to return the value as int.
func (v Value) AsInt() (int, error) {
	if v.Type != TypeInt {
		return 0, fmt.Errorf("not an INT")
	}
	i, ok := v.Val.(int)
	if !ok {
		// Fallback for JSON decoding which often treats numbers as float64
		if f, ok := v.Val.(float64); ok {
			return int(f), nil
		}
		return 0, fmt.Errorf("val is not int: %v", v.Val)
	}
	return i, nil
}

// AsText returns the value as string.
func (v Value) AsText() (string, error) {
	if v.Type != TypeText {
		return "", fmt.Errorf("not a TEXT")
	}
	s, ok := v.Val.(string)
	if !ok {
		return "", fmt.Errorf("val is not string: %v", v.Val)
	}
	return s, nil
}

// Compare returns -1 if v < other, 0 if v == other, 1 if v > other.
func (v Value) Compare(other Value) (int, error) {
	if v.Type != other.Type {
		return 0, fmt.Errorf("type mismatch: %s vs %s", v.Type, other.Type)
	}
	switch v.Type {
	case TypeInt:
		i1, _ := v.AsInt()
		i2, _ := other.AsInt()
		if i1 < i2 {
			return -1, nil
		}
		if i1 > i2 {
			return 1, nil
		}
		return 0, nil
	case TypeText:
		s1, _ := v.AsText()
		s2, _ := other.AsText()
		if s1 < s2 {
			return -1, nil
		}
		if s1 > s2 {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("unsupported comparison type: %s", v.Type)
}
