package engine

import (
	"context"
	"os"
	"testing"
)

func TestEngineIntegration(t *testing.T) {
	// Cleanup previous test data
	os.RemoveAll("data")
	defer os.RemoveAll("data")

	e := NewEngine()
	ctx := context.Background()

	// 1. Create Table
	sql := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
	res, err := e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if res.Message != "Table users created" {
		t.Errorf("Unexpected message: %s", res.Message)
	}

	// 2. Insert Data
	sql = "INSERT INTO users VALUES (1, 'Alice')"
	_, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	sql = "INSERT INTO users VALUES (2, 'Bob')"
	_, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// 3. Select Data
	sql = "SELECT * FROM users"
	res, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(res.Rows))
	}

	// 4. Update Data
	sql = "UPDATE users SET name = 'Charlie' WHERE id = 1"
	_, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Verify Update
	sql = "SELECT * FROM users WHERE id = 1"
	res, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to select after update: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(res.Rows))
	}
	val, _ := res.Rows[0].Values[1].AsText()
	if val != "Charlie" {
		t.Errorf("Expected name 'Charlie', got '%s'", val)
	}

	// 5. Delete Data
	sql = "DELETE FROM users WHERE id = 2"
	_, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	sql = "SELECT * FROM users"
	res, err = e.Execute(ctx, sql)
	if err != nil {
		t.Fatalf("Failed to select after delete: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(res.Rows))
	}
}
