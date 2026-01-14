# Minimal RDBMS

A minimal, embedded Relational Database Management System (RDBMS) written in Go.
Designed for the Pesapal Junior Dev Challenge ’26.

## Overview

This project implements a functional RDBMS from scratch, featuring:

- **SQL-like Interface**: Support for CREATE, INSERT, SELECT, UPDATE, DELETE, and JOIN.
- **Interactive REPL**: A command-line shell for interacting with the database.
- **Persistent Storage**: JSON-based file persistence for tables.
- **Indexing**: Hash-based indexing for Primary Keys and Unique constraints.
- **Clean Architecture**: Separation of concerns into Parser, Engine, and Storage layers.

## Architecture

The system is organized into modular components:

```
cmd/
 ├── repl/       # Interactive Shell entry point
 └── web/        # Demo Web Application
db/
 ├── parser/     # Lexer and Recursive Descent Parser (AST generation)
 ├── engine/     # Query Planner and Executor
 ├── storage/    # In-memory Table/Row structure and Disk I/O
 ├── types/      # SQL Data Types (INT, TEXT)
 └── index/      # Hash Index implementation
```

### Key Components

1.  **Storage Engine**:

    - Tables are stored in memory as a map of `PrimaryKey -> Row`.
    - Persistence is achieved by serializing the table/schema to JSON files in the `data/` directory.
    - Each table has a dedicated file (e.g., `users.json`).

2.  **Indexing**:

    - A Hash Index is maintained for Primary Keys and Unique columns.
    - This allows O(1) lookups for `WHERE id = ?`.

3.  **Query Execution**:
    - **Parser**: Converts SQL strings into an Abstract Syntax Tree (AST).
    - **Planner**: select the best strategy (Index Scan vs Full Table Scan) and handles JOIN logic.
    - **Executor**: Iterates over the plan and produces a Result Set.

## Supported SQL Syntax

### Data Definition

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  name TEXT UNIQUE,
  email TEXT
);
```

### Data Manipulation

```sql
-- Insert
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');

-- Select
SELECT * FROM users;
SELECT name, email FROM users WHERE id = 1;

-- Join (Inner Join)
SELECT users.name, orders.amount
FROM users
JOIN orders ON users.id = orders.user_id;

-- Update
UPDATE users SET name = 'Bob' WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 1;
```

## Getting Started

### Prerequisites

- Go 1.23 or later

### Running the REPL

The REPL provides a persistent session to interact with the database.

```bash
# From the project root
go run cmd/repl/main.go
```

**Example Session:**

```
db> CREATE TABLE items (id INT PRIMARY KEY, name TEXT);
Table items created
db> INSERT INTO items VALUES (10, 'Book');
Insert successful
db> SELECT * FROM items;
id  name
10  Book
```

### Running the Web Application

The web app demonstrates a real-world use case (Users & Orders domain).

```bash
go run cmd/web/main.go
```

The server starts on `http://localhost:8080`.

**Endpoints:**

- `POST /users` (Create user)
- `GET /users` (List users)
- `POST /orders` (Create order)
- `GET /orders` (List orders, supports `?details=true` for JOIN)

## Design Decisions & Trade-offs

- **JSON Storage**: Chosen for human readability and simple debugging. **Trade-off**: Performance overhead on Save/Load compared to binary format; not suitable for massive datasets.
- **Hash Indexing**: Simple O(1) lookup. **Trade-off**: Does not support Range queries (e.g., `> 100`) or efficient ordering.
- **No Concurrency Control**: Single-threaded logical model. **Trade-off**: Race conditions possible if multiple processes access the same files.
- **In-Memory + Flush**: Tables are loaded fully into memory and saved on every implementation. **Trade-off**: Very slow write performance for large tables, but ensures durability for this scale.

## Future Improvements

- Implement B-Tree Indexing for Range queries.
- Add Transaction support (Commit/Rollback).
- Improve Parser to support complex expressions (AND/OR, Math).
- Binary storage format for efficiency.
