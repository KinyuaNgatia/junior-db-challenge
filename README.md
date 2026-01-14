# Mini RDBMS: Lightweight SQL Engine in Go

## Project Objective

This project is a bottom-up implementation of a relational database management system designed to demonstrate core architectural principles of data storage, indexing, and query execution. It exists to provide a clean, readable codebase for understanding how SQL strings are parsed into execution plans and how ACID-lite properties can be achieved in a simplified embedded environment.

## Architecture Overview

The system follows a classic decoupled architecture, separating the front-end query interface from the back-end storage engine.

```mermaid
graph TD
    A[Client: REPL / Web UI] -->|SQL String| B[Parser]
    B -->|AST| C[Query Planner]
    C -->|Execution Plan| D[Executor]
    D -->|Row Operations| E[Storage Engine]
    E <-->|JSON Serialization| F[Disk: /data/*.json]
    E <-->|O(1) Lookups| G[Hash Indices]
```

### 1. Engine & Data Flow

- **Parser**: A recursive descent parser that tokenizes SQL and builds an Abstract Syntax Tree (AST).
- **Planner**: Analyzes the AST to determine the optimal access path. It distinguishes between **Index Scans** (for Primary Key/Unique lookups) and **Full Table Scans**.
- **Executor**: A push-based execution model that processes rows according to the plan. It handles relational algebra operations like `Filter`, `Project`, and `Nested Loop Join`.

### 2. UI Layer

- **REPL**: A CLI tool for direct low-level interaction.
- **API/Web**: A Go net/http server providing RESTful access to the engine, serving a modern dashboard for CRUD visualization.

## Supported Database Operations

| Category | Supported Syntax / Operations                                                            |
| :------- | :--------------------------------------------------------------------------------------- |
| **DDL**  | `CREATE TABLE` (INT, TEXT types), `PRIMARY KEY`, `UNIQUE` constraints.                   |
| **DML**  | `INSERT INTO`, `UPDATE ... SET ... WHERE`, `DELETE FROM ... WHERE`.                      |
| **DQL**  | `SELECT *`, `SELECT col1, col2`, `WHERE` (with `=`, `AND`, `OR`), `INNER JOIN`, `LIMIT`. |

## Data Integrity Guarantees

- **Entity Integrity**: Enforced via Primary Key constraints during insertion and update.
- **Domain Integrity**: Type checking for `INT` and `TEXT` fields during the execution phase.
- **Uniqueness**: Secondary Hash Indices prevent duplicate entries in columns marked `UNIQUE`.
- **Durability (Atomic Writes)**: The storage engine utilizes an **Atomic Rename** strategy. Data is written to a temporary file and renamed to the target `.json` file only upon successful write to ensure table files are never left in a corrupted state.

## Limitations and Intentional Trade-offs

- **JSON Persistence**: Chosen for transparency and ease of inspection at the cost of disk I/O and CPU overhead during serialization. Not suitable for O(N) scaling.
- **Single-Threaded Model**: The current engine uses coarse-grained locking. It is functional for concurrent web access but does not support high-concurrency write throughput.
- **In-Memory Primary State**: Data is fully loaded into memory. While this allows for extremely fast reads, the total dataset size is limited by available RAM.
- **Nested Loop Join**: Joins are implemented via nested loops (O(N\*M)). While efficient for small datasets, hash-joins or sort-merge joins would be required for production-scale loads.

## How to Run & Test Locally

### Prerequisites

- Go 1.23+

### 1. Launch the Engine (Web Dashboard)

```powershell
go run cmd/web/main.go
```

The dashboard will be available at `http://localhost:8080`.

### 2. Interactive REPL

```powershell
go run cmd/repl/main.go
```

### 3. Automated Verification

```powershell
# Run the integration test script
go run TEST_SCRIPT.go
# Or use the makefile if available
make test
```

### Storage Access

The physical table data is stored in the `./data` directory relative to the executable. Each table is represented by a readable `.json` file containing both the schema definition and its records.
