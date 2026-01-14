# RDBMS Core Refactor - Test Script

This script tests the new RDBMS guarantees: PK uniqueness, FK constraints, and deterministic results.

## Prerequisites

Server must be running: `go run cmd/web/main.go`

## Test 1: Primary Key Uniqueness

```powershell
# Create a user
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{\"id\": 1, \"name\": \"Alice\", \"email\": \"alice@example.com\"}'

# Try to create duplicate PK (should fail)
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{\"id\": 1, \"name\": \"Bob\", \"email\": \"bob@example.com\"}'
```

**Expected**: Second request returns 500 error with "duplicate primary key"

## Test 2: Foreign Key Constraint

```powershell
# Try to create order with non-existent user_id (should fail)
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{\"id\": 100, \"user_id\": 999, \"amount\": 50, \"description\": \"Test order\"}'

# Create valid user first
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{\"id\": 2, \"name\": \"Charlie\", \"email\": \"charlie@example.com\"}'

# Now create order with valid user_id (should succeed)
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{\"id\": 101, \"user_id\": 2, \"amount\": 75, \"description\": \"Valid order\"}'
```

**Expected**: First order fails with FK violation, second succeeds after user creation

## Test 3: Deterministic Results

```powershell
# Query users multiple times
curl http://localhost:8080/users
curl http://localhost:8080/users
curl http://localhost:8080/users
```

**Expected**: All three responses return users in the same order (sorted by PK)

## Test 4: Unique Constraint

```powershell
# Try to create user with duplicate name (should fail)
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{\"id\": 3, \"name\": \"Alice\", \"email\": \"alice2@example.com\"}'
```

**Expected**: Error with "duplicate unique value"
