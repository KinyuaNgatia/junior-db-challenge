# INNER JOIN Implementation Guide

## Overview

This document explains how the INNER JOIN operation works in the Mini RDBMS, providing a clear understanding of the relational algebra semantics and implementation details.

## Relational Algebra Foundation

### Formal Definition

Given two relations **R** (Left) and **S** (Right), and a join condition **θ** (theta), the INNER JOIN produces a new relation containing all combinations of rows from R and S where θ evaluates to true.

**Formally**: `R ⋈_θ S = { r ∪ s | r ∈ R ∧ s ∈ S ∧ θ(r,s) }`

Where:

- `⋈` (bowtie) is the JOIN operator
- `θ` is the join predicate (in our case, equality: `LeftCol = RightCol`)
- `r ∪ s` is the concatenation of matching rows
- `r ∈ R` means "for each row r in relation R"
- `∧` means "and"

### INNER JOIN Semantics

**Critical Property**: Only rows that have matching values in both tables appear in the result.

**Non-matching rows are EXCLUDED**. This is what makes it an "INNER" join.

---

## Implementation: Nested Loop Join

### Algorithm

```
For each row r in Left table:
    For each row s in Right table:
        If r[LeftCol] == s[RightCol]:
            Combine r and s
            Add to result
```

### Complexity Analysis

- **Time Complexity**: O(|R| × |S|)
  - Where |R| = number of rows in Left table
  - Where |S| = number of rows in Right table
- **Space Complexity**: O(|R| + |S| + |Result|)
  - Must materialize both input tables
  - Result size depends on number of matches

### Why Nested Loop?

- **Simplicity**: Easy to understand and implement correctly
- **Correctness**: Guaranteed to find all matches
- **Suitable for small datasets**: Our in-memory DB targets small-medium data
- **No index required**: Works even without indexes on join columns

---

## Practical Example

### Input Tables

**users** table:
| id | name | email |
|----|---------|-------------------|
| 1 | Alice | alice@example.com |
| 2 | Bob | bob@example.com |

**orders** table:
| id | user_id | amount | description |
|-----|---------|--------|------------------|
| 100 | 1 | 50 | Stainless Sufuria|
| 101 | 3 | 75 | Cooking Pot |
| 102 | 1 | 120 | Frying Pan |

### JOIN Query

```sql
SELECT orders.id, orders.description, users.name, orders.amount
FROM orders
JOIN users ON orders.user_id = users.id
```

### Execution Steps

**Step 1**: Materialize left table (orders)

```
[
  {id: 100, user_id: 1, amount: 50, description: "Stainless Sufuria"},
  {id: 101, user_id: 3, amount: 75, description: "Cooking Pot"},
  {id: 102, user_id: 1, amount: 120, description: "Frying Pan"}
]
```

**Step 2**: Materialize right table (users)

```
[
  {id: 1, name: "Alice", email: "alice@example.com"},
  {id: 2, name: "Bob", email: "bob@example.com"}
]
```

**Step 3**: Nested loop comparison

| Left Row (order) | Right Row (user) | user_id == id? | Action                   |
| ---------------- | ---------------- | -------------- | ------------------------ |
| 100 (user_id=1)  | 1 (id=1)         | ✅ YES         | **MATCH** - Combine rows |
| 100 (user_id=1)  | 2 (id=2)         | ❌ NO          | Skip                     |
| 101 (user_id=3)  | 1 (id=1)         | ❌ NO          | Skip                     |
| 101 (user_id=3)  | 2 (id=2)         | ❌ NO          | Skip                     |
| 102 (user_id=1)  | 1 (id=1)         | ✅ YES         | **MATCH** - Combine rows |
| 102 (user_id=1)  | 2 (id=2)         | ❌ NO          | Skip                     |

**Step 4**: Result

| orders.id | orders.description | users.name | orders.amount |
| --------- | ------------------ | ---------- | ------------- |
| 100       | Stainless Sufuria  | Alice      | 50            |
| 102       | Frying Pan         | Alice      | 120           |

**Notice**: Order 101 (user_id=3) is **EXCLUDED** because user 3 doesn't exist. This is INNER JOIN behavior.

---

## Guarantees

### 1. Referential Integrity at Query Time

Orders without valid users **never appear** in joined results. This enforces data consistency even if FK constraints weren't checked at insert time.

### 2. Deterministic Results

Results are **always in the same order** because:

- Input rows are sorted by primary key (via `GetSnapshot()`)
- Slice iteration order is stable (not map iteration)
- Join condition is deterministic (equality check)

### 3. Type Safety

The `Compare()` method ensures type-safe comparisons:

- INT values are compared numerically
- TEXT values are compared lexicographically
- Type mismatches return errors

---

## Code Location

- **Implementation**: [`db/engine/planner.go`](file:///c:/Users/Kinyua%20Ngatia/.gemini/antigravity/scratch/mini-rdbms/db/engine/planner.go) (JoinNode struct)
- **Usage**: [`cmd/web/main.go`](file:///c:/Users/Kinyua%20Ngatia/.gemini/antigravity/scratch/mini-rdbms/cmd/web/main.go) (handleOrders function)

---

## Future Optimizations

### Hash Join

For larger datasets, a hash join would be more efficient:

- **Time Complexity**: O(|R| + |S|) average case
- **Requires**: Hash table construction on join column
- **Trade-off**: More memory, more complex implementation

### Index Nested Loop Join

If the right table has an index on the join column:

- **Time Complexity**: O(|R| × log|S|)
- **Benefit**: Avoid full scan of right table for each left row
- **Current Status**: Noted in code comments but not implemented

---

## Summary

The INNER JOIN implementation prioritizes **correctness** and **simplicity** over raw performance. It guarantees:

- ✅ Only matching rows appear in results
- ✅ Deterministic, repeatable output
- ✅ Type-safe comparisons
- ✅ Clear, readable code suitable for junior developers
