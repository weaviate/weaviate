# GraphQL Int Coercion Fix

This document summarizes the work completed for issue [#8928](https://github.com/weaviate/weaviate/issues/8928), where GraphQL queries that supplied numeric variables for arguments such as `limit`, `offset`, or pagination inputs could panic at runtime.

## What was happening?

GraphQL variables originating from JSON payloads often arrive as `int64`, `json.Number`, or `float64`. The previous implementation performed direct type assertions (for example `value.(int)`), which failed when the value was not already a Go `int`. In the pagination and filtering code paths this triggered panics, breaking otherwise valid queries.

## How it was fixed

A dedicated helper, `graphqlutil.ToInt`, now centralises GraphQL Int coercion. It:

- Accepts common numeric representations (`int`, `int32`, `int64`, `float64` when integral, `json.Number`, and numeric strings).
- Enforces the GraphQL 32-bit integer bounds.
- Returns descriptive errors when coercion is not possible.

All GraphQL argument parsing that previously asserted `.(int)` now uses this helper, ensuring consistent and safe handling of numeric inputs across resolvers, filters, and module arguments.

## Tests covering the fix

- `adapters/handlers/graphql/local/graphqlutil/to_int_test.go`: Table-driven tests that validate successful coercion and error conditions for the helper.
- `entities/filters/pagination_test.go`: Exercises `ExtractPaginationFromArgs` with various numeric representations to confirm pagination extraction no longer panics and surfaces meaningful errors.

Together these tests confirm the helper behaves correctly and that downstream consumers handle mixed numeric types safely.
