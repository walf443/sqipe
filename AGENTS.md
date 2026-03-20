# AGENTS.md

## Project Overview

qbey is a Rust SQL query builder library. It is a Cargo workspace with two crates:

- **qbey** (`qbey/`) — Core query builder supporting SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY, HAVING, UNION, subqueries, LIKE (with injection-safe `LikeExpression`), and a `qbey_schema!` macro for typed column references.
- **qbey-mysql** (`qbey-mysql/`) — MySQL dialect extension providing backtick quoting, index hints, and STRAIGHT_JOIN.

## Language & Tooling

- Rust (edition 2024)
- Build/test: `cargo`
- CI: GitHub Actions (`.github/workflows/ci.yml`)

## Building & Testing

```sh
# Build the entire workspace
cargo build --workspace --all-targets --features full

# Test each crate
cargo test -p qbey --features full
cargo test -p qbey-mysql --features full

# Format check
cargo fmt --all --check

# Lint
cargo clippy -p qbey --all-targets --features full -- -D warnings
cargo clippy -p qbey-mysql --all-targets --features full -- -D warnings
```

Use `cargo` commands directly rather than `act` or other CI runners for local verification.

## Workflow

1. Write tests for the new feature and verify they fail (TDD).
2. Implement the new feature.
3. Verify the new tests pass.
4. Run full tests (`cargo test -p qbey --features full && cargo test -p qbey-mysql --features full`).
5. Check clippy and fmt (`cargo clippy ... -- -D warnings && cargo fmt --all --check`).

When introducing new SQL syntax, write driver integration tests (sqlx, rusqlite, etc.) to verify the generated SQL executes correctly against real databases.

## Feature Flags

- `returning` — Enables RETURNING clause support (PostgreSQL, SQLite, MariaDB)
- `test-sqlx`, `test-sqlx-mysql`, `test-rusqlite`, `test-tokio-postgres`, `test-postgres` — Enable integration tests for specific drivers
- `full` — Enables all features above

## Project Structure

```
qbey/
  src/
    lib.rs          — Public API re-exports
    query.rs        — Core Query builder
    column.rs       — Column types and expressions
    join.rs         — JOIN support
    insert.rs       — INSERT builder
    update.rs       — UPDATE builder (type-state WHERE enforcement)
    delete.rs       — DELETE builder (type-state WHERE enforcement)
    like.rs         — LikeExpression (injection-safe LIKE patterns)
    raw_sql.rs      — RawSql wrapper for raw SQL injection points
    value.rs        — Value enum for bind parameters
    where_clause.rs — WHERE clause building
    schema.rs       — qbey_schema! macro
    prelude.rs      — Prelude module
    renderer/       — SQL rendering
  tests/            — Integration tests (per-feature, per-driver)
  benches/          — Benchmarks

qbey-mysql/
  src/
    lib.rs          — MySQL dialect and extensions
    select.rs       — MySQL SELECT (index hints, STRAIGHT_JOIN)
    insert.rs       — MySQL INSERT extensions
    update.rs       — MySQL UPDATE extensions
    delete.rs       — MySQL DELETE extensions
    index_hint.rs   — Index hint types
    *_tests.rs      — Unit tests
  tests/            — Integration tests (sqlx + MySQL/MariaDB)
```

## Design Principles

- **Safety by default** — UPDATE/DELETE without WHERE is a compile error (type-state pattern). LIKE requires `LikeExpression`. Raw SQL requires `RawSql` wrapper.
- **Driver agnostic** — Works with any database driver via the `Dialect` trait.
- **No macro DSL** — Query composition uses plain Rust `if`/`match`.
- Doctests in `README.md` and `lib.rs` serve as both documentation and tests — keep them in sync.

## Code Style

- Follow `cargo fmt` formatting
- All clippy warnings treated as errors (`-D warnings`)
- Tests are organized per-feature in separate files under `tests/`
