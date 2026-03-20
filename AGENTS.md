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

## Design Principles

- **Safety by default** — UPDATE/DELETE without WHERE is a compile error (type-state pattern). LIKE requires `LikeExpression`. Raw SQL requires `RawSql` wrapper.
- **Driver agnostic** — Works with any database driver via the `Dialect` trait.
- **No macro DSL** — Query composition uses plain Rust `if`/`match`.
- Doctests in `README.md` and `lib.rs` serve as both documentation and tests — keep them in sync.

## Code Style

- Follow `cargo fmt` formatting
- All clippy warnings treated as errors (`-D warnings`)
- Tests are organized per-feature in separate files under `tests/`
