# Changelog

## [0.2.1] - 2026-03-30

### Added

- `add_values()` method on `InsertQueryBuilder` trait for bulk row insertion. Accepts an iterator of `ToInsertRow` items, enabling ergonomic multi-row INSERTs.
- `ToInsertRow` trait documentation and `add_values` usage examples in README.

### Changed

- `add_values()` is now a default method on the `InsertQueryBuilder` trait.

### Dev Dependencies

- Updated `ctor` to 0.8.

## [0.2.0] - 2026-03-29

### Added

- `into_sql()` / `into_sql_with()` on all query types (`SelectQuery`, `InsertQuery`, `UpdateQuery`, `DeleteQuery`) and tree types (`SelectTree`, `InsertTree`, `UpdateTree`, `DeleteTree`). Consumes the query/tree to generate SQL without cloning bind values.
- `into_tree()` on all query types. Consumes the query and moves values into the AST tree instead of cloning.
- Benchmark suite for `into_sql` and render-only comparisons.

### Changed

- **Breaking:** `Dialect::placeholder()` now returns `Cow<'static, str>` instead of `String`. Fixed-string placeholders (`?` for MySQL/SQLite) no longer allocate.
- **Breaking:** `Renderer::render_select()` returns `String` instead of `(String, Vec<&V>)`. Renderers now use a bind counter (`usize`) internally instead of collecting bind references.
- **Breaking:** `render_insert()`, `render_update()`, `render_delete()` return `String` instead of `(String, Vec<&V>)`.
- **Breaking:** `render_order_by()` takes `&mut usize` instead of `&mut Vec<&V>`.
- **Breaking:** `RawSql::render()` takes `&mut usize` instead of `&mut Vec<&V>`.
- `to_sql()` / `to_sql_with()` now delegate to `into_sql_with()` internally (via `self.clone().into_sql_with()`). Existing API is unchanged.
- `to_tree()` now delegates to `into_tree()` internally (via `self.clone().into_tree()`).

### Performance

- `into_sql()` for bulk INSERT (100 rows) is ~29% faster than the previous `to_sql()` (zero bind clones).
- `to_sql()` for bulk INSERT is ~16% faster (bind counter replaces `Vec<&V>` allocation).
- Render phase for bulk INSERT is ~86% faster (bind clone elimination + `Cow` placeholders).

### Dependencies

- Updated `ctor` to 0.7.
