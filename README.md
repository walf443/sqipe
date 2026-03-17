
# qbey

sql query builder

> **Note:** This project is in early development. APIs may change without backward compatibility.

## SYNOPSIS

### Basic usage

```rust
# use qbey::{qbey, col, SelectQueryBuilder};
let mut q = qbey("employee");
q.and_where(("name", "Alice"));   // tuple shorthand for Eq
q.select(&["id", "name"]);

// Standard SQL (default placeholder: ?)
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ?");
```

The `prelude` module re-exports commonly needed traits so you can import them all at once:

```rust
# use qbey::{qbey, col, count_all};
use qbey::prelude::*;

let mut q = qbey("employee");
q.and_where(col("age").gt(20));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
```

## Features

- **Standard SQL** — Generate traditional `SELECT ... FROM ... WHERE` SQL from the query builder
- **Driver agnostic** — Works with any database driver. Tested with [sqlx](https://github.com/launchbadge/sqlx) (SQLite, MySQL), [rusqlite](https://github.com/rusqlite/rusqlite), [tokio-postgres](https://github.com/sfackler/rust-postgres), and [postgres](https://github.com/sfackler/rust-postgres)
- **Extensible bind value types** — Use the built-in `Value` enum for quick prototyping, or define your own type with `qbey_with::<V>()` to match your driver's parameter types
- **Dialect support** — Customize placeholder style (`?`, `$1`, ...) and identifier quoting via the `Dialect` trait. MySQL dialect is available as a separate crate:
  - [qbey-mysql](./qbey-mysql/README.md) — backtick quoting, index hints, STRAIGHT_JOIN
- **Dynamic query building** — Conditionally add WHERE clauses, JOINs, and other clauses at runtime

## API

### Comparison operators

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.and_where(("name", "Alice"));               // tuple shorthand for Eq
q.and_where(col("age").gt(20));               // age > ?
q.and_where(col("age").lte(60));              // age <= ?
q.and_where(col("salary").lt(100000));        // salary < ?
q.and_where(col("level").gte(3));             // level >= ?
q.and_where(col("role").ne("intern"));        // role != ?
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ? AND \"age\" <= ? AND \"salary\" < ? AND \"level\" >= ? AND \"role\" != ?");
```

### BETWEEN / NOT BETWEEN

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.and_where(col("age").between(20, 30));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?");

// NOT BETWEEN
let mut q = qbey("employee");
q.and_where(col("age").not_between(20, 30));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" NOT BETWEEN ? AND ?");
```

### Range conditions

Rust range types are automatically converted to the appropriate SQL conditions.

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
// Inclusive range: BETWEEN
let (sql, _) = qbey("t").and_where(col("age").in_range(20..=30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" BETWEEN ? AND ?");

// Exclusive range: >= AND <
let (sql, _) = qbey("t").and_where(col("age").in_range(20..30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" >= ? AND \"age\" < ?");

// From range: >=
let (sql, _) = qbey("t").and_where(col("age").in_range(20..)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" >= ?");

// To range: <
let (sql, _) = qbey("t").and_where(col("age").in_range(..30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" < ?");

// To inclusive range: <=
let (sql, _) = qbey("t").and_where(col("age").in_range(..=30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" <= ?");
```

### Dynamic query building

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");

let name: Option<&str> = Some("Alice");
let min_age: Option<i32> = Some(20);

if let Some(name) = name {
    q.and_where(("name", name));
}
if let Some(min_age) = min_age {
    q.and_where(col("age").gt(min_age));
}

q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
```

### or_where

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
// Simple OR
let mut q = qbey("employee");
q.and_where(("name", "Alice"));
q.or_where(col("role").eq("admin"));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? OR \"role\" = ?");
```

### Grouping conditions with any / all

```rust
# use qbey::{qbey, col, any, all, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.and_where(("name", "Alice"));
q.and_where(any(col("role").eq("admin"), col("role").eq("manager")));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? AND (\"role\" = ? OR \"role\" = ?)");

// Combining all + any
let mut q = qbey("employee");
q.and_where(
    any(
        all(col("role").eq("admin"), col("dept").eq("eng")),
        all(col("role").eq("manager"), col("dept").eq("sales")),
    )
);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE (\"role\" = ? AND \"dept\" = ?) OR (\"role\" = ? AND \"dept\" = ?)");
```

### Negating conditions with not

```rust
# use qbey::{qbey, col, not, any, ConditionExpr, SelectQueryBuilder};
// Function style
let mut q = qbey("employee");
q.and_where(("name", "Alice"));
q.and_where(not(col("role").eq("admin")));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? AND NOT (\"role\" = ?)");

// Operator style (! operator)
let mut q = qbey("employee");
q.and_where(!col("role").eq("admin"));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");

// Combined with any/all
let mut q = qbey("employee");
q.and_where(not(any(col("role").eq("admin"), col("role").eq("manager"))));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT ((\"role\" = ? OR \"role\" = ?))");
```

### Aggregate / GROUP BY

```rust
# use qbey::{qbey, col, count_all, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["dept"]);
q.add_select(count_all().as_("cnt"));
q.group_by(&["dept"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" GROUP BY \"dept\"");
```

Raw SQL expressions can also be used for aggregate functions not yet covered by the builder API:

```rust
# use qbey::{qbey, col, RawSql, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["dept"]);
q.add_select_expr(RawSql::new("COUNT(*)"), Some("cnt"));
q.add_select_expr(RawSql::new("SUM(\"salary\")"), Some("total_salary"));
q.group_by(&["dept"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" FROM \"employee\" GROUP BY \"dept\"");
```

### HAVING

Aggregate expressions can be used directly in HAVING clauses, which is required for PostgreSQL compatibility (PostgreSQL does not allow SELECT aliases in HAVING):

```rust
# use qbey::{qbey, col, count_all, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["dept"]);
let cnt = count_all().as_("cnt");
q.add_select(cnt.clone());
q.group_by(&["dept"]);
q.having(cnt.gt(5));

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" GROUP BY \"dept\" HAVING COUNT(*) > ?");
```

For multiple conditions, use `and_having` / `or_having`:

```rust
# use qbey::{qbey, col, count_all, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["dept"]);
let cnt = count_all().as_("cnt");
let total = col("salary").sum().as_("total");
q.add_select(cnt.clone());
q.add_select(total.clone());
q.group_by(&["dept"]);
q.and_having(cnt.gt(5));
q.and_having(total.gt(100000));

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total\" FROM \"employee\" GROUP BY \"dept\" HAVING COUNT(*) > ? AND SUM(\"salary\") > ?");
```

### Order By

```rust
# use qbey::{qbey, col, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["id", "name", "age"]);
q.order_by(col("name").asc());
q.order_by(col("age").desc());

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC");
```

### Order By with raw SQL expression

Use `order_by_expr` to sort by a raw SQL expression (e.g., `RAND()`, `FIELD(...)`, `id DESC NULLS FIRST`).
The expression is rendered as-is, so the caller is responsible for including the sort direction if needed.
[`RawSql`] is required to make it explicit that raw SQL is being injected — **never pass user-supplied input**.

```rust
# use qbey::{qbey, col, RawSql, SelectQueryBuilder};
let mut q = qbey("users");
q.select(&["id", "name"]);
q.order_by_expr(RawSql::new("RAND()"));

let (sql, _) = q.to_sql();
assert_eq!(sql, r#"SELECT "id", "name" FROM "users" ORDER BY RAND()"#);
```

Column-based and expression-based ORDER BY can be mixed:

```rust
# use qbey::{qbey, col, RawSql, SelectQueryBuilder};
let mut q = qbey("users");
q.select(&["id", "name"]);
q.order_by(col("name").asc());
q.order_by_expr(RawSql::new("RAND()"));

let (sql, _) = q.to_sql();
assert_eq!(sql, r#"SELECT "id", "name" FROM "users" ORDER BY "name" ASC, RAND()"#);
```

### Limit / Offset

```rust
# use qbey::{qbey, col, SelectQueryBuilder};
let mut q = qbey("employee");
q.select(&["id", "name"]);
q.limit(10);
q.offset(20);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20");
```

### Method chaining

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let (sql, binds) = qbey("employee")
    .and_where(("name", "Alice"))
    .and_where(col("age").gt(20))
    .select(&["id", "name"])
    .to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ?");
```

### UNION / UNION ALL

`union()` / `union_all()` returns a new `Query`, so you can use the same `order_by()`, `limit()`, etc. on the result:

```rust
# use qbey::{qbey, col, SelectQueryBuilder};
let mut q1 = qbey("employee");
q1.and_where(("dept", "eng"));
q1.select(&["id", "name"]);

let mut q2 = qbey("employee");
q2.and_where(("dept", "sales"));
q2.select(&["id", "name"]);

let mut uq = q1.union_all(&q2);
uq.order_by(col("name").asc());
uq.limit(10);

let (sql, binds) = uq.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 10");
```

### IN clause

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("users");
q.and_where(col("status").included(&["active", "pending"]));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" IN (?, ?)");
```

Empty lists are safely handled as `1 = 0`.

### NOT IN clause

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("users");
q.and_where(col("status").not_included(&["inactive", "banned"]));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" NOT IN (?, ?)");
```

Empty lists are safely handled as `1 = 1`.

Subqueries are also supported:

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut sub = qbey("orders");
sub.select(&["user_id"]);
sub.and_where(col("status").eq("cancelled"));

let mut q = qbey("users");
q.and_where(col("id").not_included(sub));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"id\" NOT IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?)");
```

### LIKE / NOT LIKE

`LikeExpression` provides safe pattern construction with automatic escaping of `%` and `_` in user input.

```rust
# use qbey::{qbey, col, LikeExpression, ConditionExpr, SelectQueryBuilder};
// contains: %...%
let (sql, _) = qbey("users")
    .and_where(col("name").like(LikeExpression::contains("Ali")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// starts_with: ...%
let (sql, _) = qbey("users")
    .and_where(col("name").like(LikeExpression::starts_with("Ali")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// ends_with: %...
let (sql, _) = qbey("users")
    .and_where(col("name").like(LikeExpression::ends_with("ice")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// NOT LIKE
let (sql, _) = qbey("users")
    .and_where(col("name").not_like(LikeExpression::contains("Bob")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" NOT LIKE ? ESCAPE '\'"#);
```

Raw strings are not accepted — `LikeExpression` must be used to prevent wildcard injection.

### JOIN

```rust
# use qbey::{qbey, col, table, join, SelectQueryBuilder};
// INNER JOIN with ON
let mut q = qbey("users");
q.join("orders", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\"");

// LEFT JOIN
let mut q = qbey("users");
q.left_join("addresses", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\"");

// JOIN with USING
let mut q = qbey("users");
q.join("orders", join::using_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\")");

// Multiple columns USING
let mut q = qbey("users");
q.join("orders", join::using_cols(&["user_id", "tenant_id"]));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\", \"tenant_id\")");
```

### Table aliases and qualified columns

```rust
# use qbey::{qbey, col, table, SelectQueryBuilder};
let mut q = qbey("users");
q.as_("u");
q.join(
    table("orders").as_("o"),
    table("u").col("id").eq_col("user_id"),
);
q.select(&["id"]);
q.add_select(table("o").col("total").as_("order_total"));

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"o\".\"total\" AS \"order_total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\"");
```

### Column aliases

```rust
# use qbey::{qbey, col, SelectQueryBuilder};
let mut q = qbey("users");
q.add_select(col("name").as_("user_name"));

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"name\" AS \"user_name\" FROM \"users\"");
```

### Raw SQL expressions in SELECT

Use `add_select_expr` to include raw SQL expressions (e.g., function calls) in the SELECT list.
The expression is rendered as-is without quoting, so **never pass user-supplied input** to avoid SQL injection.

```rust
# use qbey::{qbey, col, RawSql, SelectQueryBuilder};
let mut q = qbey("users");
q.add_select(col("id"));
q.add_select_expr(RawSql::new("UPPER(\"name\")"), Some("upper_name"));
q.add_select_expr(RawSql::new("COALESCE(\"nickname\", \"name\")"), Some("display_name"));

let (sql, _) = q.to_sql();
assert_eq!(sql, r#"SELECT "id", UPPER("name") AS "upper_name", COALESCE("nickname", "name") AS "display_name" FROM "users""#);
```

### UPDATE

`Query::into_update()` converts a SELECT query builder into an UPDATE statement builder.

```rust
# use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
// Basic UPDATE
let mut u = qbey("employee").into_update();
u.set(col("name"), "Alice");
u.and_where(col("id").eq(1));

let (sql, binds) = u.to_sql();
assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
```

WHERE conditions can be built first, then converted to UPDATE:

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder, UpdateQueryBuilder};
let mut q = qbey("employee");
q.and_where(col("id").eq(1));
let mut u = q.into_update();
u.set(col("name"), "Alice");
u.set(col("age"), 31);

let (sql, binds) = u.to_sql();
assert_eq!(sql, r#"UPDATE "employee" SET "name" = ?, "age" = ? WHERE "id" = ?"#);
```

By default, calling `to_sql()` without any WHERE conditions will panic to prevent accidental full-table updates. Use `allow_without_where()` to explicitly opt in:

```rust
# use qbey::{qbey, col, UpdateQueryBuilder};
let mut u = qbey("employee").into_update();
u.set(col("status"), "inactive");
u.allow_without_where();

let (sql, binds) = u.to_sql();
assert_eq!(sql, r#"UPDATE "employee" SET "status" = ?"#);
```

Dialect support works via `to_sql_with`:

```rust
# use qbey::{qbey, col, ConditionExpr, Dialect, UpdateQueryBuilder};
# struct PgDialect;
# impl Dialect for PgDialect {
#     fn placeholder(&self, index: usize) -> String { format!("${}", index) }
# }
let mut u = qbey("employee").into_update();
u.set(col("name"), "Alice");
u.and_where(col("id").eq(1));

let (sql, binds) = u.to_sql_with(&PgDialect);
assert_eq!(sql, r#"UPDATE "employee" SET "name" = $1 WHERE "id" = $2"#);
```

For raw SQL expressions in SET clauses (e.g. incrementing a counter), use `RawSql`:

```rust
# use qbey::{qbey, col, ConditionExpr, RawSql, UpdateQueryBuilder};
let mut u = qbey("employee").into_update();
u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
u.and_where(col("id").eq(1));

let (sql, binds) = u.to_sql();
assert_eq!(sql, r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#);
```

### DELETE

`Query::into_delete()` converts a SELECT query builder into a DELETE statement builder.

```rust
# use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder};
// Basic DELETE
let mut d = qbey("employee").into_delete();
d.and_where(col("id").eq(1));

let (sql, binds) = d.to_sql();
assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
```

WHERE conditions can be built first, then converted to DELETE:

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
let mut q = qbey("employee");
q.and_where(col("id").eq(1));
let d = q.into_delete();

let (sql, binds) = d.to_sql();
assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
```

By default, calling `to_sql()` without any WHERE conditions will panic to prevent accidental full-table deletes. Use `allow_without_where()` to explicitly opt in:

```rust
# use qbey::{qbey, DeleteQueryBuilder};
let mut d = qbey("employee").into_delete();
d.allow_without_where();

let (sql, binds) = d.to_sql();
assert_eq!(sql, r#"DELETE FROM "employee""#);
```

### INSERT

`Query::into_insert()` converts a SELECT query builder into an INSERT statement builder.
Values are set using `add_value()` with column-value pairs.
Multiple rows can be inserted by calling `add_value()` multiple times.
Column order may differ between calls — values are automatically reordered to match the first call.
`add_col_value_expr()` appends a raw SQL expression (e.g., `NOW()`) to every row:

```rust
# use qbey::{qbey, col, Value, RawSql, InsertQueryBuilder};
let mut ins = qbey("employee").into_insert();
ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
ins.add_value(&[("age", 25.into()), ("name", "Bob".into())]);
ins.add_col_value_expr("created_at", RawSql::new("NOW()"));

let (sql, binds) = ins.to_sql();
assert_eq!(sql, r#"INSERT INTO "employee" ("name", "age", "created_at") VALUES (?, ?, NOW()), (?, ?, NOW())"#);
```

INSERT ... SELECT is also supported via `from_select()`:

```rust
# use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder, InsertQueryBuilder};
let mut sub = qbey("old_employee");
sub.select(&["name", "age"]);
sub.and_where(col("active").eq(true));

let mut ins = qbey("employee").into_insert();
ins.from_select(sub);

let (sql, binds) = ins.to_sql();
assert_eq!(sql, r#"INSERT INTO "employee" SELECT "name", "age" FROM "old_employee" WHERE "active" = ?"#);
```

Calling `to_sql()` without any `add_value()` or `from_select()` will panic.
When building rows from a dynamic collection, the caller is responsible for ensuring the collection is non-empty.

### MySQL dialect

See [qbey-mysql](./qbey-mysql/README.md) for MySQL-specific features (backtick quoting, index hints, STRAIGHT_JOIN, etc.).

# Example

You can see [walf443/isucon#3](https://github.com/walf443/isucon13/pull/3) for the practical example.
