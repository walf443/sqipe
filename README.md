
# sqipe

pipe syntax based sql query builder

> **Note:** This project is in early development. APIs may change without backward compatibility.

## SYNOPSIS

### Basic usage

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");
q.and_where(("name", "Alice"));   // tuple shorthand for Eq
q.select(&["id", "name"]);

// Standard SQL (default placeholder: ?)
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ?");

// Pipe syntax SQL (default placeholder: ?)
let (sql, binds) = q.to_pipe_sql();
assert_eq!(sql, "FROM \"employee\" |> WHERE \"name\" = ? |> SELECT \"id\", \"name\"");
```

## Features

- **Standard SQL & pipe syntax** — Generate both traditional `SELECT ... FROM ... WHERE` and pipe syntax `FROM ... |> WHERE ... |> SELECT ...` from the same query builder
- **Driver agnostic** — Works with any database driver. Tested with [sqlx](https://github.com/launchbadge/sqlx) (SQLite, MySQL), [rusqlite](https://github.com/rusqlite/rusqlite), [tokio-postgres](https://github.com/sfackler/rust-postgres), and [postgres](https://github.com/sfackler/rust-postgres)
- **Extensible bind value types** — Use the built-in `Value` enum for quick prototyping, or define your own type with `sqipe_with::<V>()` to match your driver's parameter types
- **Dialect support** — Customize placeholder style (`?`, `$1`, ...) and identifier quoting via the `Dialect` trait. MySQL dialect is available as a separate crate:
  - [sqipe-mysql](./sqipe-mysql/README.md) — backtick quoting, index hints, STRAIGHT_JOIN
- **Dynamic query building** — Conditionally add WHERE clauses, JOINs, and other clauses at runtime

## API

### Comparison operators

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");
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
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");
q.and_where(col("age").between(20, 30));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?");

// NOT BETWEEN
let mut q = sqipe("employee");
q.and_where(col("age").not_between(20, 30));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" NOT BETWEEN ? AND ?");
```

### Range conditions

Rust range types are automatically converted to the appropriate SQL conditions.

```rust
# use sqipe::{sqipe, col};
// Inclusive range: BETWEEN
let (sql, _) = sqipe("t").and_where(col("age").in_range(20..=30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" BETWEEN ? AND ?");

// Exclusive range: >= AND <
let (sql, _) = sqipe("t").and_where(col("age").in_range(20..30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" >= ? AND \"age\" < ?");

// From range: >=
let (sql, _) = sqipe("t").and_where(col("age").in_range(20..)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" >= ?");

// To range: <
let (sql, _) = sqipe("t").and_where(col("age").in_range(..30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" < ?");

// To inclusive range: <=
let (sql, _) = sqipe("t").and_where(col("age").in_range(..=30)).to_sql();
assert_eq!(sql, "SELECT * FROM \"t\" WHERE \"age\" <= ?");
```

### Dynamic query building

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");

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
# use sqipe::{sqipe, col};
// Simple OR
let mut q = sqipe("employee");
q.and_where(("name", "Alice"));
q.or_where(col("role").eq("admin"));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? OR \"role\" = ?");
```

### Grouping conditions with any / all

```rust
# use sqipe::{sqipe, col, any, all};
let mut q = sqipe("employee");
q.and_where(("name", "Alice"));
q.and_where(any(col("role").eq("admin"), col("role").eq("manager")));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? AND (\"role\" = ? OR \"role\" = ?)");

// Combining all + any
let mut q = sqipe("employee");
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
# use sqipe::{sqipe, col, not, any};
// Function style
let mut q = sqipe("employee");
q.and_where(("name", "Alice"));
q.and_where(not(col("role").eq("admin")));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ? AND NOT (\"role\" = ?)");

// Operator style (! operator)
let mut q = sqipe("employee");
q.and_where(!col("role").eq("admin"));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");

// Combined with any/all
let mut q = sqipe("employee");
q.and_where(not(any(col("role").eq("admin"), col("role").eq("manager"))));
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT ((\"role\" = ? OR \"role\" = ?))");
```

### Aggregate / GROUP BY

```rust
# use sqipe::{sqipe, col, aggregate};
let mut q = sqipe("employee");
q.aggregate(&[
    aggregate::count_all().as_("cnt"),
    aggregate::sum("salary").as_("total_salary"),
]);
q.group_by(&["dept"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" FROM \"employee\" GROUP BY \"dept\"");

let (sql, binds) = q.to_pipe_sql();
assert_eq!(sql, "FROM \"employee\" |> AGGREGATE COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" GROUP BY \"dept\"");
```

Available aggregate functions: `count_all()`, `count(col)`, `sum(col)`, `avg(col)`, `min(col)`, `max(col)`, `expr(raw_sql)`.

### HAVING

`and_where` / `or_where` called after `aggregate()` automatically become HAVING conditions.

```rust
# use sqipe::{sqipe, col, aggregate};
let mut q = sqipe("employee");
q.and_where(col("active").eq(true));       // WHERE (before aggregate)
q.aggregate(&[aggregate::count_all().as_("cnt")]);
q.group_by(&["dept"]);
q.and_where(col("cnt").gt(5));             // HAVING (after aggregate)

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" WHERE \"active\" = ? GROUP BY \"dept\" HAVING \"cnt\" > ?");

let (sql, binds) = q.to_pipe_sql();
assert_eq!(sql, "FROM \"employee\" |> WHERE \"active\" = ? |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"dept\" |> WHERE \"cnt\" > ?");
```

### Order By

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");
q.select(&["id", "name", "age"]);
q.order_by(col("name").asc());
q.order_by(col("age").desc());

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC");

let (sql, binds) = q.to_pipe_sql();
assert_eq!(sql, "FROM \"employee\" |> SELECT \"id\", \"name\", \"age\" |> ORDER BY \"name\" ASC, \"age\" DESC");
```

### Limit / Offset

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("employee");
q.select(&["id", "name"]);
q.limit(10);
q.offset(20);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20");

let (sql, binds) = q.to_pipe_sql();
assert_eq!(sql, "FROM \"employee\" |> SELECT \"id\", \"name\" |> LIMIT 10 OFFSET 20");
```

### Method chaining

```rust
# use sqipe::{sqipe, col};
let (sql, binds) = sqipe("employee")
    .and_where(("name", "Alice"))
    .and_where(col("age").gt(20))
    .select(&["id", "name"])
    .to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ?");
```

### UNION / UNION ALL

```rust
# use sqipe::{sqipe, col, UnionQueryOps};
let mut q1 = sqipe("employee");
q1.and_where(("dept", "eng"));
q1.select(&["id", "name"]);

let mut q2 = sqipe("employee");
q2.and_where(("dept", "sales"));
q2.select(&["id", "name"]);

let uq = q1.union_all(&q2);
let (sql, binds) = uq.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ?");
```

### IN clause

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("users");
q.and_where(col("status").included(&["active", "pending"]));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" IN (?, ?)");
```

Empty lists are safely handled as `1 = 0`.

### NOT IN clause

```rust
# use sqipe::{sqipe, col};
let mut q = sqipe("users");
q.and_where(col("status").not_included(&["inactive", "banned"]));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" NOT IN (?, ?)");
```

Empty lists are safely handled as `1 = 1`.

Subqueries are also supported:

```rust
# use sqipe::{sqipe, col};
let mut sub = sqipe("orders");
sub.select(&["user_id"]);
sub.and_where(col("status").eq("cancelled"));

let mut q = sqipe("users");
q.and_where(col("id").not_included(sub));
q.select(&["id", "name"]);

let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE \"id\" NOT IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?)");
```

### LIKE / NOT LIKE

`LikeExpression` provides safe pattern construction with automatic escaping of `%` and `_` in user input.

```rust
# use sqipe::{sqipe, col, LikeExpression};
// contains: %...%
let (sql, _) = sqipe("users")
    .and_where(col("name").like(LikeExpression::contains("Ali")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// starts_with: ...%
let (sql, _) = sqipe("users")
    .and_where(col("name").like(LikeExpression::starts_with("Ali")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// ends_with: %...
let (sql, _) = sqipe("users")
    .and_where(col("name").like(LikeExpression::ends_with("ice")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#);

// NOT LIKE
let (sql, _) = sqipe("users")
    .and_where(col("name").not_like(LikeExpression::contains("Bob")))
    .to_sql();
assert_eq!(sql, r#"SELECT * FROM "users" WHERE "name" NOT LIKE ? ESCAPE '\'"#);
```

Raw strings are not accepted — `LikeExpression` must be used to prevent wildcard injection.

### JOIN

```rust
# use sqipe::{sqipe, col, table, join};
// INNER JOIN with ON
let mut q = sqipe("users");
q.join("orders", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\"");

// LEFT JOIN
let mut q = sqipe("users");
q.left_join("addresses", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\"");

// JOIN with USING
let mut q = sqipe("users");
q.join("orders", join::using_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\")");

// Multiple columns USING
let mut q = sqipe("users");
q.join("orders", join::using_cols(&["user_id", "tenant_id"]));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\", \"tenant_id\")");
```

#### Call order matters: WHERE before JOIN

The order of `and_where` / `or_where` and `join` / `left_join` calls affects the generated SQL.
In pipe syntax, this naturally produces `FROM ... |> WHERE ... |> JOIN ...`.
In standard SQL, a CTE (Common Table Expression) is automatically generated to preserve the intended semantics.

```rust
# use sqipe::{sqipe, col, table};
let mut q = sqipe("users");
q.and_where(col("age").gt(25));   // WHERE first
q.join("orders", table("users").col("id").eq_col("user_id"));  // then JOIN
q.select(&["id", "name"]);

// Pipe SQL: WHERE before JOIN is natural
let (sql, _) = q.to_pipe_sql();
assert_eq!(sql, r#"FROM "users" |> WHERE "age" > ? |> INNER JOIN "orders" ON "users"."id" = "orders"."user_id" |> SELECT "id", "name""#);

// Standard SQL: CTE is generated to filter before joining
let (sql, _) = q.to_sql();
assert_eq!(sql, r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id""#);
```

When `join` is called before `and_where` (the traditional order), no CTE is generated and standard SQL is produced as usual.

### Table aliases and qualified columns

```rust
# use sqipe::{sqipe, col, table};
let mut q = sqipe("users");
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
# use sqipe::{sqipe, col};
let mut q = sqipe("users");
q.add_select(col("name").as_("user_name"));

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT \"name\" AS \"user_name\" FROM \"users\"");
```

### MySQL dialect

See [sqipe-mysql](./sqipe-mysql/README.md) for MySQL-specific features (backtick quoting, index hints, STRAIGHT_JOIN, etc.).
