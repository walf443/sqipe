# qbey-mysql

MySQL dialect for [qbey](../README.md) query builder.

## Usage

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut q = qbey("employee");
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` WHERE `name` = ?");
```

## Index hints

```rust
use qbey_mysql::qbey;

// FORCE INDEX
let mut q = qbey("employee");
q.force_index(&["idx_name"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name) WHERE `name` = ?");

// USE INDEX (multiple)
let mut q = qbey("employee");
q.use_index(&["idx_name", "idx_age"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` USE INDEX (idx_name, idx_age) WHERE `name` = ?");

// IGNORE INDEX
let mut q = qbey("employee");
q.ignore_index(&["idx_old"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` IGNORE INDEX (idx_old) WHERE `name` = ?");
```

## STRAIGHT_JOIN

```rust
use qbey_mysql::qbey;
use qbey::table;

let mut q = qbey("users");
q.straight_join("orders", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id`");
```

## INSERT

```rust
use qbey_mysql::qbey;
use qbey::{col, Value, RawSql};

let mut ins = qbey("users").into_insert();
ins.add_value(&[("id", 1.into()), ("name", "Alice".into()), ("age", 30.into())]);
let (sql, binds) = ins.to_sql();
assert_eq!(sql, "INSERT INTO `users` (`id`, `name`, `age`) VALUES (?, ?, ?)");
```

### ON DUPLICATE KEY UPDATE

With bind values:

```rust
use qbey_mysql::qbey;
use qbey::{col, Value};

let mut ins = qbey("users").into_insert();
ins.add_value(&[("id", 1.into()), ("name", "Alice".into()), ("age", 30.into())]);
ins.on_duplicate_key_update(col("name"), "Alicia");
let (sql, binds) = ins.to_sql();
assert_eq!(
    sql,
    "INSERT INTO `users` (`id`, `name`, `age`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = ?"
);
```

With raw SQL expressions:

```rust
use qbey_mysql::qbey;
use qbey::{col, Value, RawSql};

let mut ins = qbey("users").into_insert();
ins.add_value(&[("id", 1.into()), ("name", "Alice".into()), ("age", 30.into())]);
ins.on_duplicate_key_update_expr(RawSql::new("`name` = CONCAT(`name`, '!')"));
ins.on_duplicate_key_update_expr(RawSql::new("`age` = `age` + 1"));
let (sql, binds) = ins.to_sql();
assert_eq!(
    sql,
    "INSERT INTO `users` (`id`, `name`, `age`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `name` = CONCAT(`name`, '!'), `age` = `age` + 1"
);
```

## UPDATE

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut u = qbey("users").into_update();
u.set(col("name"), "Alice");
u.and_where(col("id").eq(1));

let (sql, binds) = u.to_sql();
assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
```

By default, UPDATE without WHERE will panic. Use `allow_without_where()` to explicitly allow full-table updates:

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut u = qbey("users").into_update();
u.set(col("age"), 99);
u.allow_without_where();

let (sql, binds) = u.to_sql();
assert_eq!(sql, "UPDATE `users` SET `age` = ?");
```

MySQL supports `ORDER BY` and `LIMIT` in UPDATE statements (not available in standard SQL):

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut u = qbey("users").into_update();
u.set(col("status"), "inactive");
u.and_where(col("dept").eq("eng"));
u.order_by(col("created_at").asc());
u.limit(10);

let (sql, binds) = u.to_sql();
assert_eq!(sql, "UPDATE `users` SET `status` = ? WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10");
```

## DELETE

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut d = qbey("users").into_delete();
d.and_where(col("id").eq(1));

let (sql, binds) = d.to_sql();
assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
```

By default, DELETE without WHERE will panic. Use `allow_without_where()` to explicitly allow full-table deletes:

```rust
use qbey_mysql::qbey;

let mut d = qbey("users").into_delete();
d.allow_without_where();

let (sql, binds) = d.to_sql();
assert_eq!(sql, "DELETE FROM `users`");
```

MySQL supports `ORDER BY` and `LIMIT` in DELETE statements (not available in standard SQL):

```rust
use qbey_mysql::qbey;
use qbey::col;

let mut d = qbey("users").into_delete();
d.and_where(col("dept").eq("eng"));
d.order_by(col("created_at").asc());
d.limit(10);

let (sql, binds) = d.to_sql();
assert_eq!(sql, "DELETE FROM `users` WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10");
```
