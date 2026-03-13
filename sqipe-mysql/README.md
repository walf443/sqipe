# sqipe-mysql

MySQL dialect for [sqipe](../README.md) query builder.

## Usage

```rust
use sqipe_mysql::sqipe;
use sqipe::col;

let mut q = sqipe("employee");
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` WHERE `name` = ?");
```

## Index hints

```rust
use sqipe_mysql::sqipe;

// FORCE INDEX
let mut q = sqipe("employee");
q.force_index(&["idx_name"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name) WHERE `name` = ?");

// USE INDEX (multiple)
let mut q = sqipe("employee");
q.use_index(&["idx_name", "idx_age"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` USE INDEX (idx_name, idx_age) WHERE `name` = ?");

// IGNORE INDEX
let mut q = sqipe("employee");
q.ignore_index(&["idx_old"]);
q.and_where(("name", "Alice"));
q.select(&["id", "name"]);
let (sql, binds) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `employee` IGNORE INDEX (idx_old) WHERE `name` = ?");
```

## STRAIGHT_JOIN

```rust
use sqipe_mysql::sqipe;
use sqipe::table;

let mut q = sqipe("users");
q.straight_join("orders", table("users").col("id").eq_col("user_id"));
q.select(&["id", "name"]);

let (sql, _) = q.to_sql();
assert_eq!(sql, "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id`");
```

## UPDATE

```rust
use sqipe_mysql::sqipe;
use sqipe::col;

let mut u = sqipe("users").update();
u.set("name", "Alice");
u.and_where(col("id").eq(1));

let (sql, binds) = u.to_sql();
assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
```
