#![cfg(feature = "test-rusqlite")]

use qbey::{
    ConditionExpr, DeleteQueryBuilder, InsertQueryBuilder, LikeExpression, SelectQueryBuilder,
    ToInsertRow, UpdateQueryBuilder, col, count_all, qbey_from_subquery_with, qbey_with,
    row_number, table, window,
};
use rusqlite::{Connection, params_from_iter};

#[derive(Debug, Clone)]
enum SqliteValue {
    Text(String),
    Integer(i64),
    Real(f64),
}

impl From<&str> for SqliteValue {
    fn from(s: &str) -> Self {
        SqliteValue::Text(s.to_string())
    }
}

impl From<i32> for SqliteValue {
    fn from(n: i32) -> Self {
        SqliteValue::Integer(n as i64)
    }
}

impl From<i64> for SqliteValue {
    fn from(n: i64) -> Self {
        SqliteValue::Integer(n)
    }
}

impl From<f64> for SqliteValue {
    fn from(n: f64) -> Self {
        SqliteValue::Real(n)
    }
}

impl From<String> for SqliteValue {
    fn from(s: String) -> Self {
        SqliteValue::Text(s)
    }
}

fn to_rusqlite_params(binds: &[SqliteValue]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    binds
        .iter()
        .map(|v| -> Box<dyn rusqlite::types::ToSql> {
            match v {
                SqliteValue::Text(s) => Box::new(s.clone()),
                SqliteValue::Integer(n) => Box::new(*n),
                SqliteValue::Real(f) => Box::new(*f),
            }
        })
        .collect()
}

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
        INSERT INTO orders (id, user_id, total, status) VALUES (1, 1, 100.0, 'shipped'), (2, 1, 200.0, 'pending'), (3, 2, 50.0, 'shipped');",
    )
    .unwrap();

    conn
}

#[test]
fn test_basic_select() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[test]
fn test_where_condition() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String, i64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, "Alice".to_string(), 30));
}

#[test]
fn test_order_by_and_limit() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Charlie", "Alice"]);
}

#[test]
fn test_join() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String, f64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
}

#[test]
fn test_join_with_alias() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select(&cols);
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String, f64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1, "Alice");
}

#[test]
fn test_left_join() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Alice=2 orders, Bob=1 order, Charlie=0 orders (NULL → 1 row)
    assert_eq!(rows.len(), 4);
}

#[test]
fn test_between() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[test]
fn test_union() {
    let conn = setup_db();

    let mut q1 = qbey_with::<SqliteValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<SqliteValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names.len(), 2); // Charlie (35), Bob (25)
}

#[test]
fn test_in_subquery() {
    let conn = setup_db();

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[test]
fn test_in_subquery_with_outer_binds() {
    let conn = setup_db();

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Alice (age=30 > 26, has shipped order) — Bob (age=25) filtered out by age > 26
    assert_eq!(names, vec!["Alice"]);
}

#[test]
fn test_not_in_subquery() {
    let conn = setup_db();

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(names, vec!["Charlie"]);
}

#[test]
fn test_from_subquery() {
    let conn = setup_db();

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, f64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1); // Alice, total=100
    assert_eq!(rows[1].0, 2); // Bob, total=50
}

#[test]
fn test_from_subquery_with_outer_where() {
    let conn = setup_db();

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, f64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Only Alice's order (total=100) passes total > 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
}

#[test]
fn test_like_contains() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn test_like_starts_with() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice"]);
}

#[test]
fn test_not_like() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Bob"]);
}

#[test]
fn test_like_custom_escape_char() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let names: Vec<String> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get::<_, String>(1)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn test_update_basic() {
    let conn = setup_db();

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    // Verify the update
    let mut stmt = conn
        .prepare(r#"SELECT "name" FROM "users" WHERE "id" = 1"#)
        .unwrap();
    let name: String = stmt.query_row([], |row| row.get(0)).unwrap();
    assert_eq!(name, "Alicia");
}

#[test]
fn test_update_multiple_sets() {
    let conn = setup_db();

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name", "age" FROM "users" WHERE "id" = 1"#)
        .unwrap();
    let (name, age): (String, i64) = stmt
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap();
    assert_eq!(name, "Alicia");
    assert_eq!(age, 31);
}

#[test]
fn test_update_from_query_with_where() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").eq(2));
    let mut u = q.into_update();
    u.set(col("name"), "Bobby");
    let (sql, binds) = u.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name" FROM "users" WHERE "id" = 2"#)
        .unwrap();
    let name: String = stmt.query_row([], |row| row.get(0)).unwrap();
    assert_eq!(name, "Bobby");
}

#[test]
fn test_update_allow_without_where() {
    let conn = setup_db();

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("age"), 99);
    u.allow_without_where();
    let (sql, binds) = u.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    // All rows should be updated
    let mut stmt = conn.prepare(r#"SELECT "age" FROM "users""#).unwrap();
    let ages: Vec<i64> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(ages.iter().all(|&a| a == 99));
}

#[test]
fn test_delete_basic() {
    let conn = setup_db();

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    // Verify Alice was deleted
    let mut stmt = conn.prepare(r#"SELECT "id" FROM "users""#).unwrap();
    let ids: Vec<i64> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(!ids.contains(&1));
}

#[test]
fn test_delete_from_query_with_where() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").lt(30));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    // Only Bob (age=25) should be deleted
    let mut stmt = conn
        .prepare(r#"SELECT "name" FROM "users" ORDER BY "name" ASC"#)
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[test]
fn test_delete_allow_without_where() {
    let conn = setup_db();

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn.prepare(r#"SELECT "id" FROM "users""#).unwrap();
    let ids: Vec<i64> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(ids.len(), 0);
}

#[test]
fn test_count_all_with_reserved_word_alias() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.add_select(count_all().as_("count"));
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_insert_single_row() {
    let conn = setup_db();

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    let (sql, binds) = ins.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name", "age" FROM "users" WHERE "id" = 4"#)
        .unwrap();
    let (name, age): (String, i64) = stmt
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap();
    assert_eq!(name, "Dave");
    assert_eq!(age, 40);
}

#[test]
fn test_insert_multiple_rows() {
    let conn = setup_db();

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    ins.add_value(&[
        ("id", SqliteValue::Integer(5)),
        ("name", SqliteValue::Text("Eve".to_string())),
        ("age", SqliteValue::Integer(28)),
    ]);
    let (sql, binds) = ins.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name" FROM "users" WHERE "id" >= 4 ORDER BY "id" ASC"#)
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, vec!["Dave", "Eve"]);
}

#[test]
fn test_insert_from_select() {
    let conn = setup_db();

    // Create an archive table
    conn.execute_batch(
        "CREATE TABLE users_archive (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        );",
    )
    .unwrap();

    let mut sub = qbey_with::<SqliteValue>("users");
    sub.select(&["id", "name", "age"]);
    sub.and_where(col("age").gt(30));

    let mut ins = qbey_with::<SqliteValue>("users_archive").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name" FROM "users_archive" ORDER BY "name" ASC"#)
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, vec!["Charlie"]);
}

struct User {
    id: i64,
    name: String,
    age: i64,
}

impl ToInsertRow<SqliteValue> for User {
    fn to_insert_row(&self) -> Vec<(&'static str, SqliteValue)> {
        vec![
            ("id", self.id.into()),
            ("name", self.name.as_str().into()),
            ("age", self.age.into()),
        ]
    }
}

#[test]
fn test_insert_with_to_insert_row_trait() {
    let conn = setup_db();

    let users = vec![
        User {
            id: 4,
            name: "Dave".to_string(),
            age: 40,
        },
        User {
            id: 5,
            name: "Eve".to_string(),
            age: 28,
        },
    ];

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    for u in &users {
        ins.add_value(u);
    }
    let (sql, binds) = ins.to_sql();

    let params = to_rusqlite_params(&binds);
    conn.execute(&sql, params_from_iter(params.iter().map(|p| p.as_ref())))
        .unwrap();

    let mut stmt = conn
        .prepare(r#"SELECT "name", "age" FROM "users" WHERE "id" >= 4 ORDER BY "id" ASC"#)
        .unwrap();
    let rows: Vec<(String, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("Dave".to_string(), 40));
    assert_eq!(rows[1], ("Eve".to_string(), 28));
}

// --- DISTINCT ---

#[test]
fn test_distinct() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let statuses: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // orders has: shipped, pending, shipped → distinct gives: pending, shipped
    assert_eq!(statuses, vec!["pending", "shipped"]);
}

// --- HAVING ---

#[test]
fn test_having() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["user_id"]);
    q.having(col("cnt").gt(1));
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, i64)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Only Alice (user_id=1) has 2 orders
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (1, 2));
}

#[test]
fn test_having_with_where() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.and_where(col("status").eq("shipped"));
    q.group_by(&["user_id"]);
    q.and_having(col("cnt").gt(0));
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<i64> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            row.get(0)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Alice (1 shipped) and Bob (1 shipped)
    assert_eq!(rows.len(), 2);
}

// ── Window functions ──

#[test]
fn test_row_number_over() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (3, "Charlie".to_string(), 35, 1));
    assert_eq!(rows[1], (1, "Alice".to_string(), 30, 2));
    assert_eq!(rows[2], (2, "Bob".to_string(), 25, 3));
}

#[test]
fn test_sum_over_partition() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, i64, f64, f64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].3, 300.0); // order 1, user 1
    assert_eq!(rows[1].3, 300.0); // order 2, user 1
    assert_eq!(rows[2].3, 50.0); // order 3, user 2
}

#[test]
fn test_count_over_partition() {
    let conn = setup_db();

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, i64, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].2, 2); // order 1, user 1
    assert_eq!(rows[1].2, 2); // order 2, user 1
    assert_eq!(rows[2].2, 1); // order 3, user 2
}

#[test]
fn test_cte() {
    let conn = setup_db();

    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id", "name", "age"]);
    cte_q.and_where(col("age").gt(28));

    let mut q = qbey_with::<SqliteValue>("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string())); // age 30
    assert_eq!(rows[1], (3, "Charlie".to_string())); // age 35
}

#[test]
fn test_recursive_cte() {
    let conn = setup_db();

    // Create a simple hierarchy table
    conn.execute_batch(
        "CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            parent_id INTEGER
        );
        INSERT INTO categories (id, name, parent_id) VALUES
            (1, 'Root', NULL),
            (2, 'Child1', 1),
            (3, 'Child2', 1),
            (4, 'Grandchild1', 2);",
    )
    .unwrap();

    // Base case: root categories
    let mut base = qbey_with::<SqliteValue>("categories");
    base.select(&["id", "name", "parent_id"]);
    base.and_where(col("parent_id").eq(1));

    // Recursive case
    let c = table("c");
    let mut recursive = qbey_with::<SqliteValue>(table("categories").as_("c"));
    recursive.select(&[c.col("id"), c.col("name"), c.col("parent_id")]);
    recursive.join("tree", c.col("parent_id").eq_col(table("tree").col("id")));

    let cte_query = base.union_all(&recursive);

    let mut q = qbey_with::<SqliteValue>("tree");
    q.with_recursive_cte("tree", &["id", "name", "parent_id"], cte_query);
    q.select(&["id", "name"]);
    q.order_by(col("id").asc());
    let (sql, binds) = q.to_sql();

    let params = to_rusqlite_params(&binds);
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String)> = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Children of Root (id=1): Child1(2), Child2(3), Grandchild1(4)
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (2, "Child1".to_string()));
    assert_eq!(rows[1], (3, "Child2".to_string()));
    assert_eq!(rows[2], (4, "Grandchild1".to_string()));
}

#[test]
fn test_named_window() {
    let conn = setup_db();

    let w = window().order_by(col("age").desc()).as_("w");

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(row_number().over(w.clone()).as_("rn"));
    q.add_select(col("age").sum_over(w).as_("running"));
    let (sql, _) = q.to_sql();

    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(i64, String, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (3, "Charlie".to_string(), 35, 1));
    assert_eq!(rows[1], (1, "Alice".to_string(), 30, 2));
    assert_eq!(rows[2], (2, "Bob".to_string(), 25, 3));
}
