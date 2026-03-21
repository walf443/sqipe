#![cfg(feature = "test-libsql")]

use libsql::{Builder, Connection, Value};
#[cfg(feature = "returning")]
use qbey::RawSql;
use qbey::{
    ConditionExpr, DeleteQueryBuilder, InsertQueryBuilder, LikeExpression, SelectQueryBuilder,
    ToInsertRow, UpdateQueryBuilder, col, count_all, exists, not, not_exists,
    qbey_from_subquery_with, qbey_with, row_number, table, window,
};

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

fn to_libsql_params(binds: &[SqliteValue]) -> Vec<Value> {
    binds
        .iter()
        .map(|v| match v {
            SqliteValue::Text(s) => Value::Text(s.clone()),
            SqliteValue::Integer(n) => Value::Integer(*n),
            SqliteValue::Real(f) => Value::Real(*f),
        })
        .collect()
}

async fn setup_db() -> Connection {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

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
    .await
    .unwrap();

    conn
}

#[tokio::test]
async fn test_basic_select() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[tokio::test]
async fn test_where_condition() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, String, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
        ));
    }

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (1, "Alice".to_string(), 30));
}

#[tokio::test]
async fn test_order_by_and_limit() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Charlie", "Alice"]);
}

#[tokio::test]
async fn test_join() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.join("orders", table("users").col("id").eq(col("user_id")));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, String, f64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<f64>(2).unwrap(),
        ));
    }

    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq(col("user_id")),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select(&cols);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, String, f64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<f64>(2).unwrap(),
        ));
    }

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].1, "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq(col("user_id")),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut result: Vec<(i64, String)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
    }

    // Alice=2 orders, Bob=1 order, Charlie=0 orders (NULL -> 1 row)
    assert_eq!(result.len(), 4);
}

#[tokio::test]
async fn test_between() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn test_not_between() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").not_between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // Charlie (age=35) is outside [25, 30]
    assert_eq!(names, vec!["Charlie"]);
}

#[tokio::test]
async fn test_union() {
    let conn = setup_db().await;

    let mut q1 = qbey_with::<SqliteValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<SqliteValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names.len(), 2); // Charlie (35), Bob (25)
}

#[tokio::test]
async fn test_in_subquery() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn test_in_subquery_with_outer_binds() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // Alice (age=30 > 26, has shipped order) -- Bob (age=25) filtered out by age > 26
    assert_eq!(names, vec!["Alice"]);
}

#[tokio::test]
async fn test_not_in_subquery() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(names, vec!["Charlie"]);
}

#[tokio::test]
async fn test_exists_subquery() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(exists(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // EXISTS is true (shipped orders exist), so all users are returned
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[tokio::test]
async fn test_not_exists_subquery() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(not_exists(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // NOT EXISTS is false (shipped orders exist), so no users are returned
    assert_eq!(names, Vec::<String>::new());
}

#[tokio::test]
async fn test_exists_with_no_matching_rows() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("cancelled"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(exists(sub));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // EXISTS is false (no cancelled orders), so no users returned
    assert_eq!(names, Vec::<String>::new());
}

#[tokio::test]
async fn test_exists_with_outer_binds() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(exists(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // EXISTS is true, age > 26 filters to Alice (30) and Charlie (35)
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_not_where() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[tokio::test]
async fn test_not_where_with_and() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(24));
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    // Bob (age=25 > 24, not Alice), Charlie (age=35 > 24, not Alice)
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[tokio::test]
async fn test_from_subquery() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, f64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<f64>(1).unwrap()));
    }

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, 1); // Alice, total=100
    assert_eq!(result[1].0, 2); // Bob, total=50
}

#[tokio::test]
async fn test_from_subquery_with_outer_where() {
    let conn = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, f64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<f64>(1).unwrap()));
    }

    // Only Alice's order (total=100) passes total > 60
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 1);
}

#[tokio::test]
async fn test_like_contains() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_like_starts_with() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice"]);
}

#[tokio::test]
async fn test_like_ends_with() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ob")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Bob"]);
}

#[tokio::test]
async fn test_not_like() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Bob"]);
}

#[tokio::test]
async fn test_like_custom_escape_char() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }

    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_update_basic() {
    let conn = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    // Verify the update
    let mut rows = conn
        .query(r#"SELECT "name" FROM "users" WHERE "id" = 1"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let name: String = row.get::<String>(0).unwrap();
    assert_eq!(name, "Alicia");
}

#[tokio::test]
async fn test_update_multiple_sets() {
    let conn = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(r#"SELECT "name", "age" FROM "users" WHERE "id" = 1"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let name: String = row.get::<String>(0).unwrap();
    let age: i64 = row.get::<i64>(1).unwrap();
    assert_eq!(name, "Alicia");
    assert_eq!(age, 31);
}

#[tokio::test]
async fn test_update_from_query_with_where() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").eq(2));
    let mut u = q.into_update();
    u.set(col("name"), "Bobby");
    let u = u.where_set();
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(r#"SELECT "name" FROM "users" WHERE "id" = 2"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let name: String = row.get::<String>(0).unwrap();
    assert_eq!(name, "Bobby");
}

#[tokio::test]
async fn test_update_allow_without_where() {
    let conn = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("age"), 99);
    let u = u.allow_without_where();
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    // All rows should be updated
    let mut rows = conn
        .query(r#"SELECT "age" FROM "users""#, ())
        .await
        .unwrap();
    let mut ages: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        ages.push(row.get::<i64>(0).unwrap());
    }
    assert!(ages.iter().all(|&a| a == 99));
}

#[tokio::test]
async fn test_delete_basic() {
    let conn = setup_db().await;

    let d = qbey_with::<SqliteValue>("users")
        .into_delete()
        .and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    // Verify Alice was deleted
    let mut rows = conn.query(r#"SELECT "id" FROM "users""#, ()).await.unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        ids.push(row.get::<i64>(0).unwrap());
    }
    assert_eq!(ids.len(), 2);
    assert!(!ids.contains(&1));
}

#[tokio::test]
async fn test_delete_from_query_with_where() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").lt(30));
    let d = q.into_delete().where_set();
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    // Only Bob (age=25) should be deleted
    let mut rows = conn
        .query(r#"SELECT "name" FROM "users" ORDER BY "name" ASC"#, ())
        .await
        .unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

#[tokio::test]
async fn test_delete_allow_without_where() {
    let conn = setup_db().await;

    let d = qbey_with::<SqliteValue>("users")
        .into_delete()
        .allow_without_where();
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn.query(r#"SELECT "id" FROM "users""#, ()).await.unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        ids.push(row.get::<i64>(0).unwrap());
    }
    assert_eq!(ids.len(), 0);
}

#[tokio::test]
async fn test_count_all_with_reserved_word_alias() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.add_select(count_all().as_("count"));
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let count: i64 = row.get::<i64>(0).unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_insert_single_row() {
    let conn = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    let (sql, binds) = ins.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(r#"SELECT "name", "age" FROM "users" WHERE "id" = 4"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let name: String = row.get::<String>(0).unwrap();
    let age: i64 = row.get::<i64>(1).unwrap();
    assert_eq!(name, "Dave");
    assert_eq!(age, 40);
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let conn = setup_db().await;

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

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(
            r#"SELECT "name" FROM "users" WHERE "id" >= 4 ORDER BY "id" ASC"#,
            (),
        )
        .await
        .unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(names, vec!["Dave", "Eve"]);
}

#[tokio::test]
async fn test_insert_from_select() {
    let conn = setup_db().await;

    // Create an archive table
    conn.execute_batch(
        "CREATE TABLE users_archive (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        );",
    )
    .await
    .unwrap();

    let mut sub = qbey_with::<SqliteValue>("users");
    sub.select(&["id", "name", "age"]);
    sub.and_where(col("age").gt(30));

    let mut ins = qbey_with::<SqliteValue>("users_archive").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(
            r#"SELECT "name" FROM "users_archive" ORDER BY "name" ASC"#,
            (),
        )
        .await
        .unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(0).unwrap());
    }
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

#[tokio::test]
async fn test_insert_with_to_insert_row_trait() {
    let conn = setup_db().await;

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

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(
            r#"SELECT "name", "age" FROM "users" WHERE "id" >= 4 ORDER BY "id" ASC"#,
            (),
        )
        .await
        .unwrap();
    let mut result: Vec<(String, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<String>(0).unwrap(), row.get::<i64>(1).unwrap()));
    }
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], ("Dave".to_string(), 40));
    assert_eq!(result[1], ("Eve".to_string(), 28));
}

// --- DISTINCT ---

#[tokio::test]
async fn test_distinct() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut statuses: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        statuses.push(row.get::<String>(0).unwrap());
    }

    // orders has: shipped, pending, shipped -> distinct gives: pending, shipped
    assert_eq!(statuses, vec!["pending", "shipped"]);
}

// --- HAVING ---

#[tokio::test]
async fn test_having() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["user_id"]);
    q.having(col("cnt").gt(1));
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap()));
    }

    // Only Alice (user_id=1) has 2 orders
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (1, 2));
}

#[tokio::test]
async fn test_having_with_where() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.and_where(col("status").eq("shipped"));
    q.group_by(&["user_id"]);
    q.and_having(col("cnt").gt(0));
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push(row.get::<i64>(0).unwrap());
    }

    // Alice (1 shipped) and Bob (1 shipped)
    assert_eq!(result.len(), 2);
}

// -- Window functions --

#[tokio::test]
async fn test_row_number_over() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut result: Vec<(i64, String, i64, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
            row.get::<i64>(3).unwrap(),
        ));
    }

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (3, "Charlie".to_string(), 35, 1));
    assert_eq!(result[1], (1, "Alice".to_string(), 30, 2));
    assert_eq!(result[2], (2, "Bob".to_string(), 25, 3));
}

#[tokio::test]
async fn test_sum_over_partition() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut result: Vec<(i64, i64, f64, f64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<i64>(1).unwrap(),
            row.get::<f64>(2).unwrap(),
            row.get::<f64>(3).unwrap(),
        ));
    }

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].3, 300.0); // order 1, user 1
    assert_eq!(result[1].3, 300.0); // order 2, user 1
    assert_eq!(result[2].3, 50.0); // order 3, user 2
}

#[tokio::test]
async fn test_count_over_partition() {
    let conn = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut result: Vec<(i64, i64, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<i64>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
        ));
    }

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].2, 2); // order 1, user 1
    assert_eq!(result[1].2, 2); // order 2, user 1
    assert_eq!(result[2].2, 1); // order 3, user 2
}

#[tokio::test]
async fn test_cte() {
    let conn = setup_db().await;

    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id", "name", "age"]);
    cte_q.and_where(col("age").gt(28));

    let mut q = qbey_with::<SqliteValue>("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, String)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
    }

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, "Alice".to_string())); // age 30
    assert_eq!(result[1], (3, "Charlie".to_string())); // age 35
}

#[tokio::test]
async fn test_cte_update() {
    let conn = setup_db().await;

    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id"]);
    cte_q.and_where(col("age").gt(28));

    let mut cte_ref = qbey_with::<SqliteValue>("older_users");
    cte_ref.select(&["id"]);

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.with_cte("older_users", &[], cte_q);
    u.set(col("name"), "Senior");
    let u = u.and_where(col("id").included(cte_ref));
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    // Alice(30) and Charlie(35) are > 28
    let mut rows = conn
        .query(r#"SELECT "name" FROM "users" WHERE "id" = 1"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "Senior");

    let mut rows = conn
        .query(r#"SELECT "name" FROM "users" WHERE "id" = 3"#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "Senior");
}

#[tokio::test]
async fn test_cte_delete() {
    let conn = setup_db().await;

    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id"]);
    cte_q.and_where(col("age").gt(30));

    let mut cte_ref = qbey_with::<SqliteValue>("old_users");
    cte_ref.select(&["id"]);

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.with_cte("old_users", &[], cte_q);
    let d = d.and_where(col("id").included(cte_ref));
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    conn.execute(&sql, params).await.unwrap();

    let mut rows = conn
        .query(r#"SELECT COUNT(*) FROM "users""#, ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let count: i64 = row.get::<i64>(0).unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_recursive_cte() {
    let conn = setup_db().await;

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
    .await
    .unwrap();

    // Base case: root categories
    let mut base = qbey_with::<SqliteValue>("categories");
    base.select(&["id", "name", "parent_id"]);
    base.and_where(col("parent_id").eq(1));

    // Recursive case
    let c = table("c");
    let mut recursive = qbey_with::<SqliteValue>(table("categories").as_("c"));
    recursive.select(&[c.col("id"), c.col("name"), c.col("parent_id")]);
    recursive.join("tree", c.col("parent_id").eq(table("tree").col("id")));

    let cte_query = base.union_all(&recursive);

    let mut q = qbey_with::<SqliteValue>("tree");
    q.with_recursive_cte("tree", &["id", "name", "parent_id"], cte_query);
    q.select(&["id", "name"]);
    q.order_by(col("id").asc());
    let (sql, binds) = q.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut result: Vec<(i64, String)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
    }

    // Children of Root (id=1): Child1(2), Child2(3), Grandchild1(4)
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (2, "Child1".to_string()));
    assert_eq!(result[1], (3, "Child2".to_string()));
    assert_eq!(result[2], (4, "Grandchild1".to_string()));
}

#[tokio::test]
async fn test_named_window() {
    let conn = setup_db().await;

    let w = window().order_by(col("age").desc()).as_("w");

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(row_number().over(w.clone()).as_("rn"));
    q.add_select(col("age").sum_over(w).as_("running"));
    let (sql, _) = q.to_sql();

    let mut rows = conn.query(&sql, ()).await.unwrap();
    let mut result: Vec<(i64, String, i64, i64)> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        result.push((
            row.get::<i64>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<i64>(2).unwrap(),
            row.get::<i64>(3).unwrap(),
        ));
    }

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], (3, "Charlie".to_string(), 35, 1));
    assert_eq!(result[1], (1, "Alice".to_string(), 30, 2));
    assert_eq!(result[2], (2, "Bob".to_string(), 25, 3));
}

// -- RETURNING clause --

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_returning() {
    let conn = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 4);
    assert_eq!(row.get::<String>(1).unwrap(), "Dave");
    assert!(rows.next().await.unwrap().is_none());
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_multiple_rows_returning() {
    let conn = setup_db().await;

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
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(1).unwrap());
    }
    assert_eq!(names, vec!["Dave", "Eve"]);
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_returning_with_col_expr() {
    let conn = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
    ]);
    ins.add_col_value_expr(col("age"), RawSql::new("20 + 20"));
    ins.returning(&[col("id"), col("age")]);
    let (sql, binds) = ins.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 4);
    assert_eq!(row.get::<i64>(1).unwrap(), 40);
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_update_returning() {
    let conn = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    let mut u = u.and_where(col("id").eq(1));
    u.returning(&[col("id"), col("name")]);
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alicia");
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_update_returning_multiple_rows() {
    let conn = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("age"), 99);
    let mut u = u.and_where(col("age").gte(30));
    u.returning(&[col("id"), col("name"), col("age")]);
    let (sql, binds) = u.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut ages: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        ages.push(row.get::<i64>(2).unwrap());
    }
    // Alice(30) and Charlie(35) match age >= 30
    assert_eq!(ages.len(), 2);
    assert!(ages.iter().all(|&a| a == 99));
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_delete_returning() {
    let conn = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users")
        .into_delete()
        .and_where(col("id").eq(1));
    d.returning(&[col("id"), col("name")]);
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");

    // Verify Alice was actually deleted
    let mut rows = conn.query(r#"SELECT "id" FROM "users""#, ()).await.unwrap();
    let mut count = 0;
    while rows.next().await.unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_delete_returning_multiple_rows() {
    let conn = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users")
        .into_delete()
        .and_where(col("age").gte(30));
    d.returning(&[col("name"), col("age")]);
    let (sql, binds) = d.to_sql();

    let params = to_libsql_params(&binds);
    let mut rows = conn.query(&sql, params).await.unwrap();
    let mut count = 0;
    while rows.next().await.unwrap().is_some() {
        count += 1;
    }
    // Alice(30) and Charlie(35)
    assert_eq!(count, 2);

    // Only Bob remains
    let mut rows = conn.query(r#"SELECT "id" FROM "users""#, ()).await.unwrap();
    let mut remaining = 0;
    while rows.next().await.unwrap().is_some() {
        remaining += 1;
    }
    assert_eq!(remaining, 1);
}
