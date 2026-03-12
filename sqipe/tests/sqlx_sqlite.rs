#![cfg(feature = "test-sqlx")]

use sqipe::{col, sqipe, table};
use sqlx::{Row, SqlitePool};

async fn setup_db() -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.unwrap();

    sqlx::query(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            total REAL NOT NULL,
            status TEXT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query("INSERT INTO orders (id, user_id, total, status) VALUES (1, 1, 100.0, 'shipped'), (2, 1, 200.0, 'pending'), (3, 2, 50.0, 'shipped')")
        .execute(&pool)
        .await
        .unwrap();

    pool
}

#[tokio::test]
async fn test_basic_select() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    let (sql, _binds) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_where_condition() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql();

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            sqipe::Value::String(s) => query.bind(s.as_str()),
            sqipe::Value::Int(n) => query.bind(*n),
            sqipe::Value::Float(f) => query.bind(*f),
            sqipe::Value::Bool(b) => query.bind(*b),
        };
    }

    let rows = query.fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[0].get::<i64, _>("age"), 30);
}

#[tokio::test]
async fn test_order_by_and_limit() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
    assert_eq!(rows[1].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_join() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select_cols(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql();

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            sqipe::Value::String(s) => query.bind(s.as_str()),
            sqipe::Value::Int(n) => query.bind(*n),
            sqipe::Value::Float(f) => query.bind(*f),
            sqipe::Value::Bool(b) => query.bind(*b),
        };
    }

    let rows = query.fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select_cols(&cols);
    let (sql, binds) = q.to_sql();

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            sqipe::Value::String(s) => query.bind(s.as_str()),
            sqipe::Value::Int(n) => query.bind(*n),
            sqipe::Value::Float(f) => query.bind(*f),
            sqipe::Value::Bool(b) => query.bind(*b),
        };
    }

    let rows = query.fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select_cols(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    // Alice has 2 orders, Bob has 1 order, Charlie has 0 orders (NULL total)
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_between() {
    let pool = setup_db().await;

    let mut q = sqipe("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            sqipe::Value::String(s) => query.bind(s.as_str()),
            sqipe::Value::Int(n) => query.bind(*n),
            sqipe::Value::Float(f) => query.bind(*f),
            sqipe::Value::Bool(b) => query.bind(*b),
        };
    }

    let rows = query.fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_aggregate_count() {
    let pool = setup_db().await;

    let mut q = sqipe("orders");
    q.aggregate(&[sqipe::aggregate::count_all().as_("cnt")]);
    q.group_by(&["status"]);
    q.select(&["status"]);
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2); // shipped, pending
}

#[tokio::test]
async fn test_union() {
    let pool = setup_db().await;

    use sqipe::UnionQueryOps;

    let mut q1 = sqipe("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = sqipe("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql();

    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = match bind {
            sqipe::Value::String(s) => query.bind(s.as_str()),
            sqipe::Value::Int(n) => query.bind(*n),
            sqipe::Value::Float(f) => query.bind(*f),
            sqipe::Value::Bool(b) => query.bind(*b),
        };
    }

    let rows = query.fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2); // Charlie (35) and Bob (25)
}
