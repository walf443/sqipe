#![cfg(feature = "test-sqlx")]

#[cfg(feature = "returning")]
use qbey::RawSql;
use qbey::{
    ConditionExpr, DeleteQueryBuilder, InsertQueryBuilder, LikeExpression, SelectQueryBuilder,
    UpdateQueryBuilder, col, count_all, exists, not, not_exists, qbey_from_subquery_with,
    qbey_with, row_number, table, window,
};
use sqlx::{Row, SqlitePool};

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

fn bind_params<'a>(
    mut query: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
    binds: &'a [SqliteValue],
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    for bind in binds {
        query = match bind {
            SqliteValue::Text(s) => query.bind(s.as_str()),
            SqliteValue::Integer(n) => query.bind(*n),
            SqliteValue::Real(f) => query.bind(*f),
        };
    }
    query
}

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

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name"]);
    let (sql, _binds) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_where_condition() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[0].get::<i64, _>("age"), 30);
}

#[tokio::test]
async fn test_order_by_and_limit() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
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

    let mut q = qbey_with::<SqliteValue>("users");
    q.join("orders", table("users").col("id").eq(col("user_id")));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let pool = setup_db().await;

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

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq(col("user_id")),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    // Alice=2 orders, Bob=1 order, Charlie=0 orders (NULL total)
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_between() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_not_between() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").not_between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Charlie (age=35) is outside [25, 30]
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_union() {
    let pool = setup_db().await;

    let mut q1 = qbey_with::<SqliteValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<SqliteValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2); // Charlie (35) and Bob (25)
}

#[tokio::test]
async fn test_in_subquery() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_in_subquery_with_outer_binds() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice (age=30 > 26, has shipped order) — Bob (age=25) filtered out by age > 26
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_not_in_subquery() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_exists_subquery() {
    let pool = setup_db().await;

    // All users when shipped orders exist (non-correlated EXISTS)
    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(exists(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // EXISTS is true (shipped orders exist), so all users are returned
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_not_exists_subquery() {
    let pool = setup_db().await;

    // No users when shipped orders exist (non-correlated NOT EXISTS)
    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(not_exists(sub));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // NOT EXISTS is false (shipped orders exist), so no users
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_exists_with_outer_binds() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(exists(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // EXISTS is true, age > 26 filters to Alice (30) and Charlie (35)
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_not_where() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_not_where_with_and() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").gt(24));
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Bob (age=25 > 24, not Alice), Charlie (age=35 > 24, not Alice)
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_from_subquery() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1); // Alice, total=100
    assert_eq!(rows[1].get::<i64, _>("user_id"), 2); // Bob, total=50
}

#[tokio::test]
async fn test_from_subquery_with_outer_where() {
    let pool = setup_db().await;

    let mut sub = qbey_with::<SqliteValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Only Alice's order (total=100) passes total > 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1);
}

#[tokio::test]
async fn test_like_contains() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_like_starts_with() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_like_ends_with() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ob")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_not_like() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_like_custom_escape_char() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_update_basic() {
    let pool = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Verify the update
    let rows = sqlx::query(r#"SELECT "name" FROM "users" WHERE "id" = 1"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
}

#[tokio::test]
async fn test_update_multiple_sets() {
    let pool = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "name", "age" FROM "users" WHERE "id" = 1"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
    assert_eq!(rows[0].get::<i64, _>("age"), 31);
}

#[tokio::test]
async fn test_update_from_query_with_where() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("id").eq(2));
    let mut u = q.into_update();
    u.set(col("name"), "Bobby");
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "name" FROM "users" WHERE "id" = 2"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Bobby");
}

#[tokio::test]
async fn test_update_allow_without_where() {
    let pool = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("age"), 99);
    u.allow_without_where();
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "age" FROM "users""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(rows.iter().all(|r| r.get::<i64, _>("age") == 99));
}

#[tokio::test]
async fn test_delete_basic() {
    let pool = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Verify Alice was deleted
    let rows = sqlx::query(r#"SELECT "id" FROM "users""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get::<i64, _>("id") != 1));
}

#[tokio::test]
async fn test_delete_from_query_with_where() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.and_where(col("age").lt(30));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Only Bob (age=25) should be deleted
    let rows = sqlx::query(r#"SELECT "name" FROM "users" ORDER BY "name" ASC"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_delete_allow_without_where() {
    let pool = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "id" FROM "users""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_count_all_with_reserved_word_alias() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.add_select(count_all().as_("count"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("count"), 3);
}

#[tokio::test]
async fn test_insert_single_row() {
    let pool = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    let (sql, binds) = ins.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "name", "age" FROM "users" WHERE "id" = 4"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[0].get::<i64, _>("age"), 40);
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let pool = setup_db().await;

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

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "name" FROM "users" WHERE "id" >= 4 ORDER BY "id" ASC"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[1].get::<String, _>("name"), "Eve");
}

#[tokio::test]
async fn test_insert_from_select() {
    let pool = setup_db().await;

    sqlx::query(
        "CREATE TABLE users_archive (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut sub = qbey_with::<SqliteValue>("users");
    sub.select(&["id", "name", "age"]);
    sub.and_where(col("age").gt(30));

    let mut ins = qbey_with::<SqliteValue>("users_archive").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query(r#"SELECT "name" FROM "users_archive" ORDER BY "name" ASC"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

// --- DISTINCT ---

#[tokio::test]
async fn test_distinct() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // orders has: shipped, pending, shipped → distinct gives: pending, shipped
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("status"), "pending");
    assert_eq!(rows[1].get::<String, _>("status"), "shipped");
}

// --- HAVING ---

#[tokio::test]
async fn test_having() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["user_id"]);
    q.having(col("cnt").gt(1));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Only Alice (user_id=1) has 2 orders
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1);
}

#[tokio::test]
async fn test_having_with_where() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.and_where(col("status").eq("shipped"));
    q.group_by(&["user_id"]);
    q.and_having(col("cnt").gt(0));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice (1 shipped) and Bob (1 shipped)
    assert_eq!(rows.len(), 2);
}

// ── Window functions ──

#[tokio::test]
async fn test_row_number_over() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64, _>("rn"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
    assert_eq!(rows[2].get::<i64, _>("rn"), 3);
    assert_eq!(rows[2].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_sum_over_partition() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<f64, _>("user_total"), 300.0);
    assert_eq!(rows[1].get::<f64, _>("user_total"), 300.0);
    assert_eq!(rows[2].get::<f64, _>("user_total"), 50.0);
}

#[tokio::test]
async fn test_count_over_partition() {
    let pool = setup_db().await;

    let mut q = qbey_with::<SqliteValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i32, _>("user_order_count"), 2);
    assert_eq!(rows[1].get::<i32, _>("user_order_count"), 2);
    assert_eq!(rows[2].get::<i32, _>("user_order_count"), 1);
}

#[tokio::test]
async fn test_cte() {
    let pool = setup_db().await;

    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id", "name", "age"]);
    cte_q.and_where(col("age").gt(28));

    let mut q = qbey_with::<SqliteValue>("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice"); // age 30
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie"); // age 35
}

#[tokio::test]
async fn test_cte_update() {
    let pool = setup_db().await;

    // CTE: users older than 28
    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id"]);
    cte_q.and_where(col("age").gt(28));

    // UPDATE users SET name = 'Senior' WHERE id IN (SELECT id FROM older_users)
    let mut cte_ref = qbey_with::<SqliteValue>("older_users");
    cte_ref.select(&["id"]);

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.with_cte("older_users", &[], cte_q);
    u.set(col("name"), "Senior");
    u.and_where(col("id").included(cte_ref));
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Verify the update: Alice(30) and Charlie(35) are > 28
    let rows = sqlx::query(r#"SELECT "name" FROM "users" WHERE "id" = 1"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Senior");
    let rows = sqlx::query(r#"SELECT "name" FROM "users" WHERE "id" = 3"#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Senior");
}

#[tokio::test]
async fn test_cte_delete() {
    let pool = setup_db().await;

    // CTE: users older than 30
    let mut cte_q = qbey_with::<SqliteValue>("users");
    cte_q.select(&["id"]);
    cte_q.and_where(col("age").gt(30));

    // DELETE FROM users WHERE id IN (SELECT id FROM old_users)
    let mut cte_ref = qbey_with::<SqliteValue>("old_users");
    cte_ref.select(&["id"]);

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.with_cte("old_users", &[], cte_q);
    d.and_where(col("id").included(cte_ref));
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Verify Charlie (age 35, > 30) was deleted via CTE
    let rows = sqlx::query(r#"SELECT "id" FROM "users" ORDER BY "id""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64, _>("id"), 1); // Alice
    assert_eq!(rows[1].get::<i64, _>("id"), 2); // Bob
}

#[tokio::test]
async fn test_named_window() {
    let pool = setup_db().await;

    let w = window().order_by(col("age").desc()).as_("w");

    let mut q = qbey_with::<SqliteValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(row_number().over(w.clone()).as_("rn"));
    q.add_select(col("age").sum_over(w).as_("running"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64, _>("rn"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
    assert_eq!(rows[2].get::<i64, _>("rn"), 3);
    assert_eq!(rows[2].get::<String, _>("name"), "Bob");
}

// ── RETURNING clause ──

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_returning() {
    let pool = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
        ("age", SqliteValue::Integer(40)),
    ]);
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("id"), 4);
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_multiple_rows_returning() {
    let pool = setup_db().await;

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

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[1].get::<String, _>("name"), "Eve");
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_insert_returning_with_col_expr() {
    let pool = setup_db().await;

    let mut ins = qbey_with::<SqliteValue>("users").into_insert();
    ins.add_value(&[
        ("id", SqliteValue::Integer(4)),
        ("name", SqliteValue::Text("Dave".to_string())),
    ]);
    ins.add_col_value_expr(col("age"), RawSql::new("20 + 20"));
    ins.returning(&[col("id"), col("age")]);
    let (sql, binds) = ins.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("id"), 4);
    assert_eq!(rows[0].get::<i64, _>("age"), 40);
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_update_returning() {
    let pool = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    u.returning(&[col("id"), col("name")]);
    let (sql, binds) = u.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("id"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_update_returning_multiple_rows() {
    let pool = setup_db().await;

    let mut u = qbey_with::<SqliteValue>("users").into_update();
    u.set(col("age"), 99);
    u.and_where(col("age").gte(30));
    u.returning(&[col("id"), col("name"), col("age")]);
    let (sql, binds) = u.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice(30) and Charlie(35) match age >= 30
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get::<i64, _>("age") == 99));
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_delete_returning() {
    let pool = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    d.returning(&[col("id"), col("name")]);
    let (sql, binds) = d.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("id"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");

    // Verify Alice was actually deleted
    let remaining = sqlx::query(r#"SELECT "id" FROM "users""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 2);
}

#[cfg(feature = "returning")]
#[tokio::test]
async fn test_delete_returning_multiple_rows() {
    let pool = setup_db().await;

    let mut d = qbey_with::<SqliteValue>("users").into_delete();
    d.and_where(col("age").gte(30));
    d.returning(&[col("name"), col("age")]);
    let (sql, binds) = d.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice(30) and Charlie(35)
    assert_eq!(rows.len(), 2);

    let remaining = sqlx::query(r#"SELECT "id" FROM "users""#)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1); // Only Bob remains
}
