#![cfg(feature = "test-sqlx-mysql")]

use qbey::{
    ConditionExpr, DeleteQueryBuilder, InsertQueryBuilder, LikeExpression, SelectQueryBuilder,
    UpdateQueryBuilder, col, count_all, not, qbey_from_subquery_with, qbey_with, row_number, table,
    window,
};
use sqlx::{MySqlPool, Row};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

static DIALECT: qbey::MySqlDialect = qbey::MySqlDialect;

/// Custom value type for MySQL — maps directly to sqlx bind types.
#[derive(Debug, Clone)]
enum MysqlValue {
    Text(String),
    Int(i64),
    Float(f64),
}

impl From<&str> for MysqlValue {
    fn from(s: &str) -> Self {
        MysqlValue::Text(s.to_string())
    }
}

impl From<i32> for MysqlValue {
    fn from(n: i32) -> Self {
        MysqlValue::Int(n as i64)
    }
}

impl From<f64> for MysqlValue {
    fn from(n: f64) -> Self {
        MysqlValue::Float(n)
    }
}

impl From<String> for MysqlValue {
    fn from(s: String) -> Self {
        MysqlValue::Text(s)
    }
}

struct SharedContainer {
    _container: testcontainers::ContainerAsync<Mysql>,
    host_port: u16,
}

static SHARED_CONTAINER: tokio::sync::OnceCell<SharedContainer> =
    tokio::sync::OnceCell::const_new();
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn get_shared_container() -> &'static SharedContainer {
    SHARED_CONTAINER
        .get_or_init(|| async {
            let container = Mysql::default().start().await.unwrap();
            let host_port = container.get_host_port_ipv4(3306).await.unwrap();
            SharedContainer {
                _container: container,
                host_port,
            }
        })
        .await
}

async fn setup_pool() -> MySqlPool {
    let shared = get_shared_container().await;
    let db_id = DB_COUNTER.fetch_add(1, Relaxed);
    let db_name = format!("test_{}", db_id);

    let root_url = format!("mysql://root@127.0.0.1:{}", shared.host_port);
    let root_pool = MySqlPool::connect(&root_url).await.unwrap();

    sqlx::query(&format!("CREATE DATABASE `{}`", db_name))
        .execute(&root_pool)
        .await
        .unwrap();

    let url = format!("mysql://root@127.0.0.1:{}/{}", shared.host_port, db_name);
    let pool = MySqlPool::connect(&url).await.unwrap();

    sqlx::query(
        "CREATE TABLE users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE orders (
            id INT PRIMARY KEY AUTO_INCREMENT,
            user_id INT NOT NULL,
            total DOUBLE NOT NULL,
            status VARCHAR(255) NOT NULL
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

fn bind_params<'a>(
    mut query: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    binds: &'a [MysqlValue],
) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    for bind in binds {
        query = match bind {
            MysqlValue::Text(s) => query.bind(s.as_str()),
            MysqlValue::Int(n) => query.bind(*n),
            MysqlValue::Float(f) => query.bind(*f),
        };
    }
    query
}

// --- SELECT ---

#[tokio::test]
async fn test_basic_select() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_where_condition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
    assert_eq!(rows[1].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_join() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select(&cols);
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    // Alice=2 orders, Bob=1 order, Charlie=0 orders (NULL total)
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_between() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").not_between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q1 = qbey_with::<MysqlValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<MysqlValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2); // Charlie (35) and Bob (25)
}

// --- Subqueries ---

#[tokio::test]
async fn test_in_subquery() {
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_not_where() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").gt(24));
    q.and_where(not(col("name").eq("Alice")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Only Alice's order (total=100) passes total > 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1);
}

// --- LIKE ---

#[tokio::test]
async fn test_like_contains() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_like_ends_with() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ob")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_not_like() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_like_custom_escape_char() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

// --- FOR UPDATE / FOR SHARE ---

#[tokio::test]
async fn test_for_update() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update();
    let (sql, binds) = q.to_sql_with(&DIALECT);

    assert!(sql.ends_with("FOR UPDATE"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_for_update_with_nowait() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update_with("NOWAIT");
    let (sql, binds) = q.to_sql_with(&DIALECT);

    assert!(sql.ends_with("FOR UPDATE NOWAIT"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_for_update_skip_locked() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.for_update_with("SKIP LOCKED");
    let (sql, _) = q.to_sql_with(&DIALECT);

    assert!(sql.ends_with("FOR UPDATE SKIP LOCKED"));

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_for_with_share() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("SHARE");
    let (sql, binds) = q.to_sql_with(&DIALECT);

    assert!(sql.ends_with("FOR SHARE"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

// --- COUNT ---

#[tokio::test]
async fn test_count_all_with_reserved_word_alias() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.add_select(count_all().as_("count"));
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("count"), 3);
}

// --- UPDATE ---

#[tokio::test]
async fn test_update_basic() {
    let pool = setup_pool().await;

    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name FROM users WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
}

#[tokio::test]
async fn test_update_multiple_sets() {
    let pool = setup_pool().await;

    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name, age FROM users WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
    assert_eq!(rows[0].get::<i64, _>("age"), 31);
}

#[tokio::test]
async fn test_update_from_query_with_where() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("id").eq(2));
    let mut u = q.into_update();
    u.set(col("name"), "Bobby");
    let (sql, binds) = u.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name FROM users WHERE id = 2")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Bobby");
}

#[tokio::test]
async fn test_update_allow_without_where() {
    let pool = setup_pool().await;

    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("age"), 99);
    u.allow_without_where();
    let (sql, binds) = u.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT age FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(rows.iter().all(|r| r.get::<i64, _>("age") == 99));
}

// --- DELETE ---

#[tokio::test]
async fn test_delete_basic() {
    let pool = setup_pool().await;

    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT id FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get::<i64, _>("id") != 1));
}

#[tokio::test]
async fn test_delete_from_query_with_where() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").lt(30));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Only Bob (age=25) should be deleted
    let rows = sqlx::query("SELECT name FROM users ORDER BY name ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_delete_allow_without_where() {
    let pool = setup_pool().await;

    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT id FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

// --- INSERT ---

#[tokio::test]
async fn test_insert_single_row() {
    let pool = setup_pool().await;

    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", MysqlValue::Int(4)),
        ("name", MysqlValue::Text("Dave".to_string())),
        ("age", MysqlValue::Int(40)),
    ]);
    let (sql, binds) = ins.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name, age FROM users WHERE id = 4")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[0].get::<i64, _>("age"), 40);
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let pool = setup_pool().await;

    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", MysqlValue::Int(4)),
        ("name", MysqlValue::Text("Dave".to_string())),
        ("age", MysqlValue::Int(40)),
    ]);
    ins.add_value(&[
        ("id", MysqlValue::Int(5)),
        ("name", MysqlValue::Text("Eve".to_string())),
        ("age", MysqlValue::Int(28)),
    ]);
    let (sql, binds) = ins.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name FROM users WHERE id >= 4 ORDER BY id ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[1].get::<String, _>("name"), "Eve");
}

#[tokio::test]
async fn test_insert_from_select() {
    let pool = setup_pool().await;

    sqlx::query(
        "CREATE TABLE users_archive (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut sub = qbey_with::<MysqlValue>("users");
    sub.select(&["id", "name", "age"]);
    sub.and_where(col("age").gt(30));

    let mut ins = qbey_with::<MysqlValue>("users_archive").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql_with(&DIALECT);

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name FROM users_archive ORDER BY name ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

// --- DISTINCT ---

#[tokio::test]
async fn test_distinct() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // orders has: shipped, pending, shipped → distinct gives: pending, shipped
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("status"), "pending");
    assert_eq!(rows[1].get::<String, _>("status"), "shipped");
}

// --- HAVING ---

#[tokio::test]
async fn test_having() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["user_id"]);
    q.having(col("cnt").gt(1));
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["user_id"]);
    q.add_select(count_all().as_("cnt"));
    q.and_where(col("status").eq("shipped"));
    q.group_by(&["user_id"]);
    q.and_having(col("cnt").gt(0));
    let (sql, binds) = q.to_sql_with(&DIALECT);

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
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<u64, _>("rn"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_sum_over_partition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<f64, _>("user_total"), 300.0);
    assert_eq!(rows[2].get::<f64, _>("user_total"), 50.0);
}

#[tokio::test]
async fn test_count_over_partition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64, _>("user_order_count"), 2);
    assert_eq!(rows[2].get::<i64, _>("user_order_count"), 1);
}

#[tokio::test]
async fn test_named_window() {
    let pool = setup_pool().await;

    let w = window().order_by(col("age").desc()).as_("w");

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(row_number().over(w.clone()).as_("rn"));
    q.add_select(col("age").sum_over(w).as_("running"));
    let (sql, _) = q.to_sql_with(&DIALECT);

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<u64, _>("rn"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_cte() {
    let pool = setup_pool().await;

    let mut cte_q = qbey_with::<MysqlValue>("users");
    cte_q.select(&["id", "name", "age"]);
    cte_q.and_where(col("age").gt(28));

    let mut q = qbey_with::<MysqlValue>("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql_with(&DIALECT);

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice"); // age 30
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie"); // age 35
}
