#![cfg(feature = "test-tokio-postgres")]

use sqipe::{Dialect, col, sqipe_from_subquery_with, sqipe_with, table};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{NoTls, types::ToSql};

struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }
}

/// Custom value type for PostgreSQL — stores i32 directly instead of i64,
/// matching PostgreSQL's INT type without needing a cast.
#[derive(Debug, Clone)]
enum PgValue {
    Text(String),
    Int(i32),
    Float(f64),
    Bool(bool),
}

impl From<&str> for PgValue {
    fn from(s: &str) -> Self {
        PgValue::Text(s.to_string())
    }
}

impl From<i32> for PgValue {
    fn from(n: i32) -> Self {
        PgValue::Int(n)
    }
}

impl From<f64> for PgValue {
    fn from(n: f64) -> Self {
        PgValue::Float(n)
    }
}

impl From<bool> for PgValue {
    fn from(b: bool) -> Self {
        PgValue::Bool(b)
    }
}

fn to_pg_params(binds: &[PgValue]) -> Vec<Box<dyn ToSql + Sync>> {
    binds
        .iter()
        .map(|v| -> Box<dyn ToSql + Sync> {
            match v {
                PgValue::Text(s) => Box::new(s.clone()),
                PgValue::Int(n) => Box::new(*n),
                PgValue::Float(f) => Box::new(*f),
                PgValue::Bool(b) => Box::new(*b),
            }
        })
        .collect()
}

async fn setup_container() -> (
    testcontainers::ContainerAsync<Postgres>,
    tokio_postgres::Client,
) {
    let container = Postgres::default().start().await.unwrap();
    let host_port = container.get_host_port_ipv4(5432).await.unwrap();

    let conn_str = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        host_port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .batch_execute(
            "CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL
            );
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                total DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL
            );
            INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
            INSERT INTO orders (id, user_id, total, status) VALUES (1, 1, 100.0, 'shipped'), (2, 1, 200.0, 'pending'), (3, 2, 50.0, 'shipped');",
        )
        .await
        .unwrap();

    (container, client)
}

#[tokio::test]
async fn test_basic_select() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
}

#[tokio::test]
async fn test_where_condition() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[0].get::<_, i32>("age"), 30);
}

#[tokio::test]
async fn test_order_by_and_limit() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Charlie");
    assert_eq!(rows[1].get::<_, String>("name"), "Alice");
}

#[tokio::test]
async fn test_join() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select_cols(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select_cols(&cols);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select_cols(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).await.unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_between() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Bob");
}

#[tokio::test]
async fn test_aggregate_count() {
    let (_container, client) = setup_container().await;

    let mut q = sqipe_with::<PgValue>("orders");
    q.aggregate(&[sqipe::aggregate::count_all().as_("cnt")]);
    q.group_by(&["status"]);
    q.select(&["status"]);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_union() {
    let (_container, client) = setup_container().await;

    let mut q1 = sqipe_with::<PgValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = sqipe_with::<PgValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_in_subquery() {
    let (_container, client) = setup_container().await;

    let mut sub = sqipe_with::<PgValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = sqipe_with::<PgValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Bob");
}

#[tokio::test]
async fn test_in_subquery_with_outer_binds() {
    let (_container, client) = setup_container().await;

    let mut sub = sqipe_with::<PgValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = sqipe_with::<PgValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    // Alice (age=30 > 26, has shipped order) — Bob (age=25) filtered out by age > 26
    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
}

#[tokio::test]
async fn test_not_in_subquery() {
    let (_container, client) = setup_container().await;

    let mut sub = sqipe_with::<PgValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = sqipe_with::<PgValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Charlie");
}

#[tokio::test]
async fn test_from_subquery() {
    let (_container, client) = setup_container().await;

    let mut sub = sqipe_with::<PgValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = sqipe_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("user_id"), 1); // Alice, total=100
    assert_eq!(rows[1].get::<_, i32>("user_id"), 2); // Bob, total=50
}

#[tokio::test]
async fn test_from_subquery_with_outer_where() {
    let (_container, client) = setup_container().await;

    let mut sub = sqipe_with::<PgValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = sqipe_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).await.unwrap();
    // Only Alice's order (total=100) passes total > 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("user_id"), 1);
}
