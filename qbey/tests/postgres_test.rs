#![cfg(feature = "test-postgres")]

use postgres::{Client, NoTls, types::ToSql};
use qbey::{
    ConditionExpr, DeleteQueryBuilder, InsertQueryBuilder, LikeExpression, SelectQueryBuilder,
    UpdateQueryBuilder, col, count_all, qbey_from_subquery_with, qbey_with, row_number, table,
    window,
};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

use qbey::PgDialect as PostgresDialect;

/// Custom value type for PostgreSQL — stores i32 directly.
#[derive(Debug, Clone)]
enum PgValue {
    Text(String),
    Int(i32),
    BigInt(i64),
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

impl From<i64> for PgValue {
    fn from(n: i64) -> Self {
        PgValue::BigInt(n)
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

impl From<String> for PgValue {
    fn from(s: String) -> Self {
        PgValue::Text(s)
    }
}

fn to_pg_params(binds: &[PgValue]) -> Vec<Box<dyn ToSql + Sync>> {
    binds
        .iter()
        .map(|v| -> Box<dyn ToSql + Sync> {
            match v {
                PgValue::Text(s) => Box::new(s.clone()),
                PgValue::Int(n) => Box::new(*n),
                PgValue::BigInt(n) => Box::new(*n),
                PgValue::Float(f) => Box::new(*f),
                PgValue::Bool(b) => Box::new(*b),
            }
        })
        .collect()
}

struct SharedContainer {
    _container: testcontainers::ContainerAsync<Postgres>,
    host_port: u16,
}

static SHARED_CONTAINER: tokio::sync::OnceCell<SharedContainer> =
    tokio::sync::OnceCell::const_new();
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn get_shared_container() -> &'static SharedContainer {
    SHARED_CONTAINER
        .get_or_init(|| async {
            let container = Postgres::default().start().await.unwrap();
            let host_port = container.get_host_port_ipv4(5432).await.unwrap();
            SharedContainer {
                _container: container,
                host_port,
            }
        })
        .await
}

/// Get a sync Client connected to a fresh per-test database.
async fn setup_client() -> Client {
    let shared = get_shared_container().await;
    let db_id = DB_COUNTER.fetch_add(1, Relaxed);
    let db_name = format!("test_{}", db_id);
    let host_port = shared.host_port;

    // postgres::Client::connect internally calls block_on, so run it outside tokio.
    tokio::task::spawn_blocking(move || {
        let admin_conn_str = format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            host_port
        );
        let mut admin_client = Client::connect(&admin_conn_str, NoTls).unwrap();
        admin_client
            .execute(&format!("CREATE DATABASE \"{}\"", db_name), &[])
            .unwrap();

        let conn_str = format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname={}",
            host_port, db_name
        );
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
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
            .unwrap();
        client
    })
    .await
    .unwrap()
}

macro_rules! pg_test {
    ($name:ident, |$client:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            let mut client = setup_client().await;
            tokio::task::spawn_blocking(move || {
                let $client = &mut client;
                $body
            })
            .await
            .unwrap();
        }
    };
}

pg_test!(test_basic_select, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_where_condition, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[0].get::<_, i32>("age"), 30);
});

pg_test!(test_order_by_and_limit, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Charlie");
    assert_eq!(rows[1].get::<_, String>("name"), "Alice");
});

pg_test!(test_join, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
});

pg_test!(test_join_with_alias, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select(&cols);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_left_join, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 4);
});

pg_test!(test_between, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Bob");
});

pg_test!(test_union, |client| {
    let mut q1 = qbey_with::<PgValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<PgValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
});

pg_test!(test_in_subquery, |client| {
    let mut sub = qbey_with::<PgValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Bob");
});

pg_test!(test_in_subquery_with_outer_binds, |client| {
    let mut sub = qbey_with::<PgValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    // Alice (age=30 > 26, has shipped order) — Bob (age=25) filtered out by age > 26
    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_from_subquery, |client| {
    let mut sub = qbey_with::<PgValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("user_id"), 1); // Alice, total=100
    assert_eq!(rows[1].get::<_, i32>("user_id"), 2); // Bob, total=50
});

pg_test!(test_from_subquery_with_outer_where, |client| {
    let mut sub = qbey_with::<PgValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    // Only Alice's order (total=100) passes total > 60
    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("user_id"), 1);
});

pg_test!(test_like_contains, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Charlie");
});

pg_test!(test_like_starts_with, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_like_ends_with, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ob")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Bob");
});

pg_test!(test_not_like, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Bob");
});

pg_test!(test_for_update, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update();
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    assert!(sql.ends_with("FOR UPDATE"));

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_for_update_with_option, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update_with("NOWAIT");
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    assert!(sql.ends_with("FOR UPDATE NOWAIT"));

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_for_with_share, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("SHARE");
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    assert!(sql.ends_with("FOR SHARE"));

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_for_with_no_key_update, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("NO KEY UPDATE");
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    assert!(sql.ends_with("FOR NO KEY UPDATE"));

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_for_update_skip_locked, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name"]);
    q.for_update_with("SKIP LOCKED");
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    assert!(sql.ends_with("FOR UPDATE SKIP LOCKED"));

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 3);
});

pg_test!(test_like_custom_escape_char, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Charlie");
});

pg_test!(test_update_basic, |client| {
    let mut u = qbey_with::<PgValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name FROM users WHERE id = 1", &[])
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "Alicia");
});

pg_test!(test_update_multiple_sets, |client| {
    let mut u = qbey_with::<PgValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name, age FROM users WHERE id = 1", &[])
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "Alicia");
    assert_eq!(rows[0].get::<_, i32>("age"), 31);
});

pg_test!(test_update_from_query_with_where, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("id").eq(2));
    let mut u = q.into_update();
    u.set(col("name"), "Bobby");
    let (sql, binds) = u.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name FROM users WHERE id = 2", &[])
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "Bobby");
});

pg_test!(test_update_allow_without_where, |client| {
    let mut u = qbey_with::<PgValue>("users").into_update();
    u.set(col("age"), 99);
    u.allow_without_where();
    let (sql, binds) = u.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client.query("SELECT age FROM users", &[]).unwrap();
    assert!(rows.iter().all(|r| r.get::<_, i32>("age") == 99));
});

pg_test!(test_delete_basic, |client| {
    let mut d = qbey_with::<PgValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client.query("SELECT id FROM users", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get::<_, i32>("id") != 1));
});

pg_test!(test_delete_from_query_with_where, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.and_where(col("age").lt(30));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name FROM users ORDER BY name ASC", &[])
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
    assert_eq!(rows[1].get::<_, String>("name"), "Charlie");
});

pg_test!(test_delete_allow_without_where, |client| {
    let mut d = qbey_with::<PgValue>("users").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client.query("SELECT id FROM users", &[]).unwrap();
    assert_eq!(rows.len(), 0);
});

pg_test!(test_count_all_with_reserved_word_alias, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.add_select(count_all().as_("count"));
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>("count"), 3);
});

pg_test!(test_insert_single_row, |client| {
    let mut ins = qbey_with::<PgValue>("users").into_insert();
    ins.add_value(&[
        ("id", PgValue::Int(4)),
        ("name", PgValue::Text("Dave".to_string())),
        ("age", PgValue::Int(40)),
    ]);
    let (sql, binds) = ins.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name, age FROM users WHERE id = 4", &[])
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "Dave");
    assert_eq!(rows[0].get::<_, i32>("age"), 40);
});

pg_test!(test_insert_multiple_rows, |client| {
    let mut ins = qbey_with::<PgValue>("users").into_insert();
    ins.add_value(&[
        ("id", PgValue::Int(4)),
        ("name", PgValue::Text("Dave".to_string())),
        ("age", PgValue::Int(40)),
    ]);
    ins.add_value(&[
        ("id", PgValue::Int(5)),
        ("name", PgValue::Text("Eve".to_string())),
        ("age", PgValue::Int(28)),
    ]);
    let (sql, binds) = ins.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name FROM users WHERE id >= 4 ORDER BY id ASC", &[])
        .unwrap();
    assert_eq!(rows[0].get::<_, String>("name"), "Dave");
    assert_eq!(rows[1].get::<_, String>("name"), "Eve");
});

pg_test!(test_insert_from_select, |client| {
    client
        .batch_execute(
            "CREATE TABLE users_archive (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL
            );",
        )
        .unwrap();

    let mut sub = qbey_with::<PgValue>("users");
    sub.select(&["id", "name", "age"]);
    sub.and_where(col("age").gt(30));

    let mut ins = qbey_with::<PgValue>("users_archive").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();
    client.execute(&sql, &param_refs).unwrap();

    let rows = client
        .query("SELECT name FROM users_archive ORDER BY name ASC", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("name"), "Charlie");
});

// --- DISTINCT ---

pg_test!(test_distinct, |client| {
    let mut q = qbey_with::<PgValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();

    // orders has: shipped, pending, shipped → distinct gives: pending, shipped
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("status"), "pending");
    assert_eq!(rows[1].get::<_, String>("status"), "shipped");
});

// --- HAVING ---

pg_test!(test_having, |client| {
    // Data: Alice(30), Bob(25), Charlie(35) — all ages are unique, each has count=1
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["age"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["age"]);
    q.having(count_all().gte(1_i64));

    let (sql, binds) = q.to_sql_with(&PostgresDialect);
    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    // All 3 age groups have count >= 1
    assert_eq!(rows.len(), 3);
});

pg_test!(test_having_with_where, |client| {
    // Data: Alice(30), Bob(25), Charlie(35)
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["age"]);
    q.add_select(count_all().as_("cnt"));
    q.and_where(col("age").gte(30_i32));
    q.group_by(&["age"]);
    q.and_having(count_all().gte(1_i64));

    let (sql, binds) = q.to_sql_with(&PostgresDialect);
    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    // age >= 30: Alice(30), Charlie(35) → 2 age groups, each with count=1
    assert_eq!(rows.len(), 2);
});

// ── Window functions ──

pg_test!(test_row_number_over, |client| {
    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    let first_name: String = rows[0].get("name");
    let first_rn: i64 = rows[0].get("rn");
    assert_eq!(first_name, "Charlie");
    assert_eq!(first_rn, 1);
});

pg_test!(test_sum_over_partition, |client| {
    let mut q = qbey_with::<PgValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(rows.len(), 3);
    let total0: f64 = rows[0].get("user_total");
    let total2: f64 = rows[2].get("user_total");
    assert_eq!(total0, 300.0);
    assert_eq!(total2, 50.0);
});

pg_test!(test_count_over_partition, |client| {
    let mut q = qbey_with::<PgValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(rows.len(), 3);
    let cnt0: i64 = rows[0].get("user_order_count");
    let cnt2: i64 = rows[2].get("user_order_count");
    assert_eq!(cnt0, 2);
    assert_eq!(cnt2, 1);
});

pg_test!(test_named_window, |client| {
    let w = window().order_by(col("age").desc()).as_("w");

    let mut q = qbey_with::<PgValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(row_number().over(w.clone()).as_("rn"));
    q.add_select(col("age").sum_over(w).as_("running"));
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    let first_name: String = rows[0].get("name");
    let first_rn: i64 = rows[0].get("rn");
    assert_eq!(first_name, "Charlie");
    assert_eq!(first_rn, 1);
});
