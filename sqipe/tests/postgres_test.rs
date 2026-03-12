#![cfg(feature = "test-postgres")]

use postgres::{Client, NoTls, types::ToSql};
use sqipe::{col, sqipe, table, Dialect};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }
}

fn to_pg_params(binds: &[sqipe::Value]) -> Vec<Box<dyn ToSql + Sync>> {
    binds
        .iter()
        .map(|v| -> Box<dyn ToSql + Sync> {
            match v {
                sqipe::Value::String(s) => Box::new(s.clone()),
                sqipe::Value::Int(n) => Box::new(*n as i32),
                sqipe::Value::Float(f) => Box::new(*f),
                sqipe::Value::Bool(b) => Box::new(*b),
            }
        })
        .collect()
}

/// Start a Postgres container (async), then connect with sync Client on a
/// separate thread (postgres crate uses internal block_on, which conflicts
/// with tokio runtime).
async fn setup_container() -> (testcontainers::ContainerAsync<Postgres>, Client) {
    let container = Postgres::default().start().await.unwrap();
    let host_port = container.get_host_port_ipv4(5432).await.unwrap();

    let conn_str = format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        host_port
    );

    // postgres::Client::connect internally calls block_on, so run it outside tokio.
    let client = tokio::task::spawn_blocking(move || {
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
    .unwrap();

    (container, client)
}

macro_rules! pg_test {
    ($name:ident, |$client:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            let (_container, mut client) = setup_container().await;
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
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_where_condition, |client| {
    let mut q = sqipe("users");
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
    let mut q = sqipe("users");
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
    let mut q = sqipe("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select_cols(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
});

pg_test!(test_join_with_alias, |client| {
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
    let (sql, binds) = q.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("name"), "Alice");
});

pg_test!(test_left_join, |client| {
    let mut q = sqipe("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select_cols(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 4);
});

pg_test!(test_between, |client| {
    let mut q = sqipe("users");
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

pg_test!(test_aggregate_count, |client| {
    let mut q = sqipe("orders");
    q.aggregate(&[sqipe::aggregate::count_all().as_("cnt")]);
    q.group_by(&["status"]);
    q.select(&["status"]);
    let (sql, _) = q.to_sql_with(&PostgresDialect);

    let rows = client.query(&sql, &[]).unwrap();
    assert_eq!(rows.len(), 2);
});

pg_test!(test_union, |client| {
    let mut q1 = sqipe("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = sqipe("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql_with(&PostgresDialect);

    let params = to_pg_params(&binds);
    let param_refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p.as_ref()).collect();

    let rows = client.query(&sql, &param_refs).unwrap();
    assert_eq!(rows.len(), 2);
});
