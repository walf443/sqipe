use sqlx::MySqlPool;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

/// Custom value type for MySQL — maps directly to sqlx bind types.
#[derive(Debug, Clone)]
pub enum MysqlValue {
    Text(String),
    Int(i64),
    Float(f64),
    Bool(bool),
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

impl From<bool> for MysqlValue {
    fn from(b: bool) -> Self {
        MysqlValue::Bool(b)
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

pub async fn setup_pool() -> MySqlPool {
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

pub fn bind_params<'a>(
    mut query: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    binds: &'a [MysqlValue],
) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    for bind in binds {
        query = match bind {
            MysqlValue::Text(s) => query.bind(s.as_str()),
            MysqlValue::Int(n) => query.bind(*n),
            MysqlValue::Float(f) => query.bind(*f),
            MysqlValue::Bool(b) => query.bind(*b),
        };
    }
    query
}
