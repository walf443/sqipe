use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{ConditionExpr, InsertQueryBuilder, SelectQueryBuilder, UpdateQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_update_basic() {
    let pool = setup_pool().await;

    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alicia");
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
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
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
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
    let u = u.where_set();
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
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
    let u = u.allow_without_where();
    let (sql, binds) = u.to_sql();

    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT age FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(rows.iter().all(|r| r.get::<i64, _>("age") == 99));
}

#[tokio::test]
async fn test_update_blob() {
    let pool = setup_pool().await;

    sqlx::query(
        "CREATE TABLE files (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            data BLOB NOT NULL
        )",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Insert initial row with qbey
    let mut ins = qbey_with::<MysqlValue>("files").into_insert();
    ins.add_value(&[
        ("id", 1.into()),
        ("name", "test.bin".into()),
        ("data", MysqlValue::Blob(vec![0x00, 0x01])),
    ]);
    let (sql, binds) = ins.to_sql();
    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let new_data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

    let mut u = qbey_with::<MysqlValue>("files").into_update();
    u.set(col("data"), MysqlValue::Blob(new_data.clone()));
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    bind_params(sqlx::query(sqlx::AssertSqlSafe(sql.as_str())), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT data FROM files WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<Vec<u8>, _>("data"), new_data);
}
