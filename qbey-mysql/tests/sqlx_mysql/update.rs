use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{SelectQueryBuilder, UpdateQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_update_basic() {
    let pool = setup_pool().await;

    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();

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
    let (sql, binds) = u.to_sql();

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
    let (sql, binds) = u.to_sql();

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
    let (sql, binds) = u.to_sql();

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
