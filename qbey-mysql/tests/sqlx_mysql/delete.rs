use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{DeleteQueryBuilder, SelectQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_delete_basic() {
    let pool = setup_pool().await;

    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Verify Alice was deleted
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
    let (sql, binds) = d.to_sql();

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
    let (sql, binds) = d.to_sql();

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

#[tokio::test]
async fn test_delete_with_order_by_and_limit() {
    let pool = setup_pool().await;

    // Delete the oldest user only
    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.allow_without_where();
    d.order_by(col("age").desc());
    d.limit(1);
    let (sql, binds) = d.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // Charlie (age=35) should be deleted, Alice and Bob remain
    let rows = sqlx::query("SELECT name FROM users ORDER BY name ASC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}
