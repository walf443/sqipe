// TODO: sqlx's MySQL driver does not expose column names correctly for
// RETURNING result sets, so tests use column index access (e.g., `get::<_, _>(0usize)`).
// Revisit when sqlx adds proper RETURNING support for MariaDB.

use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{ConditionExpr, DeleteQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_delete_returning() {
    let pool = setup_pool().await;

    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.and_where(col("id").eq(1));
    d.returning(&[col("id"), col("name")]);
    let (sql, binds) = d.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // Use column index — sqlx's MySQL driver may not expose column names
    // correctly for RETURNING result sets.
    assert_eq!(rows[0].get::<i64, _>(0usize), 1);
    assert_eq!(rows[0].get::<String, _>(1usize), "Alice");

    // Verify Alice was actually deleted
    let remaining = sqlx::query("SELECT id FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 2);
}

#[tokio::test]
async fn test_delete_returning_multiple_rows() {
    let pool = setup_pool().await;

    let mut d = qbey_with::<MysqlValue>("users").into_delete();
    d.and_where(col("age").gte(30));
    d.returning(&[col("name"), col("age")]);
    let (sql, binds) = d.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice(30) and Charlie(35)
    assert_eq!(rows.len(), 2);

    let remaining = sqlx::query("SELECT id FROM users")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1); // Only Bob remains
}
