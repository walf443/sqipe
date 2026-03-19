// TODO: sqlx's MySQL driver does not expose column names correctly for
// RETURNING result sets, so tests use column index access (e.g., `get::<_, _>(0usize)`).
// Revisit when sqlx adds proper RETURNING support for MariaDB.

use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{InsertQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_insert_returning() {
    let pool = setup_pool().await;

    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 4.into()),
        ("name", "Dave".into()),
        ("age", 40.into()),
    ]);
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    // Use column index — sqlx's MySQL driver may not expose column names
    // correctly for RETURNING result sets.
    assert_eq!(rows[0].get::<i64, _>(0usize), 4);
    assert_eq!(rows[0].get::<String, _>(1usize), "Dave");
}

#[tokio::test]
async fn test_insert_multiple_rows_returning() {
    let pool = setup_pool().await;

    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 4.into()),
        ("name", "Dave".into()),
        ("age", 40.into()),
    ]);
    ins.add_value(&[("id", 5.into()), ("name", "Eve".into()), ("age", 28.into())]);
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>(1usize), "Dave");
    assert_eq!(rows[1].get::<String, _>(1usize), "Eve");
}

#[tokio::test]
async fn test_insert_on_duplicate_key_update_with_returning() {
    let pool = setup_pool().await;

    // id=1 (Alice) already exists — ODKU updates name, RETURNING returns the updated row
    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 1.into()),
        ("name", "Alice".into()),
        ("age", 30.into()),
    ]);
    ins.on_duplicate_key_update(col("name"), "Alicia");
    ins.returning(&[col("id"), col("name")]);
    let (sql, binds) = ins.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>(0usize), 1);
    assert_eq!(rows[0].get::<String, _>(1usize), "Alicia");
}
