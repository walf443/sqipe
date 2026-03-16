use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{InsertQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_insert_on_duplicate_key_update_with_value() {
    let pool = setup_pool().await;

    // Insert a conflicting row (id=1 already exists as Alice, age=30)
    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 1.into()),
        ("name", "Alice".into()),
        ("age", 30.into()),
    ]);
    ins.on_duplicate_key_update(col("name"), "Alicia");
    let (sql, binds) = ins.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    // name should be updated to "Alicia", age unchanged
    let rows = sqlx::query("SELECT name, age FROM users WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alicia");
    assert_eq!(rows[0].get::<i64, _>("age"), 30);
}

#[tokio::test]
async fn test_insert_on_duplicate_key_update_expr() {
    let pool = setup_pool().await;

    // Insert a conflicting row using raw expressions
    // Alice (id=1, age=30) already exists — age should become 30 + 1 = 31
    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 1.into()),
        ("name", "Alice".into()),
        ("age", 30.into()),
    ]);
    ins.on_duplicate_key_update_expr(qbey::RawSql::new("`name` = CONCAT(`name`, '!')"));
    ins.on_duplicate_key_update_expr(qbey::RawSql::new("`age` = `age` + 1"));
    let (sql, binds) = ins.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name, age FROM users WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows[0].get::<String, _>("name"), "Alice!");
    assert_eq!(rows[0].get::<i64, _>("age"), 31);
}

#[tokio::test]
async fn test_insert_on_duplicate_key_update_no_conflict() {
    let pool = setup_pool().await;

    // Insert a new row (id=100 does not exist) — no conflict, normal insert
    let mut ins = qbey_with::<MysqlValue>("users").into_insert();
    ins.add_value(&[
        ("id", 100.into()),
        ("name", "Dave".into()),
        ("age", 40.into()),
    ]);
    ins.on_duplicate_key_update(col("name"), "should_not_apply");
    let (sql, binds) = ins.to_sql();

    bind_params(sqlx::query(&sql), &binds)
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT name, age FROM users WHERE id = 100")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Dave");
    assert_eq!(rows[0].get::<i64, _>("age"), 40);
}
