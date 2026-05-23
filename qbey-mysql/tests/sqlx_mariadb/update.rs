// MariaDB does NOT support UPDATE ... RETURNING.
// RETURNING is only available for INSERT and DELETE in MariaDB.

use qbey::{ConditionExpr, InsertQueryBuilder, UpdateQueryBuilder, col};
use qbey_mysql::qbey_with;
use sqlx::{Executor, Row};

use super::common::{MysqlValue, bind_params, setup_pool};

#[test]
#[should_panic(expected = "RETURNING is not supported for UPDATE in MySQL/MariaDB")]
fn test_update_returning_panics() {
    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alice");
    let mut u = u.and_where(col("id").eq(1));
    u.returning(&[col("id"), col("name")]);
}

#[tokio::test]
async fn test_update_blob() {
    let pool = setup_pool().await;

    pool.execute(
        "CREATE TABLE files (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            data BLOB NOT NULL
        )",
    )
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
