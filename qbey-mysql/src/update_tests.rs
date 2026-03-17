use crate::qbey;
use qbey::{ConditionExpr, SelectQueryBuilder, UpdateQueryBuilder, col};

#[test]
fn test_update_basic() {
    let mut u = qbey("users").into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));

    let (sql, binds) = u.to_sql();
    assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("Alicia".to_string()),
            qbey::Value::Int(1)
        ]
    );
}

#[test]
fn test_update_multiple_sets() {
    let mut u = qbey("users").into_update();
    u.set(col("name"), "Alicia");
    u.set(col("age"), 31);
    u.and_where(col("id").eq(1));

    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        "UPDATE `users` SET `name` = ?, `age` = ? WHERE `id` = ?"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("Alicia".to_string()),
            qbey::Value::Int(31),
            qbey::Value::Int(1)
        ]
    );
}

#[test]
fn test_update_from_query_with_where() {
    let mut q = qbey("users");
    q.and_where(col("id").eq(1));
    let mut u = q.into_update();
    u.set(col("name"), "Alicia");

    let (sql, _) = u.to_sql();
    assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
}

#[test]
fn test_update_allow_without_where() {
    let mut u = qbey("users").into_update();
    u.set(col("age"), 99);
    u.allow_without_where();

    let (sql, _) = u.to_sql();
    assert_eq!(sql, "UPDATE `users` SET `age` = ?");
}

#[test]
fn test_update_with_table_alias() {
    let mut q = qbey("users");
    q.as_("u");
    let mut u = q.into_update();
    u.set(col("name"), "Alicia");
    u.and_where(col("id").eq(1));

    let (sql, _) = u.to_sql();
    // MySQL does not support AS in UPDATE table alias
    assert_eq!(sql, "UPDATE `users` `u` SET `name` = ? WHERE `id` = ?");
}

#[test]
fn test_update_with_order_by_and_limit() {
    let mut u = qbey("users").into_update();
    u.set(col("status"), "inactive");
    u.and_where(col("dept").eq("eng"));
    u.order_by(col("created_at").asc());
    u.limit(10);

    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        "UPDATE `users` SET `status` = ? WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("inactive".to_string()),
            qbey::Value::String("eng".to_string()),
        ]
    );
}

#[test]
fn test_update_with_limit_only() {
    let mut u = qbey("users").into_update();
    u.set(col("flagged"), true);
    u.allow_without_where();
    u.limit(100);

    let (sql, _) = u.to_sql();
    assert_eq!(sql, "UPDATE `users` SET `flagged` = ? LIMIT 100");
}

#[test]
fn test_update_with_like() {
    let mut u = qbey("users").into_update();
    u.set(col("flagged"), true);
    u.and_where(col("name").like(qbey::LikeExpression::starts_with("test")));

    let (sql, binds) = u.to_sql();
    // MySQL doubles backslash in ESCAPE clause due to backslash_escape
    assert_eq!(
        sql,
        r"UPDATE `users` SET `flagged` = ? WHERE `name` LIKE ? ESCAPE '\\'"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::Bool(true),
            qbey::Value::String("test%".to_string()),
        ]
    );
}

#[test]
fn test_update_with_set_expr() {
    let mut u = qbey("users").into_update();
    u.set_expr(qbey::RawSql::new("`visit_count` = `visit_count` + 1"));
    u.and_where(col("id").eq(1));

    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        "UPDATE `users` SET `visit_count` = `visit_count` + 1 WHERE `id` = ?"
    );
    assert_eq!(binds, vec![qbey::Value::Int(1)]);
}

#[test]
fn test_update_order_by_expr() {
    let mut u = qbey("users").into_update();
    u.set(col("status"), "inactive");
    u.and_where(col("dept").eq("eng"));
    u.order_by_expr(qbey::RawSql::new("RAND()"));
    u.limit(10);

    let (sql, _) = u.to_sql();
    assert_eq!(
        sql,
        "UPDATE `users` SET `status` = ? WHERE `dept` = ? ORDER BY RAND() LIMIT 10"
    );
}
