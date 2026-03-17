use crate::qbey;
use qbey::{ConditionExpr, DeleteQueryBuilder, SelectQueryBuilder, col};

#[test]
fn test_delete_basic() {
    let mut d = qbey("users").into_delete();
    d.and_where(col("id").eq(1));

    let (sql, binds) = d.to_sql();
    assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
    assert_eq!(binds, vec![qbey::Value::Int(1)]);
}

#[test]
fn test_delete_from_query_with_where() {
    let mut q = qbey("users");
    q.and_where(col("id").eq(1));
    let d = q.into_delete();

    let (sql, _) = d.to_sql();
    assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
}

#[test]
fn test_delete_allow_without_where() {
    let mut d = qbey("users").into_delete();
    d.allow_without_where();

    let (sql, binds) = d.to_sql();
    assert_eq!(sql, "DELETE FROM `users`");
    assert_eq!(binds, vec![]);
}

#[test]
fn test_delete_with_table_alias() {
    let mut q = qbey("users");
    q.as_("u");
    let mut d = q.into_delete();
    d.and_where(col("id").eq(1));

    let (sql, _) = d.to_sql();
    assert_eq!(sql, "DELETE FROM `users` `u` WHERE `id` = ?");
}

#[test]
fn test_delete_with_order_by_and_limit() {
    let mut d = qbey("users").into_delete();
    d.and_where(col("dept").eq("eng"));
    d.order_by(col("created_at").asc());
    d.limit(10);

    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        "DELETE FROM `users` WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10"
    );
    assert_eq!(binds, vec![qbey::Value::String("eng".to_string())]);
}

#[test]
fn test_delete_with_limit_only() {
    let mut d = qbey("users").into_delete();
    d.allow_without_where();
    d.limit(100);

    let (sql, _) = d.to_sql();
    assert_eq!(sql, "DELETE FROM `users` LIMIT 100");
}

#[test]
fn test_delete_with_like() {
    let mut d = qbey("users").into_delete();
    d.and_where(col("name").like(qbey::LikeExpression::starts_with("test")));

    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r"DELETE FROM `users` WHERE `name` LIKE ? ESCAPE '\\'");
    assert_eq!(binds, vec![qbey::Value::String("test%".to_string())]);
}

#[test]
fn test_delete_with_or_where() {
    let mut d = qbey("users").into_delete();
    d.and_where(col("status").eq("pending"));
    d.or_where(col("status").eq("draft"));

    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        "DELETE FROM `users` WHERE `status` = ? OR `status` = ?"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("pending".to_string()),
            qbey::Value::String("draft".to_string()),
        ]
    );
}

#[test]
fn test_delete_order_by_expr() {
    let mut d = qbey("users").into_delete();
    d.and_where(col("dept").eq("eng"));
    d.order_by_expr(qbey::RawSql::new("RAND()"));
    d.limit(10);

    let (sql, _) = d.to_sql();
    assert_eq!(
        sql,
        "DELETE FROM `users` WHERE `dept` = ? ORDER BY RAND() LIMIT 10"
    );
}
