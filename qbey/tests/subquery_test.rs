use qbey::*;

#[test]
fn test_from_subquery() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = qbey_from_subquery(sub, "t");
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
    );
    assert_eq!(binds, vec![Value::String("completed".to_string())]);
}

#[test]
fn test_from_subquery_with_outer_where() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = qbey_from_subquery(sub, "t");
    q.and_where(col("amount").gt(100));
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" WHERE "amount" > ?"#
    );
    assert_eq!(
        binds,
        vec![Value::String("completed".to_string()), Value::Int(100)]
    );
}

#[test]
fn test_from_subquery_numbered_placeholders() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = qbey_from_subquery(sub, "t");
    q.and_where(col("amount").gt(100));
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = $1) AS "t" WHERE "amount" > $2"#
    );
    assert_eq!(
        binds,
        vec![Value::String("completed".to_string()), Value::Int(100)]
    );
}

#[test]
fn test_from_subquery_with_join() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = qbey_from_subquery(sub, "t");
    q.join("users", table("t").col("user_id").eq_col("id"));
    q.select(&["user_id"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" INNER JOIN "users" ON "t"."user_id" = "users"."id""#
    );
}
