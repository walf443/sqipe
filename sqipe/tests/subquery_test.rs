use sqipe::*;

#[test]
fn test_from_subquery() {
    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
    );
    assert_eq!(binds, vec![Value::String("completed".to_string())]);
}

#[test]
fn test_from_subquery_pipe() {
    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
    q.select(&["user_id"]);

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" |> SELECT "user_id""#
    );
}

#[test]
fn test_from_subquery_with_outer_where() {
    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
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
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
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
    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
    q.join("users", table("t").col("user_id").eq_col("id"));
    q.select(&["user_id"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" INNER JOIN "users" ON "t"."user_id" = "users"."id""#
    );
}

#[test]
fn test_from_subquery_cte_where_before_join() {
    let mut sub = sqipe("orders");
    sub.select(&["user_id", "amount"]);
    sub.and_where(col("status").eq("completed"));

    let mut q = sqipe_from_subquery(sub, "t");
    q.and_where(col("amount").gt(100));
    q.join("users", table("t").col("user_id").eq_col("id"));
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"WITH "_cte_0" AS (SELECT * FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" WHERE "amount" > ?) SELECT "user_id" FROM "_cte_0" AS "t" INNER JOIN "users" ON "t"."user_id" = "users"."id""#
    );
    assert_eq!(
        binds,
        vec![Value::String("completed".to_string()), Value::Int(100)]
    );
}
