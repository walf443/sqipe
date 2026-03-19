use qbey::*;

#[test]
fn test_join_standard() {
    let mut q = qbey("users");
    q.join("orders", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_left_join() {
    let mut q = qbey("users");
    q.left_join("orders", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_join_with_table_alias() {
    let mut q = qbey("users");
    q.join(
        table("orders").as_("o"),
        table("users").col("id").eq(col("user_id")),
    );
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" AS \"o\" ON \"users\".\"id\" = \"o\".\"user_id\""
    );
}

#[test]
fn test_table_qualified_cols_select() {
    let u = table("users");
    let mut q = qbey("users");
    q.join("orders", u.col("id").eq(col("user_id")));
    q.add_select(u.col("id"));
    q.add_select(u.col("name"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_select_cols_from_table() {
    let u = table("users");
    let mut q = qbey("users");
    q.join("orders", u.col("id").eq(col("user_id")));
    q.select(&u.cols(&["id", "name"]));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_join_with_using() {
    let mut q = qbey("users");
    q.join("orders", join::using_col("user_id"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\")"
    );
}

#[test]
fn test_join_with_using_multiple_columns() {
    let mut q = qbey("users");
    q.join("orders", join::using_cols(&["user_id", "region"]));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\", \"region\")"
    );
}

#[test]
fn test_join_with_multiple_conditions() {
    let mut q = qbey("users");
    q.join(
        "orders",
        JoinCondition::And(vec![
            table("users").col("id").eq(col("user_id")).into(),
            table("users").col("region").eq(col("region")).into(),
        ]),
    );
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" AND \"users\".\"region\" = \"orders\".\"region\""
    );
}

#[test]
fn test_join_with_qualified_col_on_right() {
    let mut q = qbey("users");
    q.join(
        "orders",
        table("users").col("id").eq(table("orders").col("user_id")),
    );
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_join_subquery_standard() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.join_subquery(sub, "o", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
    );
    assert_eq!(binds, vec![Value::String("shipped".to_string())]);
}

#[test]
fn test_left_join_subquery_standard() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "total"]);

    let mut q = qbey("users");
    q.left_join_subquery(sub, "o", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" LEFT JOIN (SELECT "user_id", "total" FROM "orders") AS "o" ON "users"."id" = "o"."user_id""#
    );
}

#[test]
fn test_join_subquery_with_outer_where() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.join_subquery(sub, "o", table("users").col("id").eq(col("user_id")));
    q.and_where(col("age").gt(25));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id" WHERE "age" > ?"#
    );
    assert_eq!(
        binds,
        vec![Value::String("shipped".to_string()), Value::Int(25)]
    );
}

#[test]
fn test_join_subquery_numbered_placeholders() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.and_where(col("age").gt(25));
    q.join_subquery(sub, "o", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = $1) AS "o" ON "users"."id" = "o"."user_id" WHERE "age" > $2"#
    );
    assert_eq!(
        binds,
        vec![Value::String("shipped".to_string()), Value::Int(25)]
    );
}

#[test]
fn test_join_subquery_mixed_with_table_join() {
    let mut sub = qbey("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.join("profiles", table("users").col("id").eq(col("user_id")));
    q.join_subquery(sub, "o", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN "profiles" ON "users"."id" = "profiles"."user_id" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
    );
    assert_eq!(binds, vec![Value::String("shipped".to_string())]);
}

#[test]
fn test_join_with_unqualified_col_eq_col() {
    let mut q = qbey("users");
    q.join("orders", col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN "orders" ON "id" = "orders"."user_id""#
    );
}

#[test]
fn test_join_condition_expr_standard() {
    let mut q = qbey("texts");
    q.join(
        "patterns",
        join::on_expr(RawSql::new(r#""texts"."text" LIKE "patterns"."pattern""#)),
    );
    q.select(&["id", "text"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."text" LIKE "patterns"."pattern""#
    );
}

#[test]
fn test_qbey_table_ref_with_join() {
    let mut q = qbey(table("users").as_("u"));
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq(col("user_id")),
    );
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" AS "u" INNER JOIN "orders" AS "o" ON "u"."id" = "o"."user_id""#
    );
}

#[test]
fn test_join_condition_expr_inside_and() {
    let mut q = qbey("texts");
    q.join(
        "patterns",
        JoinCondition::And(vec![
            table("texts").col("category").eq(col("category")).into(),
            join::on_expr(RawSql::new(r#""texts"."text" LIKE "patterns"."pattern""#)),
        ]),
    );
    q.select(&["id", "text"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."category" = "patterns"."category" AND "texts"."text" LIKE "patterns"."pattern""#
    );
}

#[test]
fn test_join_with_eq_col_ref() {
    let mut q = qbey("users");
    q.join("orders", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
    );
}

#[test]
fn test_left_join_with_eq_col_ref() {
    let mut q = qbey("users");
    q.left_join("addresses", table("users").col("id").eq(col("user_id")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\""
    );
}
