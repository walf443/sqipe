use sqipe::*;

#[test]
fn test_basic_select_to_sql() {
    let mut q = sqipe("employee");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ?"
    );
    assert_eq!(binds, vec![Value::String("Alice".to_string())]);
}

#[test]
fn test_basic_select_to_pipe_sql() {
    let mut q = sqipe("employee");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _binds) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> WHERE \"name\" = ? |> SELECT \"id\", \"name\""
    );
}

#[test]
fn test_select_star_when_no_select() {
    let mut q = sqipe("employee");
    q.and_where(("name", "Alice"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ?");
}

#[test]
fn test_order_by() {
    let mut q = sqipe("employee");
    q.select(&["id", "name", "age"]);
    q.order_by(col("name").asc());
    q.order_by(col("age").desc());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC"
    );

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> SELECT \"id\", \"name\", \"age\" |> ORDER BY \"name\" ASC, \"age\" DESC"
    );
}

#[test]
fn test_limit_offset() {
    let mut q = sqipe("employee");
    q.select(&["id", "name"]);
    q.limit(10);
    q.offset(20);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20"
    );

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> SELECT \"id\", \"name\" |> LIMIT 10 OFFSET 20"
    );
}

#[test]
fn test_method_chaining() {
    let (sql, _) = sqipe("employee")
        .and_where(("name", "Alice"))
        .and_where(col("age").gt(20))
        .select(&["id", "name"])
        .to_sql();

    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ?"
    );
}

#[test]
fn test_col_alias() {
    let mut q = sqipe("employee");
    q.add_select(col("full_name").as_("name"));
    q.add_select(col("age"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"full_name\" AS \"name\", \"age\" FROM \"employee\""
    );
}

#[test]
fn test_qualified_col_alias() {
    let u = table("users");
    let mut q = sqipe("users");
    q.add_select(u.col("full_name").as_("name"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"users\".\"full_name\" AS \"name\" FROM \"users\""
    );
}

#[test]
fn test_table_alias() {
    let mut q = sqipe("employee");
    q.as_("e");
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" AS \"e\"");
}

#[test]
fn test_sqipe_with_table_ref() {
    let q = sqipe(table("users"));
    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT * FROM "users""#);
}

#[test]
fn test_sqipe_with_table_ref_alias() {
    let mut q = sqipe(table("employee").as_("e"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT "id", "name" FROM "employee" AS "e""#);

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(sql, r#"FROM "employee" AS "e" |> SELECT "id", "name""#);
}

#[test]
fn test_sqipe_with_table_ref_alias_where() {
    let mut q = sqipe(table("employee").as_("e"));
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "employee" AS "e" WHERE "name" = ?"#
    );
    assert_eq!(binds, vec![Value::String("Alice".to_string())]);
}

#[test]
fn test_for_update() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update();

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE"#
    );
}

#[test]
fn test_for_update_pipe() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update();

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE"#
    );
}

#[test]
fn test_for_update_with_option() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update_with("NOWAIT");

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE NOWAIT"#
    );
}

#[test]
fn test_for_update_with_option_pipe() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update_with("SKIP LOCKED");

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE SKIP LOCKED"#
    );
}

#[test]
fn test_for_with() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("NO KEY UPDATE");

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR NO KEY UPDATE"#
    );
}

#[test]
fn test_for_with_pipe() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("SHARE");

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR SHARE"#
    );
}

#[test]
fn test_for_update_with_order_by_and_limit() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by(col("id").asc());
    q.limit(10);
    q.for_update();

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" ORDER BY "id" ASC LIMIT 10 FOR UPDATE"#
    );
}

#[test]
fn test_qualified_col_order_by() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.order_by(table("users").col("name").asc());
    q.order_by(table("orders").col("created_at").desc());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" ORDER BY "users"."name" ASC, "orders"."created_at" DESC"#
    );

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> INNER JOIN "orders" ON "users"."id" = "orders"."user_id" |> SELECT "id", "name" |> ORDER BY "users"."name" ASC, "orders"."created_at" DESC"#
    );
}

#[test]
fn test_add_select_expr() {
    let mut q = sqipe("users");
    q.select(&["id"]);
    q.add_select_expr("UPPER(\"name\")", Some("upper_name"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", UPPER("name") AS "upper_name" FROM "users""#
    );
}

#[test]
fn test_add_select_expr_without_alias() {
    let mut q = sqipe("users");
    q.add_select_expr("COALESCE(\"nickname\", \"name\")", None);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT COALESCE("nickname", "name") FROM "users""#);
}

#[test]
fn test_select_appends() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.add_select_expr("UPPER(\"email\")", Some("upper_email"));
    q.select(&["age"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name", UPPER("email") AS "upper_email", "age" FROM "users""#
    );
}

#[test]
fn test_add_select_expr_preserves_order() {
    let mut q = sqipe("users");
    q.add_select(col("id"));
    q.add_select_expr("LENGTH(\"name\")", Some("name_len"));
    q.add_select(col("email"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", LENGTH("name") AS "name_len", "email" FROM "users""#
    );
}

#[test]
fn test_order_by_expr() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by_expr(RawSql::new("RAND()"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT "id", "name" FROM "users" ORDER BY RAND()"#);

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> SELECT "id", "name" |> ORDER BY RAND()"#
    );
}

#[test]
fn test_order_by_expr_with_direction() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by_expr(RawSql::new("id DESC NULLS FIRST"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" ORDER BY id DESC NULLS FIRST"#
    );
}

#[test]
fn test_order_by_mixed_col_and_expr() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    q.order_by_expr(RawSql::new("RAND()"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" ORDER BY "name" ASC, RAND()"#
    );
}

#[test]
fn test_for_update_with_order_by_and_limit_pipe() {
    let mut q = sqipe("users");
    q.select(&["id", "name"]);
    q.order_by(col("id").asc());
    q.limit(10);
    q.for_update();

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        r#"FROM "users" |> SELECT "id", "name" |> ORDER BY "id" ASC |> LIMIT 10 FOR UPDATE"#
    );
}
