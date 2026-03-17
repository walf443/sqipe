use qbey::*;

#[test]
fn test_basic_select_to_sql() {
    let mut q = qbey("employee");
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
fn test_select_star_when_no_select() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ?");
}

#[test]
fn test_order_by() {
    let mut q = qbey("employee");
    q.select(&["id", "name", "age"]);
    q.order_by(col("name").asc());
    q.order_by(col("age").desc());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC"
    );
}

#[test]
fn test_limit_offset() {
    let mut q = qbey("employee");
    q.select(&["id", "name"]);
    q.limit(10);
    q.offset(20);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20"
    );
}

#[test]
fn test_method_chaining() {
    let (sql, _) = qbey("employee")
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
    let mut q = qbey("employee");
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
    let mut q = qbey("users");
    q.add_select(u.col("full_name").as_("name"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"users\".\"full_name\" AS \"name\" FROM \"users\""
    );
}

#[test]
fn test_table_alias() {
    let mut q = qbey("employee");
    q.as_("e");
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" AS \"e\"");
}

#[test]
fn test_qbey_with_table_ref() {
    let q = qbey(table("users"));
    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT * FROM "users""#);
}

#[test]
fn test_qbey_with_table_ref_alias() {
    let mut q = qbey(table("employee").as_("e"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT "id", "name" FROM "employee" AS "e""#);
}

#[test]
fn test_qbey_with_table_ref_alias_where() {
    let mut q = qbey(table("employee").as_("e"));
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
    let mut q = qbey("users");
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
fn test_for_update_with_option() {
    let mut q = qbey("users");
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
fn test_for_with() {
    let mut q = qbey("users");
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
fn test_for_update_with_order_by_and_limit() {
    let mut q = qbey("users");
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
    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.order_by(table("users").col("name").asc());
    q.order_by(table("orders").col("created_at").desc());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" ORDER BY "users"."name" ASC, "orders"."created_at" DESC"#
    );
}

#[test]
fn test_add_select_expr() {
    let mut q = qbey("users");
    q.select(&["id"]);
    q.add_select_expr(RawSql::new("UPPER(\"name\")"), Some("upper_name"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", UPPER("name") AS "upper_name" FROM "users""#
    );
}

#[test]
fn test_add_select_expr_without_alias() {
    let mut q = qbey("users");
    q.add_select_expr(RawSql::new("COALESCE(\"nickname\", \"name\")"), None);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT COALESCE("nickname", "name") FROM "users""#);
}

#[test]
fn test_select_appends() {
    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.add_select_expr(RawSql::new("UPPER(\"email\")"), Some("upper_email"));
    q.select(&["age"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name", UPPER("email") AS "upper_email", "age" FROM "users""#
    );
}

#[test]
fn test_add_select_expr_preserves_order() {
    let mut q = qbey("users");
    q.add_select(col("id"));
    q.add_select_expr(RawSql::new("LENGTH(\"name\")"), Some("name_len"));
    q.add_select(col("email"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", LENGTH("name") AS "name_len", "email" FROM "users""#
    );
}

#[test]
fn test_order_by_expr() {
    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.order_by_expr(RawSql::new("RAND()"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT "id", "name" FROM "users" ORDER BY RAND()"#);
}

#[test]
fn test_order_by_expr_with_direction() {
    let mut q = qbey("users");
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
    let mut q = qbey("users");
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
fn test_col_count() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(col("id").count());
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "dept", COUNT("id") FROM "employee" GROUP BY "dept""#
    );
}

#[test]
fn test_col_count_with_alias() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "dept", COUNT("id") AS "cnt" FROM "employee" GROUP BY "dept""#
    );
}

#[test]
fn test_col_count_with_table_qualified() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(table("employee").col("id").count().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "dept", COUNT("employee"."id") AS "cnt" FROM "employee" GROUP BY "dept""#
    );
}

#[test]
fn test_count_all() {
    let mut q = qbey("employee");
    q.add_select(count_all());

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT COUNT(*) FROM "employee""#);
}

#[test]
fn test_count_all_with_alias() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "dept", COUNT(*) AS "cnt" FROM "employee" GROUP BY "dept""#
    );
}

#[test]
fn test_count_one() {
    let mut q = qbey("employee");
    q.add_select(count_one().as_("cnt"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT COUNT(1) AS "cnt" FROM "employee""#);
}

#[test]
fn test_col_sum() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").sum());
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", SUM("price") FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_sum_with_alias() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").sum().as_("total"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", SUM("price") AS "total" FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_sum_with_table_qualified() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(table("orders").col("price").sum().as_("total"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", SUM("orders"."price") AS "total" FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_avg() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").avg());
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", AVG("price") FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_avg_with_alias() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").avg().as_("avg_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", AVG("price") AS "avg_price" FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_min() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").min());
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", MIN("price") FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_min_with_alias() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").min().as_("min_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", MIN("price") AS "min_price" FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_max() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").max());
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", MAX("price") FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_col_max_with_alias() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").max().as_("max_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", MAX("price") AS "max_price" FROM "orders" GROUP BY "product""#
    );
}

#[test]
fn test_having() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.having(col("cnt").gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt" FROM "orders" GROUP BY "product" HAVING "cnt" > ?"#
    );
    assert_eq!(binds, vec![Value::Int(5)]);
}

#[test]
fn test_having_multiple_conditions() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.add_select(col("price").sum().as_("total"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(5));
    q.and_having(col("total").gt(100));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt", SUM("price") AS "total" FROM "orders" GROUP BY "product" HAVING "cnt" > ? AND "total" > ?"#
    );
    assert_eq!(binds, vec![Value::Int(5), Value::Int(100)]);
}

#[test]
fn test_having_or() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(10));
    q.or_having(col("cnt").lt(2));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt" FROM "orders" GROUP BY "product" HAVING "cnt" > ? OR "cnt" < ?"#
    );
    assert_eq!(binds, vec![Value::Int(10), Value::Int(2)]);
}

#[test]
fn test_having_with_where() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.and_where(col("status").eq("completed"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt" FROM "orders" WHERE "status" = ? GROUP BY "product" HAVING "cnt" > ?"#
    );
    assert_eq!(
        binds,
        vec![Value::String("completed".to_string()), Value::Int(5),]
    );
}

#[test]
fn test_having_in_subquery() {
    let mut sub = qbey("popular_products");
    sub.select(&["product"]);

    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("product").included(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt" FROM "orders" GROUP BY "product" HAVING "product" IN (SELECT "product" FROM "popular_products")"#
    );
    assert!(binds.is_empty());
}

#[test]
fn test_having_not_in_subquery() {
    let mut sub = qbey("blocked_products");
    sub.select(&["product"]);

    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("product").not_included(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt" FROM "orders" GROUP BY "product" HAVING "product" NOT IN (SELECT "product" FROM "blocked_products")"#
    );
    assert!(binds.is_empty());
}

#[test]
fn test_having_count_all() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) > ?"#
    );
    assert_eq!(binds, vec![Value::Int(5)]);
}

#[test]
fn test_having_count_all_shared_select_item() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    let cnt = count_all().as_("cnt");
    q.add_select(cnt.clone());
    q.group_by(&["product"]);
    q.having(cnt.gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) > ?"#
    );
    assert_eq!(binds, vec![Value::Int(5)]);
}

#[test]
fn test_having_count_all_pg() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().gt(5));

    let (sql, binds) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) > $1"#
    );
    assert_eq!(binds, vec![Value::Int(5)]);
}

#[test]
fn test_having_sum_aggregate() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").sum().as_("total"));
    q.group_by(&["product"]);
    q.having(col("price").sum().gt(100));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", SUM("price") AS "total" FROM "orders" GROUP BY "product" HAVING SUM("price") > ?"#
    );
    assert_eq!(binds, vec![Value::Int(100)]);
}

#[test]
fn test_having_count_one() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_one().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_one().gte(3));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(1) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(1) >= ?"#
    );
    assert_eq!(binds, vec![Value::Int(3)]);
}

#[test]
fn test_having_aggregate_between() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().between(2, 10));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) BETWEEN ? AND ?"#
    );
    assert_eq!(binds, vec![Value::Int(2), Value::Int(10)]);
}

#[test]
fn test_having_aggregate_not_between() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().not_between(2, 10));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) NOT BETWEEN ? AND ?"#
    );
    assert_eq!(binds, vec![Value::Int(2), Value::Int(10)]);
}

#[test]
fn test_having_aggregate_in_range() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().in_range(5..=10));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) BETWEEN ? AND ?"#
    );
    assert_eq!(binds, vec![Value::Int(5), Value::Int(10)]);
}

#[test]
fn test_having_aggregate_in_range_from() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().in_range(5..));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) >= ?"#
    );
    assert_eq!(binds, vec![Value::Int(5)]);
}

#[test]
fn test_having_aggregate_included() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().included(&[1, 2, 3]));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) IN (?, ?, ?)"#
    );
    assert_eq!(binds, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
}

#[test]
fn test_having_aggregate_not_included() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["product"]);
    q.having(count_all().not_included(&[1, 2, 3]));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT(*) AS "cnt" FROM "orders" GROUP BY "product" HAVING COUNT(*) NOT IN (?, ?, ?)"#
    );
    assert_eq!(binds, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
}

#[test]
fn test_multiple_aggregates() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.add_select(col("price").sum().as_("total"));
    q.add_select(col("price").avg().as_("avg_price"));
    q.add_select(col("price").min().as_("min_price"));
    q.add_select(col("price").max().as_("max_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "product", COUNT("id") AS "cnt", SUM("price") AS "total", AVG("price") AS "avg_price", MIN("price") AS "min_price", MAX("price") AS "max_price" FROM "orders" GROUP BY "product""#
    );
}
