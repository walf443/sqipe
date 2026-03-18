use qbey::*;

#[test]
fn test_comparison_operators() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(col("age").gt(20));
    q.and_where(col("age").lte(60));
    q.and_where(col("salary").lt(100000));
    q.and_where(col("level").gte(3));
    q.and_where(col("role").ne("intern"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ? AND \"age\" <= ? AND \"salary\" < ? AND \"level\" >= ? AND \"role\" != ?"
    );
}

#[test]
fn test_or_where() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.or_where(col("role").eq("admin"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"name\" = ? OR \"role\" = ?"
    );
}

#[test]
fn test_any_grouping() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(any(col("role").eq("admin"), col("role").eq("manager")));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"name\" = ? AND (\"role\" = ? OR \"role\" = ?)"
    );
}

#[test]
fn test_any_all_combined() {
    let mut q = qbey("employee");
    q.and_where(any(
        all(col("role").eq("admin"), col("dept").eq("eng")),
        all(col("role").eq("manager"), col("dept").eq("sales")),
    ));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE (\"role\" = ? AND \"dept\" = ?) OR (\"role\" = ? AND \"dept\" = ?)"
    );
}

#[test]
fn test_not_where() {
    let mut q = qbey("employee");
    q.and_where(not(col("role").eq("admin")));

    let (sql, binds) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
    assert_eq!(binds, vec![Value::String("admin".to_string())]);
}

#[test]
fn test_not_where_with_and() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(not(col("role").eq("admin")));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"name\" = ? AND NOT (\"role\" = ?)"
    );
}

#[test]
fn test_not_where_with_or() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.or_where(not(col("role").eq("admin")));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"name\" = ? OR NOT (\"role\" = ?)"
    );
}

#[test]
fn test_not_with_any() {
    let mut q = qbey("employee");
    q.and_where(not(any(col("role").eq("admin"), col("role").eq("manager"))));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE NOT ((\"role\" = ? OR \"role\" = ?))"
    );
}

#[test]
fn test_not_operator() {
    let mut q = qbey("employee");
    q.and_where(!col("role").eq("admin"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
}

#[test]
fn test_in_clause() {
    let mut q = qbey("employee");
    q.and_where(col("id").included(&[1, 2, 3]));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" IN (?, ?, ?)"
    );
}

#[test]
fn test_not_in_clause() {
    let mut q = qbey("employee");
    q.and_where(col("id").not_included(&[1, 2, 3]));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" NOT IN (?, ?, ?)"
    );
}

#[test]
fn test_empty_in_clause() {
    let empty: &[i32] = &[];
    let mut q = qbey("employee");
    q.and_where(col("id").included(empty));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE 1 = 0");
}

#[test]
fn test_empty_not_in_clause() {
    let empty: &[i32] = &[];
    let mut q = qbey("employee");
    q.and_where(col("id").not_included(empty));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE 1 = 1");
}

#[test]
fn test_in_subquery() {
    let mut sub = qbey("employee");
    sub.and_where(("dept", "eng"));
    sub.select(&["id"]);

    let mut q = qbey("employee");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = ?)"
    );
}

#[test]
fn test_not_in_subquery() {
    let mut sub = qbey("employee");
    sub.and_where(("dept", "eng"));
    sub.select(&["id"]);

    let mut q = qbey("employee");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" NOT IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = ?)"
    );
}

#[test]
fn test_exists_subquery() {
    let mut sub = qbey("orders");
    sub.select(&["id"]);
    sub.and_where(("user_id", 1));

    let mut q = qbey("users");
    q.select(&["name"]);
    q.and_where(exists(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "name" FROM "users" WHERE EXISTS (SELECT "id" FROM "orders" WHERE "user_id" = ?)"#
    );
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_not_exists_subquery() {
    let mut sub = qbey("orders");
    sub.select(&["id"]);
    sub.and_where(("user_id", 1));

    let mut q = qbey("users");
    q.select(&["name"]);
    q.and_where(not_exists(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "name" FROM "users" WHERE NOT EXISTS (SELECT "id" FROM "orders" WHERE "user_id" = ?)"#
    );
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_exists_with_other_conditions() {
    let mut sub = qbey("orders");
    sub.select(&["id"]);
    sub.and_where(("status", "shipped"));

    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.and_where(col("age").gt(25));
    q.and_where(exists(sub));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "age" > ? AND EXISTS (SELECT "id" FROM "orders" WHERE "status" = ?)"#
    );
}

#[test]
fn test_exists_with_or_where() {
    let mut sub = qbey("orders");
    sub.select(&["id"]);
    sub.and_where(("status", "shipped"));

    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.and_where(("name", "Alice"));
    q.or_where(exists(sub));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "name" = ? OR EXISTS (SELECT "id" FROM "orders" WHERE "status" = ?)"#
    );
}

#[test]
fn test_between() {
    let mut q = qbey("employee");
    q.and_where(col("age").between(20, 30));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
    );
    assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
}

#[test]
fn test_not_between() {
    let mut q = qbey("employee");
    q.and_where(col("age").not_between(20, 30));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"age\" NOT BETWEEN ? AND ?"
    );
    assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
}

#[test]
fn test_between_with_in_range() {
    let mut q = qbey("employee");
    q.and_where(col("age").in_range(20..=30));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
    );
    assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
}

#[test]
fn test_range_exclusive_with_in_range() {
    let mut q = qbey("employee");
    q.and_where(col("age").in_range(20..30));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM \"employee\" WHERE \"age\" >= ? AND \"age\" < ?"
    );
    assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
}

#[test]
fn test_range_from_with_in_range() {
    let mut q = qbey("employee");
    q.and_where(col("age").in_range(20..));

    let (sql, binds) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" >= ?");
    assert_eq!(binds, vec![Value::Int(20)]);
}

#[test]
fn test_range_to_with_in_range() {
    let mut q = qbey("employee");
    q.and_where(col("age").in_range(..30));

    let (sql, binds) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" < ?");
    assert_eq!(binds, vec![Value::Int(30)]);
}

#[test]
fn test_range_to_inclusive_with_in_range() {
    let mut q = qbey("employee");
    q.and_where(col("age").in_range(..=30));

    let (sql, binds) = q.to_sql();
    assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" <= ?");
    assert_eq!(binds, vec![Value::Int(30)]);
}

#[test]
fn test_col_alias_ignored_in_where() {
    let mut q = qbey("employee");
    q.and_where(col("name").as_("n").eq("Alice"));

    let (sql, _) = q.to_sql();
    // alias is ignored in WHERE — only the column name is rendered
    assert_eq!(sql, r#"SELECT * FROM "employee" WHERE "name" = ?"#);
}

#[test]
fn test_qualified_col_alias_ignored_in_where() {
    let mut q = qbey("employee");
    q.join("dept", table("employee").col("dept_id").eq_col("id"));
    q.and_where(table("employee").col("name").as_("n").eq("Alice"));

    let (sql, _) = q.to_sql();
    // alias is ignored in WHERE — only table.column is rendered
    assert_eq!(
        sql,
        r#"SELECT * FROM "employee" INNER JOIN "dept" ON "employee"."dept_id" = "dept"."id" WHERE "employee"."name" = ?"#
    );
}

#[test]
fn test_eq_col_in_where() {
    let mut q = qbey("users");
    q.select(&["name"]);
    q.and_where(
        table("users")
            .col("dept_id")
            .eq_col(table("depts").col("id")),
    );

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "name" FROM "users" WHERE "users"."dept_id" = "depts"."id""#
    );
    assert!(binds.is_empty());
}

#[test]
fn test_eq_col_correlated_subquery_with_exists() {
    let mut sub = qbey("orders");
    sub.select(&["id"]);
    sub.and_where(
        table("orders")
            .col("user_id")
            .eq_col(table("users").col("id")),
    );

    let mut q = qbey("users");
    q.select(&["name"]);
    q.and_where(exists(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "name" FROM "users" WHERE EXISTS (SELECT "id" FROM "orders" WHERE "orders"."user_id" = "users"."id")"#
    );
    assert!(binds.is_empty());
}
