use qbey::*;

#[test]
fn test_update_basic() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}

#[test]
fn test_update_multiple_sets() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set(col("age"), 30);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = ?, "age" = ? WHERE "id" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::Int(1)
        ]
    );
}

#[test]
fn test_update_allow_without_where() {
    let mut u = qbey("employee").into_update();
    u.set(col("status"), "inactive");
    u.allow_without_where();
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "status" = ?"#);
    assert_eq!(binds, vec![Value::String("inactive".to_string())]);
}

#[test]
fn test_update_from_query_with_where() {
    let mut q = qbey("employee");
    q.and_where(col("id").eq(1));
    let mut u = q.into_update();
    u.set(col("name"), "Alice");
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}

#[test]
fn test_update_with_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set(col("age"), 30);
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = $1, "age" = $2 WHERE "id" = $3"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::Int(1)
        ]
    );
}

#[test]
fn test_update_with_complex_where() {
    let mut u = qbey("employee").into_update();
    u.set(col("status"), "active");
    u.and_where(col("age").between(20, 60));
    u.and_where(col("role").included(&["admin", "manager"]));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "status" = ? WHERE "age" BETWEEN ? AND ? AND "role" IN (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("active".to_string()),
            Value::Int(20),
            Value::Int(60),
            Value::String("admin".to_string()),
            Value::String("manager".to_string()),
        ]
    );
}

#[test]
fn test_update_with_or_where() {
    let mut u = qbey("employee").into_update();
    u.set(col("reviewed"), true);
    u.and_where(col("status").eq("pending"));
    u.or_where(col("status").eq("draft"));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "reviewed" = ? WHERE "status" = ? OR "status" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::Bool(true),
            Value::String("pending".to_string()),
            Value::String("draft".to_string()),
        ]
    );
}

#[test]
fn test_update_with_like() {
    let mut u = qbey("employee").into_update();
    u.set(col("flagged"), true);
    u.and_where(col("name").like(LikeExpression::starts_with("test")));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "flagged" = ? WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(
        binds,
        vec![Value::Bool(true), Value::String("test%".to_string()),]
    );
}

#[test]
#[should_panic(expected = "UPDATE requires at least one SET clause")]
fn test_update_empty_sets_panics() {
    let mut u = qbey("employee").into_update();
    u.allow_without_where();
    let _ = u.to_sql();
}

#[test]
#[should_panic(expected = "UPDATE without WHERE is dangerous")]
fn test_update_no_where_panics() {
    let mut u = qbey("employee").into_update();
    u.set(col("status"), "inactive");
    let _ = u.to_sql();
}

#[test]
fn test_update_with_table_ref_alias() {
    let mut u = qbey(table("employee").as_("e")).into_update();
    u.set(col("name"), "Alice");
    u.and_where(col("id").eq(1));
    let (sql, _) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" "e" SET "name" = ? WHERE "id" = ?"#
    );
}

#[test]
fn test_update_with_table_alias() {
    let mut q = qbey("employee");
    q.as_("e");
    let mut u = q.into_update();
    u.set(col("name"), "Alice");
    u.and_where(col("id").eq(1));
    let (sql, _) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" "e" SET "name" = ? WHERE "id" = ?"#
    );
}

#[test]
fn test_update_set_with_qualified_col() {
    let mut u = qbey("employee").into_update();
    u.set(table("employee").col("name"), "Alice");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}

#[test]
fn test_update_with_set_expr() {
    let mut u = qbey("employee").into_update();
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#
    );
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_update_with_set_and_set_expr_mixed() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = ?, "visit_count" = "visit_count" + 1 WHERE "id" = ?"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}

#[test]
fn test_update_with_multiple_set_exprs() {
    let mut u = qbey("employee").into_update();
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    u.set_expr(RawSql::new(r#""updated_at" = NOW()"#));
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1, "updated_at" = NOW() WHERE "id" = ?"#
    );
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_update_with_set_expr_allow_without_where() {
    let mut u = qbey("employee").into_update();
    u.set_expr(RawSql::new(r#""version" = "version" + 1"#));
    u.allow_without_where();
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "version" = "version" + 1"#);
    assert_eq!(binds, vec![]);
}

#[test]
fn test_update_with_set_expr_bind_order() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    u.set(col("status"), "active");
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = ?, "visit_count" = "visit_count" + 1, "status" = ? WHERE "id" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::String("active".to_string()),
            Value::Int(1),
        ]
    );
}

#[test]
fn test_update_with_set_expr_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = $1, "visit_count" = "visit_count" + 1 WHERE "id" = $2"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}
