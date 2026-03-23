use qbey::*;

#[test]
fn test_update_basic() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.allow_without_where();
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
    let u = u.where_set();
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(1)]
    );
}

#[test]
fn test_update_with_dialect() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set(col("age"), 30);
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("age").between(20, 60));
    let u = u.and_where(col("role").included(&["admin", "manager"]));
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
fn test_update_chained_and_where() {
    let mut u = qbey("employee").into_update();
    u.set(col("status"), "active");
    let u = u
        .and_where(col("age").between(20, 60))
        .and_where(col("role").included(&["admin", "manager"]));
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
    let u = u.and_where(col("status").eq("pending"));
    let u = u.or_where(col("status").eq("draft"));
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
    let u = u.and_where(col("name").like(LikeExpression::starts_with("test")));
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

// Note: test_update_no_where_panics was removed because WHERE-less UPDATE
// is now a compile error (see compile_fail doctest on UpdateQuery).

#[test]
#[should_panic(expected = "UPDATE requires at least one SET clause")]
fn test_update_empty_sets_panics() {
    let u = qbey("employee").into_update();
    let u = u.allow_without_where();
    let _ = u.to_sql();
}

#[test]
fn test_update_with_table_ref_alias() {
    let mut u = qbey(table("employee").as_("e")).into_update();
    u.set(col("name"), "Alice");
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.and_where(col("id").eq(1));
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
    let u = u.allow_without_where();
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
    let u = u.and_where(col("id").eq(1));
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
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    let u = u.and_where(col("id").eq(1));
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

// ── CTE support ──

#[test]
fn test_update_with_cte() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(true));

    let mut u = qbey("employee").into_update();
    u.with_cte("active_depts", &[], cte_q);
    u.set(col("status"), "active");
    let u = u.and_where(col("dept_id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"WITH "active_depts" AS (SELECT "id" FROM "departments" WHERE "active" = ?) UPDATE "employee" SET "status" = ? WHERE "dept_id" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::Bool(true),
            Value::String("active".to_string()),
            Value::Int(1),
        ]
    );
}

#[test]
fn test_update_with_cte_pg_dialect() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(true));

    let mut u = qbey("employee").into_update();
    u.with_cte("active_depts", &[], cte_q);
    u.set(col("status"), "active");
    let u = u.and_where(col("dept_id").eq(1));
    let (sql, binds) = u.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"WITH "active_depts" AS (SELECT "id" FROM "departments" WHERE "active" = $1) UPDATE "employee" SET "status" = $2 WHERE "dept_id" = $3"#
    );
    assert_eq!(
        binds,
        vec![
            Value::Bool(true),
            Value::String("active".to_string()),
            Value::Int(1),
        ]
    );
}

#[test]
fn test_update_with_cte_from_select_query() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(true));

    let mut q = qbey("employee");
    q.with_cte("active_depts", &[], cte_q);
    q.and_where(col("dept_id").eq(1));
    let mut u = q.into_update();
    u.set(col("status"), "active");
    let u = u.where_set();
    let (sql, _) = u.to_sql();
    assert!(sql.starts_with(r#"WITH "active_depts" AS"#));
    assert!(sql.contains(r#"UPDATE "employee""#));
}

// ── SetClause::Expr with binds ──

#[test]
fn test_update_set_expr_with_binds() {
    let mut u = qbey("employee").into_update();
    u.set_expr(RawSql::new(r#""score" = "score" + {}"#).binds(&[10]));
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "score" = "score" + ? WHERE "id" = ?"#
    );
    assert_eq!(binds, vec![Value::Int(10), Value::Int(1)]);
}

#[test]
fn test_update_set_expr_with_binds_pg() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""score" = "score" + {}"#).binds(&[10]));
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = $1, "score" = "score" + $2 WHERE "id" = $3"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(10),
            Value::Int(1),
        ]
    );
}

#[test]
fn test_update_set_expr_with_binds_mixed() {
    let mut u = qbey("employee").into_update();
    u.set(col("name"), "Alice");
    u.set_expr(RawSql::new(r#""score" = COALESCE({}, {})"#).binds(&[100, 0]));
    u.set(col("status"), "active");
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "employee" SET "name" = ?, "score" = COALESCE(?, ?), "status" = ? WHERE "id" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(100),
            Value::Int(0),
            Value::String("active".to_string()),
            Value::Int(1),
        ]
    );
}

#[test]
fn test_update_with_bytes_value() {
    let mut u = qbey("files").into_update();
    let data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    u.set(col("data"), Value::Bytes(data.clone()));
    let u = u.and_where(col("id").eq(1));
    let (sql, binds) = u.to_sql();
    assert_eq!(sql, r#"UPDATE "files" SET "data" = ? WHERE "id" = ?"#);
    assert_eq!(binds, vec![Value::Bytes(data), Value::Int(1)]);
}
