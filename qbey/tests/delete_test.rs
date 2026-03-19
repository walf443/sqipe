use qbey::*;

#[test]
fn test_delete_basic() {
    let mut d = qbey("employee").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_allow_without_where() {
    let mut d = qbey("employee").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee""#);
    assert_eq!(binds, vec![]);
}

#[test]
fn test_delete_from_query_with_where() {
    let mut q = qbey("employee");
    q.and_where(col("id").eq(1));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_with_dialect() {
    let mut d = qbey("employee").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql_with(&PgDialect);
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = $1"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_with_complex_where() {
    let mut d = qbey("employee").into_delete();
    d.and_where(col("age").between(20, 60));
    d.and_where(col("role").included(&["admin", "manager"]));
    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        r#"DELETE FROM "employee" WHERE "age" BETWEEN ? AND ? AND "role" IN (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::Int(20),
            Value::Int(60),
            Value::String("admin".to_string()),
            Value::String("manager".to_string()),
        ]
    );
}

#[test]
fn test_delete_with_or_where() {
    let mut d = qbey("employee").into_delete();
    d.and_where(col("status").eq("pending"));
    d.or_where(col("status").eq("draft"));
    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        r#"DELETE FROM "employee" WHERE "status" = ? OR "status" = ?"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("pending".to_string()),
            Value::String("draft".to_string()),
        ]
    );
}

#[test]
fn test_delete_with_like() {
    let mut d = qbey("employee").into_delete();
    d.and_where(col("name").like(LikeExpression::starts_with("test")));
    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        r#"DELETE FROM "employee" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("test%".to_string())]);
}

#[test]
#[should_panic(expected = "DELETE without WHERE is dangerous")]
fn test_delete_no_where_panics() {
    let d = qbey("employee").into_delete();
    let _ = d.to_sql();
}

#[test]
fn test_delete_with_table_ref_alias() {
    let mut d = qbey(table("employee").as_("e")).into_delete();
    d.and_where(col("id").eq(1));
    let (sql, _) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
}

#[test]
fn test_delete_with_table_alias() {
    let mut q = qbey("employee");
    q.as_("e");
    let mut d = q.into_delete();
    d.and_where(col("id").eq(1));
    let (sql, _) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
}

// ── CTE support ──

#[test]
fn test_delete_with_cte() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(false));

    let mut d = qbey("employee").into_delete();
    d.with_cte("inactive_depts", &[], cte_q);
    d.and_where(col("dept_id").eq(1));
    let (sql, binds) = d.to_sql();
    assert_eq!(
        sql,
        r#"WITH "inactive_depts" AS (SELECT "id" FROM "departments" WHERE "active" = ?) DELETE FROM "employee" WHERE "dept_id" = ?"#
    );
    assert_eq!(binds, vec![Value::Bool(false), Value::Int(1)]);
}

#[test]
fn test_delete_with_cte_pg_dialect() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(false));

    let mut d = qbey("employee").into_delete();
    d.with_cte("inactive_depts", &[], cte_q);
    d.and_where(col("dept_id").eq(1));
    let (sql, binds) = d.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"WITH "inactive_depts" AS (SELECT "id" FROM "departments" WHERE "active" = $1) DELETE FROM "employee" WHERE "dept_id" = $2"#
    );
    assert_eq!(binds, vec![Value::Bool(false), Value::Int(1)]);
}

#[test]
fn test_delete_with_cte_from_select_query() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id"]);
    cte_q.and_where(col("active").eq(false));

    let mut q = qbey("employee");
    q.with_cte("inactive_depts", &[], cte_q);
    q.and_where(col("dept_id").eq(1));
    let d = q.into_delete();
    let (sql, _) = d.to_sql();
    assert!(sql.starts_with(r#"WITH "inactive_depts" AS"#));
    assert!(sql.contains(r#"DELETE FROM "employee""#));
}

#[test]
fn test_delete_with_recursive_cte() {
    let mut base = qbey("categories");
    base.select(&["id"]);
    base.and_where(col("parent_id").eq(1));

    let mut recursive = qbey("categories");
    recursive.select(&["id"]);

    let cte_query = base.union_all(&recursive);

    let mut d = qbey("items").into_delete();
    d.with_recursive_cte("cat_tree", &["id"], cte_query);
    d.and_where(col("category_id").eq(1));
    let (sql, _) = d.to_sql();
    assert!(sql.starts_with(r#"WITH RECURSIVE "cat_tree""#));
    assert!(sql.contains(r#"DELETE FROM "items""#));
}

#[test]
#[should_panic(expected = "JOINs which are not supported in DELETE")]
fn test_delete_from_query_with_joins_panics() {
    let mut q = qbey("employee");
    q.join("department", table("employee").col("dept_id").eq(col("id")));
    let _ = q.into_delete();
}

#[test]
#[should_panic(expected = "ORDER BY which is not supported in DELETE")]
fn test_delete_from_query_with_order_by_panics() {
    let mut q = qbey("employee");
    q.order_by(col("id").asc());
    let _ = q.into_delete();
}

#[test]
#[should_panic(expected = "LIMIT which is not supported in DELETE")]
fn test_delete_from_query_with_limit_panics() {
    let mut q = qbey("employee");
    q.limit(10);
    let _ = q.into_delete();
}
