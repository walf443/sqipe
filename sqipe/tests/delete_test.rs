use sqipe::*;

#[test]
fn test_delete_basic() {
    let mut d = sqipe("employee").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_allow_without_where() {
    let mut d = sqipe("employee").into_delete();
    d.allow_without_where();
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee""#);
    assert_eq!(binds, vec![]);
}

#[test]
fn test_delete_from_query_with_where() {
    let mut q = sqipe("employee");
    q.and_where(col("id").eq(1));
    let d = q.into_delete();
    let (sql, binds) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_with_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut d = sqipe("employee").into_delete();
    d.and_where(col("id").eq(1));
    let (sql, binds) = d.to_sql_with(&PgDialect);
    assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = $1"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_delete_with_complex_where() {
    let mut d = sqipe("employee").into_delete();
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
    let mut d = sqipe("employee").into_delete();
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
    let mut d = sqipe("employee").into_delete();
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
    let d = sqipe("employee").into_delete();
    let _ = d.to_sql();
}

#[test]
fn test_delete_with_table_ref_alias() {
    let mut d = sqipe(table("employee").as_("e")).into_delete();
    d.and_where(col("id").eq(1));
    let (sql, _) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
}

#[test]
fn test_delete_with_table_alias() {
    let mut q = sqipe("employee");
    q.as_("e");
    let mut d = q.into_delete();
    d.and_where(col("id").eq(1));
    let (sql, _) = d.to_sql();
    assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
}

#[test]
#[should_panic(expected = "JOINs which are not supported in DELETE")]
fn test_delete_from_query_with_joins_panics() {
    let mut q = sqipe("employee");
    q.join("department", table("employee").col("dept_id").eq_col("id"));
    let _ = q.into_delete();
}

#[test]
#[should_panic(expected = "aggregates which are not supported in DELETE")]
fn test_delete_from_query_with_aggregates_panics() {
    let mut q = sqipe("employee");
    q.aggregate(&[aggregate::count_all()]);
    let _ = q.into_delete();
}

#[test]
#[should_panic(expected = "ORDER BY which is not supported in DELETE")]
fn test_delete_from_query_with_order_by_panics() {
    let mut q = sqipe("employee");
    q.order_by(col("id").asc());
    let _ = q.into_delete();
}

#[test]
#[should_panic(expected = "LIMIT which is not supported in DELETE")]
fn test_delete_from_query_with_limit_panics() {
    let mut q = sqipe("employee");
    q.limit(10);
    let _ = q.into_delete();
}
