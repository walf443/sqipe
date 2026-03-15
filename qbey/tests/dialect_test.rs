use qbey::*;

struct PgDialect;
impl Dialect for PgDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }
}

#[test]
fn test_numbered_placeholders() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(col("age").gt(20));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = $1 AND \"age\" > $2"
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(20),]
    );
}

#[test]
fn test_not_numbered_placeholders() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(not(col("role").eq("admin")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = $1 AND NOT (\"role\" = $2)"
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::String("admin".to_string())
        ]
    );
}

#[test]
fn test_in_subquery_numbered_placeholder() {
    let mut sub = qbey("employee");
    sub.and_where(("dept", "eng"));
    sub.select(&["id"]);

    let mut q = qbey("employee");
    q.and_where(col("name").eq("Alice"));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = $1 AND \"id\" IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = $2)"
    );
}
