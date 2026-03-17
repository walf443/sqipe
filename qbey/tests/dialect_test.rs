use qbey::*;

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
fn test_col_count_numbered_placeholder() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["dept"]);
    q.and_where(("status", "active"));
    let (sql, binds) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"dept\", COUNT(\"id\") AS \"cnt\" FROM \"employee\" WHERE \"status\" = $1 GROUP BY \"dept\""
    );
    assert_eq!(binds, vec![Value::String("active".to_string())]);
}

#[test]
fn test_count_all_numbered_placeholder() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(count_all().as_("cnt"));
    q.group_by(&["dept"]);
    let (sql, _) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" GROUP BY \"dept\""
    );
}

#[test]
fn test_count_one_numbered_placeholder() {
    let mut q = qbey("employee");
    q.add_select(count_one().as_("cnt"));
    let (sql, _) = q.to_sql_with(&PgDialect);

    assert_eq!(sql, "SELECT COUNT(1) AS \"cnt\" FROM \"employee\"");
}

#[test]
fn test_col_sum_numbered_placeholder() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").sum().as_("total"));
    q.group_by(&["product"]);
    q.and_where(("status", "active"));
    let (sql, binds) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"product\", SUM(\"price\") AS \"total\" FROM \"orders\" WHERE \"status\" = $1 GROUP BY \"product\""
    );
    assert_eq!(binds, vec![Value::String("active".to_string())]);
}

#[test]
fn test_col_avg_numbered_placeholder() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").avg().as_("avg_price"));
    q.group_by(&["product"]);
    let (sql, _) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"product\", AVG(\"price\") AS \"avg_price\" FROM \"orders\" GROUP BY \"product\""
    );
}

#[test]
fn test_col_min_numbered_placeholder() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").min().as_("min_price"));
    q.group_by(&["product"]);
    let (sql, _) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"product\", MIN(\"price\") AS \"min_price\" FROM \"orders\" GROUP BY \"product\""
    );
}

#[test]
fn test_col_max_numbered_placeholder() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").max().as_("max_price"));
    q.group_by(&["product"]);
    let (sql, _) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT \"product\", MAX(\"price\") AS \"max_price\" FROM \"orders\" GROUP BY \"product\""
    );
}

#[test]
fn test_distinct_numbered_placeholder() {
    let mut q = qbey("employee");
    q.distinct();
    q.select(&["dept"]);
    q.and_where(col("age").gt(20));
    let (sql, binds) = q.to_sql_with(&PgDialect);

    assert_eq!(
        sql,
        "SELECT DISTINCT \"dept\" FROM \"employee\" WHERE \"age\" > $1"
    );
    assert_eq!(binds, vec![Value::Int(20)]);
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
