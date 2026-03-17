use qbey::*;

#[test]
fn test_basic_cte() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id", "name"]);
    cte_q.and_where(col("active").eq(true));

    let mut q = qbey("dept_cte");
    q.with_cte("dept_cte", &[], cte_q);
    q.select(&["id", "name"]);

    let (sql, _binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"WITH "dept_cte" AS (SELECT "id", "name" FROM "departments" WHERE "active" = ?) SELECT "id", "name" FROM "dept_cte""#
    );
}

#[test]
fn test_cte_with_column_aliases() {
    let mut cte_q = qbey("employees");
    cte_q.select(&["id", "first_name", "last_name"]);

    let mut q = qbey("emp");
    q.with_cte("emp", &["eid", "fname", "lname"], cte_q);
    q.select(&["eid", "fname"]);

    let (sql, _binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"WITH "emp" ("eid", "fname", "lname") AS (SELECT "id", "first_name", "last_name" FROM "employees") SELECT "eid", "fname" FROM "emp""#
    );
}

#[test]
fn test_multiple_ctes() {
    let mut cte1 = qbey("departments");
    cte1.select(&["id", "name"]);
    cte1.and_where(col("active").eq(true));

    let mut cte2 = qbey("employees");
    cte2.select(&["id", "name", "dept_id"]);

    let mut q = qbey("active_depts");
    q.with_cte("active_depts", &[], cte1);
    q.with_cte("all_emps", &[], cte2);
    q.select(&["id", "name"]);
    q.join(
        "all_emps",
        col("active_depts.id").eq_col("all_emps.dept_id"),
    );

    let (sql, _binds) = q.to_sql();
    assert!(sql.starts_with(r#"WITH "active_depts" AS (SELECT"#));
    assert!(sql.contains(r#", "all_emps" AS (SELECT"#));
}

#[test]
fn test_recursive_cte() {
    let mut base = qbey("employees");
    base.select(&["id", "name", "manager_id"]);
    base.and_where(col("manager_id").eq(0));

    let mut recursive = qbey("employees");
    recursive.select(&["id", "name", "manager_id"]);

    let cte_query = base.union_all(&recursive);

    let mut q = qbey("org_tree");
    q.with_recursive_cte("org_tree", &["id", "name", "manager_id"], cte_query);
    q.select(&["id", "name"]);

    let (sql, _binds) = q.to_sql();
    assert!(sql.starts_with(r#"WITH RECURSIVE "org_tree""#));
    assert!(sql.contains("UNION ALL"));
}

#[test]
fn test_cte_with_pg_dialect() {
    let mut cte_q = qbey("users");
    cte_q.select(&["id", "name"]);
    cte_q.and_where(col("age").gt(25));

    let mut q = qbey("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"WITH "older_users" AS (SELECT "id", "name" FROM "users" WHERE "age" > $1) SELECT "id", "name" FROM "older_users""#
    );
    assert_eq!(binds.len(), 1);
}

#[test]
fn test_cte_with_compound_query() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id", "name"]);
    cte_q.and_where(col("active").eq(true));

    let mut q1 = qbey("dept_cte");
    q1.with_cte("dept_cte", &[], cte_q);
    q1.select(&["id", "name"]);

    let mut q2 = qbey("dept_cte");
    q2.select(&["id", "name"]);
    q2.and_where(col("name").eq("HR"));

    let uq = q1.union(&q2);
    let (sql, _binds) = uq.to_sql();

    // CTE should appear before the UNION
    assert!(sql.starts_with(r#"WITH "dept_cte" AS (SELECT"#));
    assert!(sql.contains("UNION"));
}

#[test]
fn test_cte_bind_parameter_order() {
    let mut cte_q = qbey("users");
    cte_q.select(&["id", "name"]);
    cte_q.and_where(col("age").gt(25));

    let mut q = qbey("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.and_where(col("name").eq("Alice"));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"WITH "older_users" AS (SELECT "id", "name" FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "older_users" WHERE "name" = ?"#
    );
    assert_eq!(binds.len(), 2);
    assert_eq!(binds[0], Value::Int(25));
    assert_eq!(binds[1], Value::String("Alice".to_string()));
}

#[test]
fn test_cte_with_add_union() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id", "name"]);

    let mut q = qbey("dept_cte");
    q.with_cte("dept_cte", &[], cte_q);
    q.select(&["id", "name"]);

    let mut q2 = qbey("dept_cte");
    q2.select(&["id", "name"]);
    q2.and_where(col("name").eq("HR"));

    q.add_union(&q2);

    let (sql, _binds) = q.to_sql();
    // CTE should be preserved after add_union
    assert!(sql.starts_with(r#"WITH "dept_cte" AS (SELECT"#));
    assert!(sql.contains("UNION"));
}

#[test]
fn test_cte_with_from_subquery() {
    let mut cte_q = qbey("orders");
    cte_q.select(&["user_id", "amount"]);
    cte_q.and_where(col("status").eq("completed"));

    let mut sub = qbey("order_summary");
    sub.select(&["user_id"]);

    let mut q = qbey_from_subquery(sub, "t");
    q.with_cte("order_summary", &[], cte_q);
    q.select(&["user_id"]);

    let (sql, binds) = q.to_sql();
    assert!(sql.starts_with(r#"WITH "order_summary" AS (SELECT"#));
    assert!(sql.contains(r#"FROM (SELECT "user_id" FROM "order_summary") AS "t""#));
    assert_eq!(binds.len(), 1);
}

#[test]
fn test_cte_with_join_subquery() {
    let mut cte_q = qbey("departments");
    cte_q.select(&["id", "name"]);

    let mut sub = qbey("dept_cte");
    sub.select(&["id", "name"]);

    let mut q = qbey("employees");
    q.with_cte("dept_cte", &[], cte_q);
    q.select(&["id", "name"]);
    q.join_subquery(sub, "d", col("employees.dept_id").eq_col("d.id"));

    let (sql, _binds) = q.to_sql();
    assert!(sql.starts_with(r#"WITH "dept_cte" AS (SELECT"#));
    assert!(sql.contains("INNER JOIN"));
}
