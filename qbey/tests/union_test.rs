use qbey::*;

#[test]
fn test_union_all_to_sql() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let uq = q1.union_all(&q2);

    let (sql, binds) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ?"
    );
    assert_eq!(
        binds,
        vec![
            Value::String("eng".to_string()),
            Value::String("sales".to_string())
        ]
    );
}

#[test]
fn test_union_to_sql() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let uq = q1.union(&q2);

    let (sql, _) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT \"dept\" FROM \"employee\" UNION SELECT \"dept\" FROM \"contractor\""
    );
}

#[test]
fn test_union_all_with_order_by_and_limit() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let mut uq = q1.union_all(&q2);
    uq.order_by(col("name").asc());
    uq.limit(10);

    let (sql, _) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 10"
    );
}

#[test]
fn test_union_query_merge() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let mut q3 = qbey("contractor");
    q3.and_where(("dept", "eng"));
    q3.select(&["id", "name"]);

    let mut q4 = qbey("contractor");
    q4.and_where(("dept", "sales"));
    q4.select(&["id", "name"]);

    let mut uq1 = q1.union_all(&q2);
    let uq2 = q3.union_all(&q4);
    uq1.union_all(&uq2);

    let (sql, _) = uq1.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"contractor\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"contractor\" WHERE \"dept\" = ?"
    );
}

#[test]
fn test_union_with_query_order_by_and_limit() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);
    q1.order_by(col("name").asc());
    q1.limit(5);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);
    q2.order_by(col("name").desc());
    q2.limit(3);

    let mut uq = q1.union_all(&q2);
    uq.order_by(col("id").asc());
    uq.limit(10);

    let (sql, _) = uq.to_sql();
    assert_eq!(
        sql,
        "(SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 5) UNION ALL (SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" DESC LIMIT 3) ORDER BY \"id\" ASC LIMIT 10"
    );
}

#[test]
fn test_union_with_one_query_having_order_by() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);
    q2.order_by(col("name").asc());
    q2.limit(5);

    let uq = q1.union_all(&q2);

    let (sql, _) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL (SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 5)"
    );
}
