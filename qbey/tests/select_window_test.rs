use qbey::*;

#[test]
fn test_row_number_over_partition_and_order() {
    let mut q = qbey("employee");
    q.select(&["id", "name", "dept", "salary"]);
    q.add_select(
        row_number()
            .over(
                window()
                    .partition_by(&[col("dept")])
                    .order_by(col("salary").desc()),
            )
            .as_("rn"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name", "dept", "salary", ROW_NUMBER() OVER (PARTITION BY "dept" ORDER BY "salary" DESC) AS "rn" FROM "employee""#
    );
}

#[test]
fn test_rank_over() {
    let mut q = qbey("employee");
    q.select(&["id", "salary"]);
    q.add_select(
        rank()
            .over(window().order_by(col("salary").desc()))
            .as_("rnk"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "salary", RANK() OVER (ORDER BY "salary" DESC) AS "rnk" FROM "employee""#
    );
}

#[test]
fn test_dense_rank_over() {
    let mut q = qbey("employee");
    q.select(&["id", "salary"]);
    q.add_select(
        dense_rank()
            .over(window().order_by(col("salary").desc()))
            .as_("drnk"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "salary", DENSE_RANK() OVER (ORDER BY "salary" DESC) AS "drnk" FROM "employee""#
    );
}

#[test]
fn test_window_empty_over() {
    let mut q = qbey("employee");
    q.select(&["id"]);
    q.add_select(row_number().over(window()).as_("rn"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", ROW_NUMBER() OVER () AS "rn" FROM "employee""#
    );
}

#[test]
fn test_sum_over() {
    let mut q = qbey("employee");
    q.select(&["id", "dept", "salary"]);
    q.add_select(
        col("salary")
            .sum_over(window().partition_by(&[col("dept")]))
            .as_("dept_total"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "dept", "salary", SUM("salary") OVER (PARTITION BY "dept") AS "dept_total" FROM "employee""#
    );
}

#[test]
fn test_count_over() {
    let mut q = qbey("employee");
    q.select(&["id", "dept"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("dept")]))
            .as_("dept_count"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "dept", COUNT("id") OVER (PARTITION BY "dept") AS "dept_count" FROM "employee""#
    );
}

#[test]
fn test_window_multiple_partition_by() {
    let mut q = qbey("employee");
    q.select(&["id"]);
    q.add_select(
        row_number()
            .over(
                window()
                    .partition_by(&[col("dept"), col("role")])
                    .order_by(col("salary").desc()),
            )
            .as_("rn"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", ROW_NUMBER() OVER (PARTITION BY "dept", "role" ORDER BY "salary" DESC) AS "rn" FROM "employee""#
    );
}

#[test]
fn test_window_qualified_cols() {
    let e = table("employee");
    let mut q = qbey("employee");
    q.select(&["id"]);
    q.add_select(
        row_number()
            .over(
                window()
                    .partition_by(&[e.col("dept")])
                    .order_by(e.col("salary").desc()),
            )
            .as_("rn"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", ROW_NUMBER() OVER (PARTITION BY "employee"."dept" ORDER BY "employee"."salary" DESC) AS "rn" FROM "employee""#
    );
}

#[test]
fn test_window_pg_dialect() {
    let mut q = qbey("employee");
    q.select(&["id", "dept", "salary"]);
    q.add_select(
        row_number()
            .over(
                window()
                    .partition_by(&[col("dept")])
                    .order_by(col("salary").desc()),
            )
            .as_("rn"),
    );
    q.and_where(col("active").eq(true));

    let (sql, binds) = q.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"SELECT "id", "dept", "salary", ROW_NUMBER() OVER (PARTITION BY "dept" ORDER BY "salary" DESC) AS "rn" FROM "employee" WHERE "active" = $1"#
    );
    assert_eq!(binds, vec![Value::Bool(true)]);
}

#[test]
fn test_window_no_alias() {
    let mut q = qbey("employee");
    q.select(&["id"]);
    q.add_select(row_number().over(window().order_by(col("id").asc())));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", ROW_NUMBER() OVER (ORDER BY "id" ASC) FROM "employee""#
    );
}

#[test]
fn test_avg_over() {
    let mut q = qbey("employee");
    q.select(&["id", "salary"]);
    q.add_select(
        col("salary")
            .avg_over(window().partition_by(&[col("dept")]))
            .as_("avg_salary"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "salary", AVG("salary") OVER (PARTITION BY "dept") AS "avg_salary" FROM "employee""#
    );
}

#[test]
fn test_min_over() {
    let mut q = qbey("employee");
    q.select(&["id", "salary"]);
    q.add_select(
        col("salary")
            .min_over(window().partition_by(&[col("dept")]))
            .as_("min_salary"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "salary", MIN("salary") OVER (PARTITION BY "dept") AS "min_salary" FROM "employee""#
    );
}

#[test]
fn test_max_over() {
    let mut q = qbey("employee");
    q.select(&["id", "salary"]);
    q.add_select(
        col("salary")
            .max_over(window().partition_by(&[col("dept")]))
            .as_("max_salary"),
    );

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "salary", MAX("salary") OVER (PARTITION BY "dept") AS "max_salary" FROM "employee""#
    );
}
