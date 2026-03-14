use sqipe::*;

#[test]
fn test_aggregate_to_sql() {
    let mut q = sqipe("employee");
    q.aggregate(&[
        aggregate::count_all().as_("cnt"),
        aggregate::sum("salary").as_("total_salary"),
    ]);
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"dept\", COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" FROM \"employee\" GROUP BY \"dept\""
    );
}

#[test]
fn test_aggregate_to_pipe_sql() {
    let mut q = sqipe("employee");
    q.aggregate(&[
        aggregate::count_all().as_("cnt"),
        aggregate::sum("salary").as_("total_salary"),
    ]);
    q.group_by(&["dept"]);

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> AGGREGATE COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" GROUP BY \"dept\""
    );
}

#[test]
fn test_aggregate_without_group_by() {
    let mut q = sqipe("employee");
    q.aggregate(&[aggregate::count_all().as_("cnt")]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT COUNT(*) AS \"cnt\" FROM \"employee\"");

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(sql, "FROM \"employee\" |> AGGREGATE COUNT(*) AS \"cnt\"");
}

#[test]
fn test_aggregate_with_where() {
    let mut q = sqipe("employee");
    q.and_where(col("active").eq(true));
    q.aggregate(&[aggregate::count_all().as_("cnt")]);
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" WHERE \"active\" = ? GROUP BY \"dept\""
    );

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> WHERE \"active\" = ? |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"dept\""
    );
}

#[test]
fn test_aggregate_all_functions() {
    let mut q = sqipe("employee");
    q.aggregate(&[
        aggregate::count_all().as_("cnt"),
        aggregate::count("id").as_("id_cnt"),
        aggregate::sum("salary").as_("total"),
        aggregate::avg("salary").as_("average"),
        aggregate::min("salary").as_("lowest"),
        aggregate::max("salary").as_("highest"),
    ]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT COUNT(*) AS \"cnt\", COUNT(\"id\") AS \"id_cnt\", SUM(\"salary\") AS \"total\", AVG(\"salary\") AS \"average\", MIN(\"salary\") AS \"lowest\", MAX(\"salary\") AS \"highest\" FROM \"employee\""
    );
}

#[test]
fn test_aggregate_expr_raw() {
    let mut q = sqipe("employee");
    q.aggregate(&[
        aggregate::count_all().as_("cnt"),
        aggregate::expr(RawSql::new("APPROX_COUNT_DISTINCT(user_id)")).as_("approx_users"),
    ]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT COUNT(*) AS \"cnt\", APPROX_COUNT_DISTINCT(user_id) AS \"approx_users\" FROM \"employee\""
    );
}

#[test]
fn test_having_auto_detect() {
    let mut q = sqipe("employee");
    q.and_where(col("active").eq(true));
    q.aggregate(&[aggregate::count_all().as_("cnt")]);
    q.group_by(&["dept"]);
    q.and_where(col("cnt").gt(5));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" WHERE \"active\" = ? GROUP BY \"dept\" HAVING \"cnt\" > ?"
    );

    let (sql, _) = q.to_pipe_sql();
    assert_eq!(
        sql,
        "FROM \"employee\" |> WHERE \"active\" = ? |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"dept\" |> WHERE \"cnt\" > ?"
    );
}
