#[doc = include_str!("../../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

pub mod aggregate;
pub mod column;
pub mod delete;
pub mod join;
pub mod like;
pub mod query;
pub mod renderer;
pub mod tree;
pub mod update;
pub mod value;
pub mod where_clause;

#[derive(Debug, Clone)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub col: String,
    pub dir: SortDir,
}

// Re-export all public types at the crate root for backwards compatibility.
pub use aggregate::AggregateExpr;
pub use column::{Col, ColRef, IntoColRef, QualifiedCol, TableRef, col, table};
pub use delete::DeleteQuery;
pub use join::{JoinClause, JoinCol, JoinCondition, JoinType};
pub use like::LikeExpression;
pub use query::{
    AsUnionParts, Dialect, IntoJoinTable, IntoSelectTree, Query, SetOp, UnionQuery, UnionQueryOps,
    sqipe, sqipe_from_subquery, sqipe_from_subquery_with, sqipe_with,
};
pub use update::{SetClause, SetExpression, UpdateQuery};
pub use value::{Op, Value};
pub use where_clause::{
    IntoIncluded, IntoRangeClause, IntoWhereClause, WhereClause, all, any, not,
};

// Crate-internal re-exports used by renderer and tree modules.
pub(crate) use aggregate::AggregateFunc;
pub(crate) use where_clause::WhereEntry;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_select_to_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ?"
        );
        assert_eq!(binds, vec![Value::String("Alice".to_string())]);
    }

    #[test]
    fn test_basic_select_to_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"name\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_select_star_when_no_select() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ?");
    }

    #[test]
    fn test_comparison_operators() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));
        q.and_where(col("age").lte(60));
        q.and_where(col("salary").lt(100000));
        q.and_where(col("level").gte(3));
        q.and_where(col("role").ne("intern"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ? AND \"age\" <= ? AND \"salary\" < ? AND \"level\" >= ? AND \"role\" != ?"
        );
    }

    #[test]
    fn test_or_where() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.or_where(col("role").eq("admin"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? OR \"role\" = ?"
        );
    }

    #[test]
    fn test_any_grouping() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(any(col("role").eq("admin"), col("role").eq("manager")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? AND (\"role\" = ? OR \"role\" = ?)"
        );
    }

    #[test]
    fn test_any_all_combined() {
        let mut q = sqipe("employee");
        q.and_where(any(
            all(col("role").eq("admin"), col("dept").eq("eng")),
            all(col("role").eq("manager"), col("dept").eq("sales")),
        ));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE (\"role\" = ? AND \"dept\" = ?) OR (\"role\" = ? AND \"dept\" = ?)"
        );
    }

    #[test]
    fn test_not_where() {
        let mut q = sqipe("employee");
        q.and_where(not(col("role").eq("admin")));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
        assert_eq!(binds, vec![Value::String("admin".to_string())]);
    }

    #[test]
    fn test_not_where_with_and() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? AND NOT (\"role\" = ?)"
        );
    }

    #[test]
    fn test_not_where_with_or() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.or_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? OR NOT (\"role\" = ?)"
        );
    }

    #[test]
    fn test_not_with_any() {
        let mut q = sqipe("employee");
        q.and_where(not(any(col("role").eq("admin"), col("role").eq("manager"))));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE NOT ((\"role\" = ? OR \"role\" = ?))"
        );
    }

    #[test]
    fn test_not_operator() {
        let mut q = sqipe("employee");
        q.and_where(!col("role").eq("admin"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
    }

    #[test]
    fn test_not_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE NOT (\"role\" = ?) |> SELECT *"
        );
    }

    #[test]
    fn test_not_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut q = sqipe("employee");
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
    fn test_order_by() {
        let mut q = sqipe("employee");
        q.select(&["id", "name", "age"]);
        q.order_by(col("name").asc());
        q.order_by(col("age").desc());

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC"
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> SELECT \"id\", \"name\", \"age\" |> ORDER BY \"name\" ASC, \"age\" DESC"
        );
    }

    #[test]
    fn test_limit_offset() {
        let mut q = sqipe("employee");
        q.select(&["id", "name"]);
        q.limit(10);
        q.offset(20);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20"
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> SELECT \"id\", \"name\" |> LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn test_method_chaining() {
        let (sql, _) = sqipe("employee")
            .and_where(("name", "Alice"))
            .and_where(col("age").gt(20))
            .select(&["id", "name"])
            .to_sql();

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ?"
        );
    }

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
            aggregate::expr("APPROX_COUNT_DISTINCT(user_id)").as_("approx_users"),
        ]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT COUNT(*) AS \"cnt\", APPROX_COUNT_DISTINCT(user_id) AS \"approx_users\" FROM \"employee\""
        );
    }

    #[test]
    fn test_union_all_to_sql() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
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
        let mut q1 = sqipe("employee");
        q1.select(&["dept"]);

        let mut q2 = sqipe("contractor");
        q2.select(&["dept"]);

        let uq = q1.union(&q2);

        let (sql, _) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT \"dept\" FROM \"employee\" UNION SELECT \"dept\" FROM \"contractor\""
        );
    }

    #[test]
    fn test_union_all_to_pipe_sql() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let uq = q1.union_all(&q2);

        let (sql, _) = uq.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"dept\" = ? |> SELECT \"id\", \"name\" |> UNION ALL FROM \"employee\" |> WHERE \"dept\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_union_all_with_order_by_and_limit() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
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
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut q3 = sqipe("contractor");
        q3.and_where(("dept", "eng"));
        q3.select(&["id", "name"]);

        let mut q4 = sqipe("contractor");
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
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);
        q1.order_by(col("name").asc());
        q1.limit(5);

        let mut q2 = sqipe("employee");
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
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
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

    #[test]
    fn test_in_clause() {
        let mut q = sqipe("employee");
        q.and_where(col("id").included(&[1, 2, 3]));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" IN (?, ?, ?)"
        );
    }

    #[test]
    fn test_not_in_clause() {
        let mut q = sqipe("employee");
        q.and_where(col("id").not_included(&[1, 2, 3]));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" NOT IN (?, ?, ?)"
        );
    }

    #[test]
    fn test_empty_in_clause() {
        let empty: &[i32] = &[];
        let mut q = sqipe("employee");
        q.and_where(col("id").included(empty));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE 1 = 0");
    }

    #[test]
    fn test_empty_not_in_clause() {
        let empty: &[i32] = &[];
        let mut q = sqipe("employee");
        q.and_where(col("id").not_included(empty));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" WHERE 1 = 1");
    }

    #[test]
    fn test_in_subquery() {
        let mut sub = sqipe("employee");
        sub.and_where(("dept", "eng"));
        sub.select(&["id"]);

        let mut q = sqipe("employee");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = ?)"
        );
    }

    #[test]
    fn test_not_in_subquery() {
        let mut sub = sqipe("employee");
        sub.and_where(("dept", "eng"));
        sub.select(&["id"]);

        let mut q = sqipe("employee");
        q.and_where(col("id").not_included(sub));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"id\" NOT IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = ?)"
        );
    }

    #[test]
    fn test_in_subquery_numbered_placeholder() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("employee");
        sub.and_where(("dept", "eng"));
        sub.select(&["id"]);

        let mut q = sqipe("employee");
        q.and_where(col("name").eq("Alice"));
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = $1 AND \"id\" IN (SELECT \"id\" FROM \"employee\" WHERE \"dept\" = $2)"
        );
    }

    #[test]
    fn test_between() {
        let mut q = sqipe("employee");
        q.and_where(col("age").between(20, 30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_not_between() {
        let mut q = sqipe("employee");
        q.and_where(col("age").not_between(20, 30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" NOT BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_between_with_in_range() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..=30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_range_exclusive_with_in_range() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" >= ? AND \"age\" < ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_range_from_with_in_range() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" >= ?");
        assert_eq!(binds, vec![Value::Int(20)]);
    }

    #[test]
    fn test_range_to_with_in_range() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(..30));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" < ?");
        assert_eq!(binds, vec![Value::Int(30)]);
    }

    #[test]
    fn test_range_to_inclusive_with_in_range() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(..=30));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" <= ?");
        assert_eq!(binds, vec![Value::Int(30)]);
    }

    #[test]
    fn test_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut q = sqipe("employee");
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
    fn test_pipe_sql_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_pipe_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"name\" = $1 AND \"age\" > $2 |> SELECT \"id\", \"name\""
        );
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(20),]
        );
    }

    #[test]
    fn test_join_standard() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_join_pipe() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_left_join() {
        let mut q = sqipe("users");
        q.left_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_join_with_table_alias() {
        let mut q = sqipe("users");
        q.join(
            table("orders").as_("o"),
            table("users").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" AS \"o\" ON \"users\".\"id\" = \"o\".\"user_id\""
        );
    }

    #[test]
    fn test_table_qualified_cols_select() {
        let u = table("users");
        let mut q = sqipe("users");
        q.join("orders", u.col("id").eq_col("user_id"));
        q.add_select(u.col("id"));
        q.add_select(u.col("name"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_select_cols_from_table() {
        let u = table("users");
        let mut q = sqipe("users");
        q.join("orders", u.col("id").eq_col("user_id"));
        q.select_cols(&u.cols(&["id", "name"]));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_join_with_using() {
        let mut q = sqipe("users");
        q.join("orders", join::using_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\")"
        );
    }

    #[test]
    fn test_join_with_using_multiple_columns() {
        let mut q = sqipe("users");
        q.join("orders", join::using_cols(&["user_id", "region"]));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\", \"region\")"
        );
    }

    #[test]
    fn test_join_with_multiple_conditions() {
        let mut q = sqipe("users");
        q.join(
            "orders",
            JoinCondition::And(vec![
                table("users").col("id").eq_col("user_id"),
                table("users").col("region").eq_col("region"),
            ]),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" AND \"users\".\"region\" = \"orders\".\"region\""
        );
    }

    #[test]
    fn test_join_with_qualified_col_on_right() {
        let mut q = sqipe("users");
        q.join(
            "orders",
            table("users")
                .col("id")
                .eq_col(table("orders").col("user_id")),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_cte_where_then_join() {
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "WITH \"_cte_0\" AS (SELECT * FROM \"users\" WHERE \"age\" > ?) SELECT \"id\", \"name\" FROM \"_cte_0\" AS \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_cte_where_then_join_pipe() {
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"age\" > ? |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_then_where_no_cte() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(col("age").gt(25));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"age\" > ?"
        );
    }

    #[test]
    fn test_col_alias() {
        let mut q = sqipe("employee");
        q.add_select(col("full_name").as_("name"));
        q.add_select(col("age"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"full_name\" AS \"name\", \"age\" FROM \"employee\""
        );
    }

    #[test]
    fn test_qualified_col_alias() {
        let u = table("users");
        let mut q = sqipe("users");
        q.add_select(u.col("full_name").as_("name"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"users\".\"full_name\" AS \"name\" FROM \"users\""
        );
    }

    #[test]
    fn test_from_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
        );
        assert_eq!(binds, vec![Value::String("completed".to_string())]);
    }

    #[test]
    fn test_from_subquery_pipe() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" |> SELECT "user_id""#
        );
    }

    #[test]
    fn test_from_subquery_with_outer_where() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.and_where(col("amount").gt(100));
        q.select(&["user_id"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" WHERE "amount" > ?"#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(100)]
        );
    }

    #[test]
    fn test_from_subquery_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.and_where(col("amount").gt(100));
        q.select(&["user_id"]);

        let (sql, binds) = q.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = $1) AS "t" WHERE "amount" > $2"#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(100)]
        );
    }

    #[test]
    fn test_from_subquery_with_join() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.join("users", table("t").col("user_id").eq_col("id"));
        q.select(&["user_id"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" INNER JOIN "users" ON "t"."user_id" = "users"."id""#
        );
    }

    #[test]
    fn test_from_subquery_cte_where_before_join() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.and_where(col("amount").gt(100));
        q.join("users", table("t").col("user_id").eq_col("id"));
        q.select(&["user_id"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" WHERE "amount" > ?) SELECT "user_id" FROM "_cte_0" AS "t" INNER JOIN "users" ON "t"."user_id" = "users"."id""#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(100)]
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

    #[test]
    fn test_table_alias() {
        let mut q = sqipe("employee");
        q.as_("e");
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"employee\" AS \"e\"");
    }

    // ── LIKE tests ──

    #[test]
    fn test_like_contains() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_starts_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::starts_with("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("Ali%".to_string())]);
    }

    #[test]
    fn test_like_ends_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::ends_with("ice")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%ice".to_string())]);
    }

    #[test]
    fn test_not_like_contains() {
        let mut q = sqipe("users");
        q.and_where(col("name").not_like(LikeExpression::contains("test")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" NOT LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%test%".to_string())]);
    }

    #[test]
    fn test_like_escape_special_chars() {
        let mut q = sqipe("products");
        q.and_where(col("name").like(LikeExpression::starts_with("a_b%")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("a\\_b\\%%".to_string())]);
    }

    #[test]
    fn test_like_contains_escape_char_itself() {
        let mut q = sqipe("products");
        q.and_where(col("name").like(LikeExpression::starts_with("a\\b")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("a\\\\b%".to_string())]);
    }

    #[test]
    fn test_like_all_special_chars_combined() {
        let mut q = sqipe("products");
        q.and_where(col("name").like(LikeExpression::starts_with("a_b%")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("a\\_b\\%%".to_string())]);
    }

    #[test]
    fn test_like_qualified_col() {
        let mut q = sqipe("users");
        q.as_("u");
        q.and_where(table("u").col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" AS "u" WHERE "u"."name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_pipe_sql() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "name" LIKE ? ESCAPE '\' |> SELECT "id", "name""#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_char() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "100%")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("%100!%%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_starts_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::starts_with_escaped_by('!', "a_b")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("a!_b%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_ends_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::ends_with_escaped_by('!', "x%y")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("%x!%y".to_string())]);
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_percent_as_escape() {
        LikeExpression::contains_escaped_by('%', "foo");
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_underscore_as_escape() {
        LikeExpression::starts_with_escaped_by('_', "foo");
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_single_quote_as_escape() {
        LikeExpression::ends_with_escaped_by('\'', "foo");
    }

    // ── join_subquery tests ──

    #[test]
    fn test_join_subquery_standard() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_left_join_subquery_standard() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);

        let mut q = sqipe("users");
        q.left_join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" LEFT JOIN (SELECT "user_id", "total" FROM "orders") AS "o" ON "users"."id" = "o"."user_id""#
        );
    }

    #[test]
    fn test_join_subquery_pipe_sql() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id" |> SELECT "id", "name""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_join_subquery_with_outer_where() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.and_where(col("age").gt(25));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id" WHERE "age" > ?"#
        );
        assert_eq!(
            binds,
            vec![Value::String("shipped".to_string()), Value::Int(25)]
        );
    }

    #[test]
    fn test_join_subquery_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > $1) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = $2) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(
            binds,
            vec![Value::Int(25), Value::String("shipped".to_string())]
        );
    }

    #[test]
    fn test_cte_where_then_join_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);

        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN (SELECT "user_id", "total" FROM "orders") AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_join_subquery_mixed_with_table_join() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join("profiles", table("users").col("id").eq_col("user_id"));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN "profiles" ON "users"."id" = "profiles"."user_id" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    // ── JoinCondition::Expr tests ──

    #[test]
    fn test_join_condition_expr_standard() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."text" LIKE "patterns"."pattern""#
        );
    }

    #[test]
    fn test_join_condition_expr_pipe() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "texts" |> INNER JOIN "patterns" ON "texts"."text" LIKE "patterns"."pattern" |> SELECT "id", "text""#
        );
    }

    #[test]
    fn test_for_update() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update();

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update();

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_with_option() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update_with("NOWAIT");

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE NOWAIT"#
        );
    }

    #[test]
    fn test_for_update_with_option_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update_with("SKIP LOCKED");

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE SKIP LOCKED"#
        );
    }

    #[test]
    fn test_for_with() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_with("NO KEY UPDATE");

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR NO KEY UPDATE"#
        );
    }

    #[test]
    fn test_for_with_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_with("SHARE");

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR SHARE"#
        );
    }

    #[test]
    fn test_for_update_with_order_by_and_limit() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.order_by(col("id").asc());
        q.limit(10);
        q.for_update();

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" ORDER BY "id" ASC LIMIT 10 FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_with_order_by_and_limit_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.order_by(col("id").asc());
        q.limit(10);
        q.for_update();

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> SELECT "id", "name" |> ORDER BY "id" ASC |> LIMIT 10 FOR UPDATE"#
        );
    }

    #[test]
    fn test_join_condition_expr_inside_and() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            JoinCondition::And(vec![
                table("texts").col("category").eq_col("category"),
                join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
            ]),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."category" = "patterns"."category" AND "texts"."text" LIKE "patterns"."pattern""#
        );
    }

    // ── UPDATE tests ──

    #[test]
    fn test_update_basic() {
        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
        u.set(col("status"), "inactive");
        u.allow_without_where();
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "status" = ?"#);
        assert_eq!(binds, vec![Value::String("inactive".to_string())]);
    }

    #[test]
    fn test_update_from_query_with_where() {
        let mut q = sqipe("employee");
        q.and_where(col("id").eq(1));
        let mut u = q.update();
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

        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
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
        let mut u = sqipe("employee").update();
        u.allow_without_where();
        let _ = u.to_sql();
    }

    #[test]
    #[should_panic(expected = "UPDATE without WHERE is dangerous")]
    fn test_update_no_where_panics() {
        let mut u = sqipe("employee").update();
        u.set(col("status"), "inactive");
        let _ = u.to_sql();
    }

    #[test]
    fn test_update_with_table_alias() {
        let mut q = sqipe("employee");
        q.as_("e");
        let mut u = q.update();
        u.set(col("name"), "Alice");
        u.and_where(col("id").eq(1));
        let (sql, _) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" "e" SET "name" = ? WHERE "id" = ?"#
        );
    }

    #[test]
    fn test_update_with_set_expr() {
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
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
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
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
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.set_expr(SetExpression::new(r#""updated_at" = NOW()"#));
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
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""version" = "version" + 1"#));
        u.allow_without_where();
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "version" = "version" + 1"#);
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_update_with_set_expr_bind_order() {
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
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

        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
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

    // ── DELETE tests ──

    #[test]
    fn test_delete_basic() {
        let mut d = sqipe("employee").delete();
        d.and_where(col("id").eq(1));
        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_delete_allow_without_where() {
        let mut d = sqipe("employee").delete();
        d.allow_without_where();
        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee""#);
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_delete_from_query_with_where() {
        let mut q = sqipe("employee");
        q.and_where(col("id").eq(1));
        let d = q.delete();
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

        let mut d = sqipe("employee").delete();
        d.and_where(col("id").eq(1));
        let (sql, binds) = d.to_sql_with(&PgDialect);
        assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = $1"#);
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_delete_with_complex_where() {
        let mut d = sqipe("employee").delete();
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
        let mut d = sqipe("employee").delete();
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
        let mut d = sqipe("employee").delete();
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
        let d = sqipe("employee").delete();
        let _ = d.to_sql();
    }

    #[test]
    fn test_delete_with_table_alias() {
        let mut q = sqipe("employee");
        q.as_("e");
        let mut d = q.delete();
        d.and_where(col("id").eq(1));
        let (sql, _) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
    }

    #[test]
    #[should_panic(expected = "JOINs which are not supported in DELETE")]
    fn test_delete_from_query_with_joins_panics() {
        let mut q = sqipe("employee");
        q.join("department", table("employee").col("dept_id").eq_col("id"));
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "aggregates which are not supported in DELETE")]
    fn test_delete_from_query_with_aggregates_panics() {
        let mut q = sqipe("employee");
        q.aggregate(&[aggregate::count_all()]);
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "ORDER BY which is not supported in DELETE")]
    fn test_delete_from_query_with_order_by_panics() {
        let mut q = sqipe("employee");
        q.order_by(OrderByClause {
            col: "id".to_string(),
            dir: SortDir::Asc,
        });
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "LIMIT which is not supported in DELETE")]
    fn test_delete_from_query_with_limit_panics() {
        let mut q = sqipe("employee");
        q.limit(10);
        let _ = q.delete();
    }
}
