#[doc = include_str!("../../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

pub mod column;
pub mod delete;
pub mod insert;
pub mod join;
pub mod like;
pub mod prelude;
pub mod query;
pub mod raw_sql;
pub mod renderer;
pub mod tree;
pub mod update;
pub mod value;
pub mod where_clause;

/// Trait for SQL dialect placeholder and quoting styles.
pub trait Dialect {
    fn placeholder(&self, index: usize) -> String;

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// Whether backslashes must be doubled inside SQL string literals.
    /// MySQL requires this by default (when `NO_BACKSLASH_ESCAPES` is not set).
    fn backslash_escape(&self) -> bool {
        false
    }
}

/// Default dialect: `?` placeholders and double-quote identifier quoting.
///
/// Matches the defaults of the `Dialect` trait and works with SQLite out of the box.
/// Use `PgDialect` or `MySqlDialect` when targeting those databases.
pub struct DefaultDialect;

impl Dialect for DefaultDialect {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }
}

/// PostgreSQL dialect: `$1`, `$2`, … placeholders and double-quote identifier quoting.
pub struct PgDialect;

impl Dialect for PgDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }
}

/// MySQL dialect: `?` placeholders and backtick identifier quoting.
pub struct MySqlDialect;

impl Dialect for MySqlDialect {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn backslash_escape(&self) -> bool {
        true
    }
}

// Re-export all public types at the crate root.
pub use column::{
    Col, ColCondition, ColRef, ConditionExpr, OrderByClause, SelectFunc, SelectItem, SortDir,
    TableRef, WindowFunc, WindowSpec, col, count_all, count_one, dense_rank, rank, row_number,
    table, window,
};
pub use delete::{DeleteQuery, DeleteQueryBuilder};
pub use insert::{InsertQuery, InsertQueryBuilder, ToInsertRow};
pub use join::{JoinClause, JoinCondition, JoinType};
pub use like::LikeExpression;
pub use query::{
    CteDefinition, IntoFromTable, IntoJoinTable, IntoSelectTree, SelectQuery, SelectQueryBuilder,
    SetOp, qbey, qbey_from_subquery, qbey_from_subquery_with, qbey_with,
};
pub use raw_sql::RawSql;
pub use update::{SetClause, UpdateQuery, UpdateQueryBuilder};
pub use value::{Op, Value};
pub use where_clause::{
    IntoIncluded, IntoRangeClause, IntoWhereClause, WhereClause, WhereEntry, all, any, exists, not,
    not_exists,
};
