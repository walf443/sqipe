#[doc = include_str!("../../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

pub mod column;
pub mod delete;
pub mod join;
pub mod like;
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

// Re-export all public types at the crate root.
pub use column::{Col, ColRef, OrderByClause, SelectItem, SortDir, TableRef, col, table};
pub use delete::DeleteQuery;
pub use join::{JoinClause, JoinCol, JoinCondition, JoinType};
pub use like::LikeExpression;
pub use query::{
    IntoFromTable, IntoJoinTable, IntoSelectTree, SelectQuery, SetOp, qbey, qbey_from_subquery,
    qbey_from_subquery_with, qbey_with,
};
pub use raw_sql::RawSql;
pub use update::{SetClause, UpdateQuery};
pub use value::{Op, Value};
pub use where_clause::{
    IntoIncluded, IntoRangeClause, IntoWhereClause, WhereClause, all, any, not,
};

// Crate-internal re-exports used by renderer and tree modules.
pub(crate) use where_clause::WhereEntry;
