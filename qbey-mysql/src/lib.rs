#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

mod delete;
mod index_hint;
mod insert;
mod select;
mod update;

pub use delete::MysqlDeleteQuery;
pub use index_hint::{IndexHint, IndexHintScope, IndexHintType};
pub use insert::MysqlInsertQuery;
pub use qbey::MySqlDialect;
pub use select::MysqlQuery;
pub use update::MysqlUpdateQuery;

#[deprecated(note = "use MySqlDialect (re-exported from this crate) or qbey::MySqlDialect instead")]
pub type MySQL = qbey::MySqlDialect;

use qbey::Value;

/// Create a MySQL-specific query builder for the given table.
///
/// Accepts a table name (`&str`) or a [`qbey::TableRef`] (created with [`qbey::table()`]).
pub fn qbey(table: impl qbey::IntoFromTable) -> MysqlQuery<Value> {
    MysqlQuery::wrap(qbey::qbey(table))
}

/// Create a MySQL-specific query that selects from a subquery.
pub fn qbey_from_subquery(sub: impl qbey::IntoSelectTree<Value>, alias: &str) -> MysqlQuery<Value> {
    MysqlQuery::wrap(qbey::SelectQuery::from_subquery(sub, alias))
}

/// Create a MySQL-specific query that selects from a subquery with a custom value type.
pub fn qbey_from_subquery_with<V: Clone + std::fmt::Debug>(
    sub: impl qbey::IntoSelectTree<V>,
    alias: &str,
) -> MysqlQuery<V> {
    MysqlQuery::wrap(qbey::SelectQuery::from_subquery(sub, alias))
}

/// Create a MySQL-specific query builder with a custom value type.
///
/// Accepts a table name (`&str`) or a [`qbey::TableRef`] (created with [`qbey::table()`]).
pub fn qbey_with<V: Clone + std::fmt::Debug>(table: impl qbey::IntoFromTable) -> MysqlQuery<V> {
    MysqlQuery::wrap(qbey::qbey_with(table))
}

#[cfg(test)]
mod select_tests;

#[cfg(test)]
mod update_tests;

#[cfg(test)]
mod delete_tests;

#[cfg(test)]
mod insert_tests;
