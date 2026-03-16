use crate::Dialect;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};

use crate::renderer::RenderConfig;

/// Trait for DELETE query builder methods.
///
/// Implement this trait on dialect-specific DELETE wrappers to ensure they
/// expose the same builder API as the core [`DeleteQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait DeleteQueryBuilder<V: Clone> {
    /// Add an AND WHERE condition.
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an OR WHERE condition.
    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Explicitly allow this DELETE to have no WHERE clause.
    ///
    /// By default, `to_sql()` panics if no WHERE conditions are set,
    /// to prevent accidental full-table deletes.
    fn allow_without_where(&mut self) -> &mut Self;
}

#[derive(Debug, Clone)]
pub struct DeleteQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) allow_without_where: bool,
}

impl<V: Clone + std::fmt::Debug> DeleteQueryBuilder<V> for DeleteQuery<V> {
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    fn allow_without_where(&mut self) -> &mut Self {
        self.allow_without_where = true;
        self
    }
}

impl<V: Clone + std::fmt::Debug> DeleteQuery<V> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
    ) -> Self {
        DeleteQuery {
            table,
            table_alias,
            wheres,
            allow_without_where: false,
        }
    }

    /// Build a DeleteTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_tree(&self) -> crate::tree::DeleteTree<V> {
        self.assert_where_present();
        crate::tree::DeleteTree {
            table: self.table.clone(),
            table_alias: self.table_alias.clone(),
            wheres: self.wheres.clone(),
            order_bys: Vec::new(),
            limit: None,
        }
    }

    fn assert_where_present(&self) {
        assert!(
            self.allow_without_where || !self.wheres.is_empty(),
            "DELETE without WHERE is dangerous and not allowed by default. \
             Use .allow_without_where() to explicitly allow full-table deletes."
        );
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        crate::renderer::delete::render_delete(
            &tree,
            &RenderConfig::from_dialect(&ph, &qi, dialect),
        )
    }
}
