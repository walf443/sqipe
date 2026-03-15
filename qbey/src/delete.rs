use crate::Dialect;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};

use crate::renderer::RenderConfig;
use crate::tree::default_quote_identifier;

#[derive(Debug, Clone)]
pub struct DeleteQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) allow_without_where: bool,
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

    /// Add an AND WHERE condition.
    pub fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    /// Explicitly allow this DELETE to have no WHERE clause.
    ///
    /// By default, `to_sql()` and `to_sql_with()` panic if no WHERE conditions are set,
    /// to prevent accidental full-table deletes. Call this method to opt in to WHERE-less deletes.
    ///
    /// ```
    /// use qbey::qbey;
    ///
    /// let mut d = qbey("employee").into_delete();
    /// d.allow_without_where();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee""#);
    /// ```
    pub fn allow_without_where(&mut self) -> &mut Self {
        self.allow_without_where = true;
        self
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
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        crate::renderer::delete::render_delete(&tree, &cfg)
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
