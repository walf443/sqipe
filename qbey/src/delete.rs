use std::marker::PhantomData;

use crate::Dialect;
use crate::query::CteDefinition;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};
use crate::{WhereNotSet, WhereProvided};

use crate::renderer::RenderConfig;

/// Trait for DELETE query builder methods that do not change the WHERE state.
///
/// Implement this trait on dialect-specific DELETE wrappers to ensure they
/// expose the same builder API as the core [`DeleteQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait DeleteQueryBuilder<V: Clone> {
    /// Add a CTE to the `WITH` clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut cte_q = qbey("users");
    /// cte_q.select(&["id"]);
    /// cte_q.and_where(col("age").gt(30));
    ///
    /// let mut d = qbey("users").into_delete();
    /// d.with_cte("old_users", &[], cte_q);
    /// let mut d = d.and_where(col("id").eq(1));
    /// let (sql, _) = d.to_sql();
    /// assert!(sql.starts_with(r#"WITH "old_users" AS"#));
    /// ```
    fn with_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;

    /// Add a recursive CTE to the `WITH RECURSIVE` clause.
    ///
    /// Note: per the SQL standard, the `RECURSIVE` keyword applies to the
    /// entire `WITH` block. If any CTE added via this method is recursive,
    /// the rendered SQL will use `WITH RECURSIVE` for all CTEs in the clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut base = qbey("categories");
    /// base.select(&["id"]);
    /// base.and_where(col("parent_id").eq(1));
    ///
    /// let mut recursive = qbey("categories");
    /// recursive.select(&["id"]);
    ///
    /// let cte_query = base.union_all(&recursive);
    ///
    /// let mut d = qbey("items").into_delete();
    /// d.with_recursive_cte("cat_tree", &["id"], cte_query);
    /// let mut d = d.and_where(col("category_id").eq(1));
    /// let (sql, _) = d.to_sql();
    /// assert!(sql.starts_with(r#"WITH RECURSIVE "cat_tree""#));
    /// ```
    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;
}

/// A DELETE query builder, generic over the bind value type `V` and WHERE state `W`.
///
/// Created via [`SelectQuery::into_delete()`] to convert a SELECT query builder into a DELETE statement.
///
/// By default, WHERE clause is required at compile time. The query starts in the
/// [`WhereNotSet`] state where `to_sql()` is not available. Call [`and_where()`],
/// [`or_where()`], or [`allow_without_where()`] to transition to [`WhereProvided`]
/// state, which enables `to_sql()` and `to_sql_with()`.
///
/// ```
/// use qbey::{qbey, col, ConditionExpr};
///
/// let mut d = qbey("employee").into_delete();
/// let mut d = d.and_where(col("id").eq(1));
/// let (sql, _) = d.to_sql();
/// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
/// ```
///
/// Attempting to call `to_sql()` without a WHERE clause is a compile error:
///
/// ```compile_fail
/// use qbey::qbey;
///
/// let d = qbey("employee").into_delete();
/// let _ = d.to_sql(); // Error: `to_sql` is not available on `WhereNotSet`
/// ```
#[derive(Debug, Clone)]
pub struct DeleteQuery<V: Clone + std::fmt::Debug = Value, W = WhereNotSet> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) ctes: Vec<CteDefinition<V>>,
    /// Columns to return via RETURNING clause (non-standard SQL).
    #[cfg(feature = "returning")]
    pub(crate) returning_columns: Vec<crate::Col>,
    pub(crate) _where_state: PhantomData<W>,
}

// ── Builder methods available in any WHERE state ──

impl<V: Clone + std::fmt::Debug, W> DeleteQueryBuilder<V> for DeleteQuery<V, W> {
    fn with_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self {
        debug_assert!(
            !self.ctes.iter().any(|c| c.name == name),
            "duplicate CTE name {:?}: each CTE must have a unique name",
            name,
        );
        self.ctes
            .push(CteDefinition::new(name, columns, query, false));
        self
    }

    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self {
        debug_assert!(
            !self.ctes.iter().any(|c| c.name == name),
            "duplicate CTE name {:?}: each CTE must have a unique name",
            name,
        );
        self.ctes
            .push(CteDefinition::new(name, columns, query, true));
        self
    }
}

// ── RETURNING (available in any WHERE state) ──

impl<V: Clone + std::fmt::Debug, W> DeleteQuery<V, W> {
    /// Add columns to the RETURNING clause (non-standard SQL; PostgreSQL, SQLite, MariaDB).
    ///
    /// Columns are accumulated — calling this method multiple times appends
    /// to the existing list rather than replacing it.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder};
    ///
    /// let mut d = qbey("employee").into_delete();
    /// let mut d = d.and_where(col("id").eq(1));
    /// d.returning(&[col("id"), col("name")]);
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ? RETURNING "id", "name""#);
    /// ```
    #[cfg(feature = "returning")]
    pub fn returning(&mut self, cols: &[crate::Col]) -> &mut Self {
        for col in cols {
            self.returning_columns.push(col.clone());
        }
        self
    }

    /// Change the WHERE-state type parameter.
    ///
    /// This is a low-level helper for dialect wrappers (e.g., `MysqlDeleteQuery`)
    /// that need to mirror state transitions on their inner `DeleteQuery`.
    /// Not intended for direct use by application code.
    #[doc(hidden)]
    pub fn change_state<W2>(self) -> DeleteQuery<V, W2> {
        DeleteQuery {
            table: self.table,
            table_alias: self.table_alias,
            wheres: self.wheres,
            ctes: self.ctes,
            #[cfg(feature = "returning")]
            returning_columns: self.returning_columns,
            _where_state: PhantomData,
        }
    }
}

// ── Constructor ──

impl<V: Clone + std::fmt::Debug> DeleteQuery<V, WhereNotSet> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
        ctes: Vec<CteDefinition<V>>,
    ) -> Self {
        DeleteQuery {
            table,
            table_alias,
            wheres,
            ctes,
            #[cfg(feature = "returning")]
            returning_columns: Vec::new(),
            _where_state: PhantomData,
        }
    }
}

// ── State-transitioning methods (WhereNotSet → WhereProvided) ──

impl<V: Clone + std::fmt::Debug> DeleteQuery<V, WhereNotSet> {
    /// Add an AND WHERE condition and transition to [`WhereProvided`] state.
    ///
    /// After this call, `to_sql()` becomes available.
    pub fn and_where(mut self, cond: impl IntoWhereClause<V>) -> DeleteQuery<V, WhereProvided> {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self.change_state()
    }

    /// Add an OR WHERE condition and transition to [`WhereProvided`] state.
    ///
    /// After this call, `to_sql()` becomes available.
    pub fn or_where(mut self, cond: impl IntoWhereClause<V>) -> DeleteQuery<V, WhereProvided> {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self.change_state()
    }

    /// Explicitly allow this DELETE to have no WHERE clause.
    ///
    /// By default, `to_sql()` is a compile error if no WHERE conditions are set,
    /// to prevent accidental full-table deletes. Call this method to acknowledge
    /// that a WHERE-less delete is intentional.
    pub fn allow_without_where(self) -> DeleteQuery<V, WhereProvided> {
        self.change_state()
    }

    /// Assert that WHERE conditions have already been set (e.g., transferred
    /// from a [`SelectQuery`](crate::SelectQuery)) and transition to
    /// [`WhereProvided`] state.
    ///
    /// Unlike [`allow_without_where()`](Self::allow_without_where), this method
    /// panics if no WHERE conditions are present, providing a runtime safety net.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
    ///
    /// let mut q = qbey("employee");
    /// q.and_where(col("id").eq(1));
    /// let d = q.into_delete().where_set();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set.
    pub fn where_set(self) -> DeleteQuery<V, WhereProvided> {
        assert!(
            !self.wheres.is_empty(),
            "where_set() called but no WHERE conditions are set. \
             Use allow_without_where() for intentional full-table deletes."
        );
        self.change_state()
    }
}

// ── Methods on WhereProvided (can add more conditions + build SQL) ──

impl<V: Clone + std::fmt::Debug> DeleteQuery<V, WhereProvided> {
    /// Add an additional AND WHERE condition.
    pub fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    /// Add an additional OR WHERE condition.
    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    /// Build a DeleteTree AST from this query.
    pub fn to_tree(&self) -> crate::tree::DeleteTree<V> {
        let mut tokens = Vec::new();
        if !self.ctes.is_empty() {
            tokens.push(crate::tree::DeleteToken::With(
                self.ctes.iter().map(|cte| cte.to_entry()).collect(),
            ));
        }
        tokens.push(crate::tree::DeleteToken::DeleteFrom {
            table: self.table.clone(),
            alias: self.table_alias.clone(),
        });
        if !self.wheres.is_empty() {
            tokens.push(crate::tree::DeleteToken::Where(self.wheres.clone()));
        }
        #[cfg(feature = "returning")]
        if !self.returning_columns.is_empty() {
            tokens.push(crate::tree::DeleteToken::Returning(
                self.returning_columns.clone(),
            ));
        }
        crate::tree::DeleteTree { tokens }
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
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
