use std::marker::PhantomData;

use crate::Dialect;
use crate::column::Col;
use crate::query::CteDefinition;
use crate::raw_sql::RawSql;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};
use crate::{WhereNotSet, WhereProvided};

/// A single SET clause entry in an UPDATE statement.
#[derive(Debug, Clone)]
pub enum SetClause<V: Clone> {
    /// `"col" = ?` — identifier-quoted column with a bind value.
    Value(String, V),
    /// Raw SQL expression via [`RawSql`].
    Expr(RawSql<V>),
}

/// Trait for UPDATE query builder methods that do not change the WHERE state.
///
/// Implement this trait on dialect-specific UPDATE wrappers to ensure they
/// expose the same builder API as the core [`UpdateQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait UpdateQueryBuilder<V: Clone> {
    /// Add a SET clause: `SET "col" = ?`.
    ///
    /// Use [`col()`](crate::col) to create a column reference for the first argument.
    /// Column names are quoted as identifiers but **not** parameterized,
    /// so never pass external (user-supplied) input as a column name.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.set(col("name"), "Alice");
    /// let u = u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self;

    /// Add a raw SQL expression to the SET clause.
    ///
    /// Use [`RawSql::new()`] to create the expression, making it explicit
    /// that raw SQL is being injected.
    ///
    /// # Security
    ///
    /// The expression is embedded directly into the generated SQL **without
    /// escaping or parameterization**. Never pass user-supplied input;
    /// doing so opens the door to SQL injection.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, RawSql, UpdateQueryBuilder};
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    /// let u = u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#);
    /// ```
    fn set_expr(&mut self, expr: RawSql<V>) -> &mut Self;

    /// Add a CTE to the `WITH` clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut cte_q = qbey("departments");
    /// cte_q.select(&["id"]);
    /// cte_q.and_where(col("active").eq(true));
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.with_cte("active_depts", &[], cte_q);
    /// u.set(col("status"), "active");
    /// let u = u.and_where(col("dept_id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert!(sql.starts_with(r#"WITH "active_depts" AS"#));
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
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut base = qbey("employees");
    /// base.select(&["id", "name", "manager_id"]);
    /// base.and_where(col("manager_id").eq(0));
    ///
    /// let mut recursive = qbey("employees");
    /// recursive.select(&["id", "name", "manager_id"]);
    ///
    /// let cte_query = base.union_all(&recursive);
    ///
    /// let mut u = qbey("employees").into_update();
    /// u.with_recursive_cte("org_tree", &["id", "name", "manager_id"], cte_query);
    /// u.set(col("active"), true);
    /// let u = u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert!(sql.starts_with(r#"WITH RECURSIVE "org_tree""#));
    /// ```
    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;
}

/// An UPDATE query builder, generic over the bind value type `V` and WHERE state `W`.
///
/// Created via [`SelectQuery::into_update()`] to convert a SELECT query builder into an UPDATE statement.
///
/// By default, WHERE clause is required at compile time. The query starts in the
/// [`WhereNotSet`] state where `to_sql()` is not available. Call [`and_where()`],
/// [`or_where()`], or [`allow_without_where()`] to transition to [`WhereProvided`]
/// state, which enables `to_sql()` and `to_sql_with()`.
///
/// ```
/// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
///
/// let mut u = qbey("employee").into_update();
/// u.set(col("name"), "Alice");
/// let u = u.and_where(col("id").eq(1));
/// let (sql, _) = u.to_sql();
/// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
/// ```
///
/// Attempting to call `to_sql()` without a WHERE clause is a compile error:
///
/// ```compile_fail
/// use qbey::{qbey, col, UpdateQueryBuilder};
///
/// let mut u = qbey("employee").into_update();
/// u.set(col("name"), "Alice");
/// let _ = u.to_sql(); // Error: `to_sql` is not available on `WhereNotSet`
/// ```
#[derive(Debug, Clone)]
pub struct UpdateQuery<V: Clone + std::fmt::Debug = Value, W = WhereNotSet> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) sets: Vec<SetClause<V>>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) ctes: Vec<CteDefinition<V>>,
    /// Columns to return via RETURNING clause (non-standard SQL).
    #[cfg(feature = "returning")]
    pub(crate) returning_columns: Vec<crate::Col>,
    pub(crate) _where_state: PhantomData<W>,
}

// ── Builder methods available in any WHERE state ──

impl<V: Clone + std::fmt::Debug, W> UpdateQueryBuilder<V> for UpdateQuery<V, W> {
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self {
        self.sets.push(SetClause::Value(col.column, val.into()));
        self
    }

    fn set_expr(&mut self, expr: RawSql<V>) -> &mut Self {
        self.sets.push(SetClause::Expr(expr));
        self
    }

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

impl<V: Clone + std::fmt::Debug, W> UpdateQuery<V, W> {
    /// Add columns to the RETURNING clause (non-standard SQL; PostgreSQL, SQLite, MariaDB).
    ///
    /// Columns are accumulated — calling this method multiple times appends
    /// to the existing list rather than replacing it.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.set(col("name"), "Alice");
    /// let mut u = u.and_where(col("id").eq(1));
    /// u.returning(&[col("id"), col("name")]);
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ? RETURNING "id", "name""#);
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
    /// This is a low-level helper for dialect wrappers (e.g., `MysqlUpdateQuery`)
    /// that need to mirror state transitions on their inner `UpdateQuery`.
    /// Not intended for direct use by application code.
    #[doc(hidden)]
    pub fn change_state<W2>(self) -> UpdateQuery<V, W2> {
        UpdateQuery {
            table: self.table,
            table_alias: self.table_alias,
            sets: self.sets,
            wheres: self.wheres,
            ctes: self.ctes,
            #[cfg(feature = "returning")]
            returning_columns: self.returning_columns,
            _where_state: PhantomData,
        }
    }
}

// ── Constructor ──

impl<V: Clone + std::fmt::Debug> UpdateQuery<V, WhereNotSet> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
        ctes: Vec<CteDefinition<V>>,
    ) -> Self {
        UpdateQuery {
            table,
            table_alias,
            sets: Vec::new(),
            wheres,
            ctes,
            #[cfg(feature = "returning")]
            returning_columns: Vec::new(),
            _where_state: PhantomData,
        }
    }
}

// ── State-transitioning methods (WhereNotSet → WhereProvided) ──

impl<V: Clone + std::fmt::Debug> UpdateQuery<V, WhereNotSet> {
    /// Add an AND WHERE condition and transition to [`WhereProvided`] state.
    ///
    /// After this call, `to_sql()` becomes available.
    pub fn and_where(mut self, cond: impl IntoWhereClause<V>) -> UpdateQuery<V, WhereProvided> {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self.change_state()
    }

    /// Add an OR WHERE condition and transition to [`WhereProvided`] state.
    ///
    /// After this call, `to_sql()` becomes available.
    pub fn or_where(mut self, cond: impl IntoWhereClause<V>) -> UpdateQuery<V, WhereProvided> {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self.change_state()
    }

    /// Explicitly allow this UPDATE to have no WHERE clause.
    ///
    /// By default, `to_sql()` is a compile error if no WHERE conditions are set,
    /// to prevent accidental full-table updates. Call this method to acknowledge
    /// that a WHERE-less update is intentional.
    pub fn allow_without_where(self) -> UpdateQuery<V, WhereProvided> {
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
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder, UpdateQueryBuilder};
    ///
    /// let mut q = qbey("employee");
    /// q.and_where(col("id").eq(1));
    /// let mut u = q.into_update();
    /// u.set(col("name"), "Alice");
    /// let u = u.where_set();
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set.
    pub fn where_set(self) -> UpdateQuery<V, WhereProvided> {
        assert!(
            !self.wheres.is_empty(),
            "where_set() called but no WHERE conditions are set. \
             Use allow_without_where() for intentional full-table updates."
        );
        self.change_state()
    }
}

// ── Methods on WhereProvided (can add more conditions + build SQL) ──

impl<V: Clone + std::fmt::Debug> UpdateQuery<V, WhereProvided> {
    /// Add an additional AND WHERE condition.
    pub fn and_where(mut self, cond: impl IntoWhereClause<V>) -> Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    /// Add an additional OR WHERE condition.
    pub fn or_where(mut self, cond: impl IntoWhereClause<V>) -> Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    /// Build an UpdateTree AST from this query.
    pub fn to_tree(&self) -> crate::tree::UpdateTree<V> {
        self.clone().into_tree()
    }

    /// Consume this query and build an UpdateTree AST by moving values instead of cloning.
    pub fn into_tree(self) -> crate::tree::UpdateTree<V> {
        let mut tokens = Vec::new();
        if !self.ctes.is_empty() {
            tokens.push(crate::tree::UpdateToken::With(
                self.ctes.into_iter().map(|cte| cte.into_entry()).collect(),
            ));
        }
        tokens.push(crate::tree::UpdateToken::Update {
            table: self.table,
            alias: self.table_alias,
        });
        tokens.push(crate::tree::UpdateToken::Set(self.sets));
        if !self.wheres.is_empty() {
            tokens.push(crate::tree::UpdateToken::Where(self.wheres));
        }
        #[cfg(feature = "returning")]
        if !self.returning_columns.is_empty() {
            tokens.push(crate::tree::UpdateToken::Returning(self.returning_columns));
        }
        crate::tree::UpdateTree { tokens }
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        self.clone().into_sql_with(dialect)
    }

    /// Consume this query and build standard SQL with `?` placeholders.
    /// More efficient than `to_sql()` as it avoids cloning the query into a tree.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn into_sql(self) -> (String, Vec<V>) {
        self.into_sql_with(&crate::DefaultDialect)
    }

    /// Consume this query and build SQL with dialect-specific placeholders and quoting.
    /// More efficient than `to_sql_with()` as it avoids cloning the query into a tree.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn into_sql_with(self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        self.into_tree().into_sql_with(dialect)
    }
}
