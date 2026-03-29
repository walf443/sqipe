use qbey::Dialect;
use qbey::Value;
use qbey::{MySqlDialect, UpdateQueryBuilder, WhereNotSet, WhereProvided};

/// MySQL-specific UPDATE query builder.
///
/// Extends the core `UpdateQuery` with MySQL-specific features like
/// `ORDER BY` and `LIMIT` in UPDATE statements.
#[derive(Debug, Clone)]
pub struct MysqlUpdateQuery<V: Clone + std::fmt::Debug = Value, W = WhereNotSet> {
    inner: qbey::UpdateQuery<V, W>,
    order_bys: Vec<qbey::OrderByClause<qbey::Value>>,
    limit_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V, WhereNotSet> {
    pub(crate) fn new(inner: qbey::UpdateQuery<V, WhereNotSet>) -> Self {
        MysqlUpdateQuery {
            inner,
            order_bys: Vec::new(),
            limit_val: None,
        }
    }
}

impl<V: Clone + std::fmt::Debug, W> UpdateQueryBuilder<V> for MysqlUpdateQuery<V, W> {
    fn set(&mut self, col: qbey::Col, val: impl Into<V>) -> &mut Self {
        self.inner.set(col, val);
        self
    }

    fn set_expr(&mut self, expr: qbey::RawSql<V>) -> &mut Self {
        self.inner.set_expr(expr);
        self
    }

    fn with_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl qbey::IntoSelectTree<V>,
    ) -> &mut Self {
        self.inner.with_cte(name, columns, query);
        self
    }

    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl qbey::IntoSelectTree<V>,
    ) -> &mut Self {
        self.inner.with_recursive_cte(name, columns, query);
        self
    }
}

// ── Methods available in any WHERE state ──

impl<V: Clone + std::fmt::Debug, W> MysqlUpdateQuery<V, W> {
    /// Add an ORDER BY clause (MySQL extension).
    pub fn order_by(&mut self, clause: qbey::OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
        self
    }

    /// Add a raw SQL expression to the ORDER BY clause (MySQL extension).
    pub fn order_by_expr(&mut self, raw: qbey::RawSql) -> &mut Self {
        self.order_bys.push(qbey::OrderByClause::Expr(raw));
        self
    }

    /// Set the LIMIT value (MySQL extension).
    pub fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    /// RETURNING clause is not supported for UPDATE in MySQL/MariaDB.
    ///
    /// MariaDB supports RETURNING only for INSERT (10.5+) and DELETE (10.0+).
    ///
    /// # Panics
    ///
    /// Always panics. Use a separate SELECT query to retrieve updated rows.
    #[cfg(feature = "returning")]
    pub fn returning(&mut self, _cols: &[qbey::Col]) -> &mut Self {
        panic!(
            "RETURNING is not supported for UPDATE in MySQL/MariaDB. \
             Use a separate SELECT query to retrieve updated rows."
        );
    }
}

// ── State-transitioning methods (WhereNotSet → WhereProvided) ──

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V, WhereNotSet> {
    /// Add an AND WHERE condition and transition to [`WhereProvided`] state.
    pub fn and_where(
        self,
        cond: impl qbey::IntoWhereClause<V>,
    ) -> MysqlUpdateQuery<V, WhereProvided> {
        let inner = self.inner.and_where(cond);
        MysqlUpdateQuery {
            inner,
            order_bys: self.order_bys,
            limit_val: self.limit_val,
        }
    }

    /// Add an OR WHERE condition and transition to [`WhereProvided`] state.
    pub fn or_where(
        self,
        cond: impl qbey::IntoWhereClause<V>,
    ) -> MysqlUpdateQuery<V, WhereProvided> {
        let inner = self.inner.or_where(cond);
        MysqlUpdateQuery {
            inner,
            order_bys: self.order_bys,
            limit_val: self.limit_val,
        }
    }

    /// Explicitly allow this UPDATE to have no WHERE clause.
    pub fn allow_without_where(self) -> MysqlUpdateQuery<V, WhereProvided> {
        let inner = self.inner.allow_without_where();
        MysqlUpdateQuery {
            inner,
            order_bys: self.order_bys,
            limit_val: self.limit_val,
        }
    }

    /// Assert that WHERE conditions have already been set (e.g., transferred
    /// from a SelectQuery) and transition to [`WhereProvided`] state.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set.
    pub fn where_set(self) -> MysqlUpdateQuery<V, WhereProvided> {
        let inner = self.inner.where_set();
        MysqlUpdateQuery {
            inner,
            order_bys: self.order_bys,
            limit_val: self.limit_val,
        }
    }
}

// ── Methods on WhereProvided ──

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V, WhereProvided> {
    /// Add an additional AND WHERE condition.
    pub fn and_where(mut self, cond: impl qbey::IntoWhereClause<V>) -> Self {
        self.inner = self.inner.and_where(cond);
        self
    }

    /// Add an additional OR WHERE condition.
    pub fn or_where(mut self, cond: impl qbey::IntoWhereClause<V>) -> Self {
        self.inner = self.inner.or_where(cond);
        self
    }

    /// Build an UpdateTree with MySQL-specific ORDER BY and LIMIT applied.
    pub fn to_tree(&self) -> qbey::tree::UpdateTree<V> {
        self.clone().into_tree()
    }

    /// Consume this query and build an UpdateTree by moving values.
    pub fn into_tree(self) -> qbey::tree::UpdateTree<V> {
        let mut tree = self.inner.into_tree();
        let ph = |n: usize| MySqlDialect.placeholder(n);
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        let cfg = qbey::renderer::RenderConfig::from_dialect(&ph, &qi, &MySqlDialect);
        // ORDER BY is rendered separately and appended as Raw(String) because
        // UpdateToken has no OrderBy variant. The binds are collected separately
        // and appended after render_update. This is correct for MySQL's `?`
        // placeholders (position-independent) but would need a different approach
        // for PostgreSQL's `$N` indexed placeholders.
        let mut order_by_binds: Vec<&Value> = Vec::new();
        if let Some(order_by) =
            qbey::renderer::render_order_by(&self.order_bys, &cfg, &mut order_by_binds)
        {
            debug_assert!(
                order_by_binds.is_empty(),
                "RawSql binds in MySQL UPDATE ORDER BY are not supported with custom value types"
            );
            tree.tokens.push(qbey::tree::UpdateToken::Raw(order_by));
        }
        if let Some(n) = self.limit_val {
            tree.tokens
                .push(qbey::tree::UpdateToken::Raw(format!("LIMIT {}", n)));
        }
        tree
    }

    /// Build standard SQL with MySQL dialect.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.clone().into_sql()
    }

    /// Consume this query and build standard SQL with MySQL dialect.
    /// More efficient than `to_sql()` as it avoids cloning the query into a tree.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn into_sql(self) -> (String, Vec<V>) {
        self.into_tree().into_sql_with(&MySqlDialect)
    }
}
