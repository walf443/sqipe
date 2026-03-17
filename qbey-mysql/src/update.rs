use qbey::Dialect;
use qbey::Value;
use qbey::{MySqlDialect, UpdateQueryBuilder};

/// MySQL-specific UPDATE query builder.
///
/// Extends the core `UpdateQuery` with MySQL-specific features like
/// `ORDER BY` and `LIMIT` in UPDATE statements.
#[derive(Debug, Clone)]
pub struct MysqlUpdateQuery<V: Clone + std::fmt::Debug = Value> {
    inner: qbey::UpdateQuery<V>,
    order_bys: Vec<qbey::OrderByClause<V>>,
    limit_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V> {
    pub(crate) fn new(inner: qbey::UpdateQuery<V>) -> Self {
        MysqlUpdateQuery {
            inner,
            order_bys: Vec::new(),
            limit_val: None,
        }
    }
}

impl<V: Clone + std::fmt::Debug> UpdateQueryBuilder<V> for MysqlUpdateQuery<V> {
    fn set(&mut self, col: qbey::Col, val: impl Into<V>) -> &mut Self {
        self.inner.set(col, val);
        self
    }

    fn set_expr(&mut self, expr: qbey::RawSql<V>) -> &mut Self {
        self.inner.set_expr(expr);
        self
    }

    fn and_where(&mut self, cond: impl qbey::IntoWhereClause<V>) -> &mut Self {
        self.inner.and_where(cond);
        self
    }

    fn or_where(&mut self, cond: impl qbey::IntoWhereClause<V>) -> &mut Self {
        self.inner.or_where(cond);
        self
    }

    fn allow_without_where(&mut self) -> &mut Self {
        self.inner.allow_without_where();
        self
    }
}

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V> {
    /// Add an ORDER BY clause (MySQL extension).
    pub fn order_by(&mut self, clause: qbey::OrderByClause<V>) -> &mut Self {
        self.order_bys.push(clause);
        self
    }

    /// Add a raw SQL expression to the ORDER BY clause (MySQL extension).
    pub fn order_by_expr(&mut self, raw: qbey::RawSql<V>) -> &mut Self {
        self.order_bys.push(qbey::OrderByClause::Expr(raw));
        self
    }

    /// Set the LIMIT value (MySQL extension).
    pub fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    /// Build standard SQL with MySQL dialect.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.inner.to_tree();
        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        let cfg = qbey::renderer::RenderConfig::from_dialect(&ph, &qi, &MySqlDialect);
        let (mut sql, mut binds) = qbey::renderer::update::render_update(&tree, &cfg);
        // Render ORDER BY / LIMIT after the main tree so that binds.len()
        // reflects the correct placeholder index. This works with both
        // MySQL's `?` and PostgreSQL's `$N` style placeholders.
        if let Some(order_by) =
            qbey::renderer::render_order_by(&self.order_bys, &cfg, &mut binds)
        {
            sql = format!("{} {}", sql, order_by);
        }
        if let Some(n) = self.limit_val {
            sql = format!("{} LIMIT {}", sql, n);
        }
        (sql, binds)
    }
}
