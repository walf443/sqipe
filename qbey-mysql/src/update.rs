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
    order_bys: Vec<qbey::OrderByClause>,
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

    fn set_expr(&mut self, expr: qbey::RawSql) -> &mut Self {
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

    /// Build standard SQL with MySQL dialect.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let mut tree = self.inner.to_tree();
        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        let cfg = qbey::renderer::RenderConfig::from_dialect(&ph, &qi, &MySqlDialect);
        if let Some(order_by) = qbey::renderer::render_order_by(&self.order_bys, &cfg) {
            tree.tokens.push(qbey::tree::UpdateToken::Raw(order_by));
        }
        if let Some(n) = self.limit_val {
            tree.tokens
                .push(qbey::tree::UpdateToken::Raw(format!("LIMIT {}", n)));
        }
        qbey::renderer::update::render_update(&tree, &cfg)
    }
}
