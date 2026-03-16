use qbey::Dialect;
use qbey::Value;
use qbey::{DeleteQueryBuilder, MySqlDialect};

/// MySQL-specific DELETE query builder.
///
/// Extends the core `DeleteQuery` with MySQL-specific features like
/// `ORDER BY` and `LIMIT` in DELETE statements.
#[derive(Debug, Clone)]
pub struct MysqlDeleteQuery<V: Clone + std::fmt::Debug = Value> {
    inner: qbey::DeleteQuery<V>,
    order_bys: Vec<qbey::OrderByClause>,
    limit_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> MysqlDeleteQuery<V> {
    pub(crate) fn new(inner: qbey::DeleteQuery<V>) -> Self {
        MysqlDeleteQuery {
            inner,
            order_bys: Vec::new(),
            limit_val: None,
        }
    }
}

impl<V: Clone + std::fmt::Debug> DeleteQueryBuilder<V> for MysqlDeleteQuery<V> {
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

impl<V: Clone + std::fmt::Debug> MysqlDeleteQuery<V> {
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
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let mut tree = self.inner.to_tree();
        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        let cfg = qbey::renderer::RenderConfig::from_dialect(&ph, &qi, &MySqlDialect);
        if let Some(order_by) = qbey::renderer::render_order_by(&self.order_bys, &cfg) {
            tree.tokens.push(qbey::tree::DeleteToken::Raw(order_by));
        }
        if let Some(n) = self.limit_val {
            tree.tokens
                .push(qbey::tree::DeleteToken::Raw(format!("LIMIT {}", n)));
        }
        qbey::renderer::delete::render_delete(&tree, &cfg)
    }
}
