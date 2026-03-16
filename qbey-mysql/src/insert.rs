use qbey::Dialect;
use qbey::Value;
use qbey::{InsertQueryBuilder, MySqlDialect};

/// A clause in the ON DUPLICATE KEY UPDATE list.
#[derive(Debug, Clone)]
enum OnDuplicateKeyUpdateClause<V: Clone> {
    /// A column set to a bind value: `` `col` = ? ``.
    Value(String, V),
    /// A raw SQL expression: `` `col` = `col` + 1 ``.
    Expr(qbey::RawSql),
}

/// MySQL-specific INSERT query builder.
///
/// Wraps the core `InsertQuery` and renders SQL with MySQL dialect
/// (backtick quoting, `?` placeholders).
///
/// Supports `ON DUPLICATE KEY UPDATE` via
/// [`on_duplicate_key_update()`](MysqlInsertQuery::on_duplicate_key_update) and
/// [`on_duplicate_key_update_expr()`](MysqlInsertQuery::on_duplicate_key_update_expr).
#[derive(Debug, Clone)]
pub struct MysqlInsertQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) inner: qbey::InsertQuery<V>,
    on_duplicate_key_updates: Vec<OnDuplicateKeyUpdateClause<V>>,
}

impl<V: Clone + std::fmt::Debug> MysqlInsertQuery<V> {
    pub(crate) fn new(inner: qbey::InsertQuery<V>) -> Self {
        MysqlInsertQuery {
            inner,
            on_duplicate_key_updates: Vec::new(),
        }
    }
}

impl<V: Clone + std::fmt::Debug> InsertQueryBuilder<V> for MysqlInsertQuery<V> {
    fn add_value(&mut self, row: &(impl qbey::ToInsertRow<V> + ?Sized)) -> &mut Self {
        self.inner.add_value(row);
        self
    }

    fn add_col_value_expr(
        &mut self,
        column: impl Into<qbey::Col>,
        expr: qbey::RawSql,
    ) -> &mut Self {
        self.inner.add_col_value_expr(column, expr);
        self
    }

    fn from_select(&mut self, sub: impl qbey::IntoSelectTree<V>) -> &mut Self {
        self.inner.from_select(sub);
        self
    }
}

impl<V: Clone + std::fmt::Debug> MysqlInsertQuery<V> {
    /// Add an ON DUPLICATE KEY UPDATE clause with a bind value.
    ///
    /// ```
    /// use qbey::{col, Value};
    /// use qbey_mysql::qbey;
    /// use qbey::InsertQueryBuilder;
    ///
    /// let mut ins = qbey("users").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
    /// ins.on_duplicate_key_update(col("name"), "Alice");
    /// let (sql, binds) = ins.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = ?"
    /// );
    /// assert_eq!(
    ///     binds,
    ///     vec![Value::Int(1), Value::String("Alice".to_string()), Value::String("Alice".to_string())]
    /// );
    /// ```
    pub fn on_duplicate_key_update(&mut self, col: qbey::Col, val: impl Into<V>) -> &mut Self {
        assert!(
            !self.on_duplicate_key_updates.iter().any(|c| matches!(
                c,
                OnDuplicateKeyUpdateClause::Value(name, _) if name == &col.column
            )),
            "on_duplicate_key_update: duplicate column {:?}",
            col.column
        );
        self.on_duplicate_key_updates
            .push(OnDuplicateKeyUpdateClause::Value(col.column, val.into()));
        self
    }

    /// Add an ON DUPLICATE KEY UPDATE clause with a raw SQL expression.
    ///
    /// Use [`RawSql::new()`] to create the expression, making it explicit
    /// that raw SQL is being injected.
    ///
    /// ```
    /// use qbey::{col, Value, RawSql};
    /// use qbey_mysql::qbey;
    /// use qbey::InsertQueryBuilder;
    ///
    /// let mut ins = qbey("users").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("age", 30.into())]);
    /// ins.on_duplicate_key_update_expr(RawSql::new("`age` = `age` + 1"));
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     "INSERT INTO `users` (`id`, `age`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `age` = `age` + 1"
    /// );
    /// ```
    pub fn on_duplicate_key_update_expr(&mut self, expr: qbey::RawSql) -> &mut Self {
        self.on_duplicate_key_updates
            .push(OnDuplicateKeyUpdateClause::Expr(expr));
        self
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let mut tree = self.inner.to_tree();

        if !self.on_duplicate_key_updates.is_empty() {
            let sets: Vec<qbey::SetClause<V>> = self
                .on_duplicate_key_updates
                .iter()
                .map(|clause| match clause {
                    OnDuplicateKeyUpdateClause::Value(col, val) => {
                        qbey::SetClause::Value(col.clone(), val.clone())
                    }
                    OnDuplicateKeyUpdateClause::Expr(expr) => qbey::SetClause::Expr(expr.clone()),
                })
                .collect();
            tree.tokens
                .push(qbey::tree::InsertToken::KeywordAssignments {
                    keyword: "ON DUPLICATE KEY UPDATE".to_string(),
                    sets,
                });
        }

        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        let cfg = qbey::renderer::RenderConfig::from_dialect(&ph, &qi, &MySqlDialect);
        qbey::renderer::insert::render_insert(&tree, &cfg)
    }
}
