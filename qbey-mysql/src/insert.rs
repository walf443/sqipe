use qbey::Value;
use qbey::{InsertQueryBuilder, MySqlDialect};

/// A clause in the ON DUPLICATE KEY UPDATE list.
#[derive(Debug, Clone)]
enum OnDuplicateKeyUpdateClause<V: Clone> {
    /// A column set to a bind value: `` `col` = ? ``.
    Value(String, V),
    /// A raw SQL expression: `` `col` = `col` + 1 ``.
    Expr(qbey::RawSql<V>),
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
        expr: qbey::RawSql<V>,
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
    pub fn on_duplicate_key_update_expr(&mut self, expr: qbey::RawSql<V>) -> &mut Self {
        self.on_duplicate_key_updates
            .push(OnDuplicateKeyUpdateClause::Expr(expr));
        self
    }

    /// Add columns to the RETURNING clause (MariaDB 10.5+ extension).
    ///
    /// Not supported by MySQL. Requires MariaDB 10.5 or later.
    ///
    /// ```
    /// use qbey::{col, Value};
    /// use qbey_mysql::qbey;
    /// use qbey::InsertQueryBuilder;
    ///
    /// let mut ins = qbey("users").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
    /// ins.returning(&[col("id")]);
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) RETURNING `id`"
    /// );
    /// ```
    #[cfg(feature = "returning")]
    pub fn returning(&mut self, cols: &[qbey::Col]) -> &mut Self {
        self.inner.returning(cols);
        self
    }

    /// Build an InsertTree with MySQL-specific ODKU applied.
    pub fn to_tree(&self) -> qbey::tree::InsertTree<V> {
        self.clone().into_tree()
    }

    /// Consume this query and build an InsertTree by moving values.
    pub fn into_tree(self) -> qbey::tree::InsertTree<V> {
        let mut tree = self.inner.into_tree();

        if !self.on_duplicate_key_updates.is_empty() {
            let sets: Vec<qbey::SetClause<V>> = self
                .on_duplicate_key_updates
                .into_iter()
                .map(|clause| match clause {
                    OnDuplicateKeyUpdateClause::Value(col, val) => qbey::SetClause::Value(col, val),
                    OnDuplicateKeyUpdateClause::Expr(expr) => qbey::SetClause::Expr(expr),
                })
                .collect();

            // Insert ODKU before the RETURNING token (if present) so that
            // the final SQL order is:
            // INSERT INTO ... VALUES (...) ON DUPLICATE KEY UPDATE ... RETURNING ...
            let odku_token = qbey::tree::InsertToken::KeywordAssignments {
                keyword: "ON DUPLICATE KEY UPDATE".to_string(),
                sets,
            };

            #[cfg(feature = "returning")]
            let insert_pos = tree
                .tokens
                .iter()
                .position(|t| matches!(t, qbey::tree::InsertToken::Returning(_)))
                .unwrap_or(tree.tokens.len());
            #[cfg(not(feature = "returning"))]
            let insert_pos = tree.tokens.len();

            tree.tokens.insert(insert_pos, odku_token);
        }

        tree
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.clone().into_sql()
    }

    /// Consume this query and build standard SQL with MySQL dialect.
    /// More efficient than `to_sql()` as it avoids cloning the query into a tree.
    pub fn into_sql(self) -> (String, Vec<V>) {
        self.into_tree().into_sql_with(&MySqlDialect)
    }
}
