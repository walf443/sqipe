#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

use qbey::Dialect;
use qbey::Value;
use qbey::renderer::standard::StandardSqlRenderer;
use qbey::renderer::{RenderConfig, Renderer};
use qbey::tree::SelectTree;

use qbey::DeleteQueryBuilder;
use qbey::InsertQueryBuilder;
use qbey::SelectQueryBuilder;
use qbey::UpdateQueryBuilder;

pub use qbey::MySqlDialect;

#[deprecated(note = "use MySqlDialect (re-exported from this crate) or qbey::MySqlDialect instead")]
pub type MySQL = qbey::MySqlDialect;

/// The type of index hint action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexHintType {
    /// `USE INDEX` – suggest indexes to the optimizer.
    Use,
    /// `FORCE INDEX` – force the optimizer to use specified indexes.
    Force,
    /// `IGNORE INDEX` – tell the optimizer to skip specified indexes.
    Ignore,
}

/// Optional `FOR` clause that restricts the scope of an index hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexHintScope {
    /// `FOR JOIN`
    Join,
    /// `FOR ORDER BY`
    OrderBy,
    /// `FOR GROUP BY`
    GroupBy,
}

/// A single MySQL index hint, e.g. `FORCE INDEX FOR JOIN (idx1, idx2)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHint {
    pub(crate) hint_type: IndexHintType,
    pub(crate) scope: Option<IndexHintScope>,
    pub(crate) indexes: Vec<String>,
}

impl IndexHint {
    fn to_sql_fragment(&self) -> String {
        let action = match self.hint_type {
            IndexHintType::Use => "USE INDEX",
            IndexHintType::Force => "FORCE INDEX",
            IndexHintType::Ignore => "IGNORE INDEX",
        };
        let scope = match &self.scope {
            None => "",
            Some(IndexHintScope::Join) => " FOR JOIN",
            Some(IndexHintScope::OrderBy) => " FOR ORDER BY",
            Some(IndexHintScope::GroupBy) => " FOR GROUP BY",
        };
        format!("{}{} ({})", action, scope, self.indexes.join(", "))
    }
}

/// MySQL-specific query builder wrapping the core SelectQuery.
///
/// Supports set operations (UNION, INTERSECT, EXCEPT) via `union()`, `union_all()`, etc.
/// When `set_operations` is non-empty, this query is a compound query.
#[derive(Clone)]
pub struct MysqlQuery<V: Clone + std::fmt::Debug = Value> {
    inner: qbey::SelectQuery<V>,
    index_hints: Vec<IndexHint>,
    set_operations: Vec<(qbey::SetOp, MysqlQuery<V>)>,
}

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
    inner: qbey::InsertQuery<V>,
    on_duplicate_key_updates: Vec<OnDuplicateKeyUpdateClause<V>>,
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

impl<V: Clone + std::fmt::Debug> qbey::IntoIncluded<V> for MysqlQuery<V> {
    fn into_in_clause(self, col: qbey::Col) -> qbey::WhereClause<V> {
        qbey::WhereClause::InSubQuery {
            col,
            sub: Box::new(self.into_tree()),
        }
    }

    fn into_not_in_clause(self, col: qbey::Col) -> qbey::WhereClause<V> {
        qbey::WhereClause::NotInSubQuery {
            col,
            sub: Box::new(self.into_tree()),
        }
    }
}

impl<V: Clone + std::fmt::Debug> qbey::IntoSelectTree<V> for MysqlQuery<V> {
    fn into_select_tree(self) -> qbey::tree::SelectTree<V> {
        self.into_tree()
    }
}

/// Create a MySQL-specific query builder for the given table.
///
/// Accepts a table name (`&str`) or a [`qbey::TableRef`] (created with [`qbey::table()`]).
pub fn qbey(table: impl qbey::IntoFromTable) -> MysqlQuery<Value> {
    MysqlQuery::wrap(qbey::qbey(table))
}

fn apply_index_hints_to<V: Clone>(tree: &mut SelectTree<V>, index_hints: &[IndexHint]) {
    use qbey::tree::SelectToken;
    if index_hints.is_empty() {
        return;
    }
    // Insert hints right after the From token
    if let Some(pos) = tree
        .tokens
        .iter()
        .position(|t| matches!(t, SelectToken::From(_)))
    {
        for (i, hint) in index_hints.iter().enumerate() {
            tree.tokens
                .insert(pos + 1 + i, SelectToken::Raw(hint.to_sql_fragment()));
        }
    }
}

/// Create a MySQL-specific query that selects from a subquery.
pub fn qbey_from_subquery(sub: impl qbey::IntoSelectTree<Value>, alias: &str) -> MysqlQuery<Value> {
    MysqlQuery::wrap(qbey::SelectQuery::from_subquery(sub, alias))
}

/// Create a MySQL-specific query that selects from a subquery with a custom value type.
pub fn qbey_from_subquery_with<V: Clone + std::fmt::Debug>(
    sub: impl qbey::IntoSelectTree<V>,
    alias: &str,
) -> MysqlQuery<V> {
    MysqlQuery::wrap(qbey::SelectQuery::from_subquery(sub, alias))
}

/// Create a MySQL-specific query builder with a custom value type.
///
/// Accepts a table name (`&str`) or a [`qbey::TableRef`] (created with [`qbey::table()`]).
pub fn qbey_with<V: Clone + std::fmt::Debug>(table: impl qbey::IntoFromTable) -> MysqlQuery<V> {
    MysqlQuery::wrap(qbey::qbey_with(table))
}

impl<V: Clone + std::fmt::Debug> SelectQueryBuilder<V> for MysqlQuery<V> {
    fn as_(&mut self, alias: &str) -> &mut Self {
        self.inner.as_(alias);
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

    fn select(&mut self, cols: &[impl Into<qbey::SelectItem> + Clone]) -> &mut Self {
        self.inner.select(cols);
        self
    }

    fn add_select(&mut self, item: impl Into<qbey::SelectItem>) -> &mut Self {
        self.inner.add_select(item);
        self
    }

    fn add_select_expr(&mut self, raw: qbey::RawSql, alias: Option<&str>) -> &mut Self {
        self.inner.add_select_expr(raw, alias);
        self
    }

    fn group_by(&mut self, cols: &[&str]) -> &mut Self {
        self.inner.group_by(cols);
        self
    }

    fn join(
        &mut self,
        table: impl qbey::IntoJoinTable,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.join(table, condition);
        self
    }

    fn left_join(
        &mut self,
        table: impl qbey::IntoJoinTable,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.left_join(table, condition);
        self
    }

    fn add_join(
        &mut self,
        join_type: qbey::JoinType,
        table: impl qbey::IntoJoinTable,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.add_join(join_type, table, condition);
        self
    }

    fn join_subquery(
        &mut self,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.join_subquery(sub, alias, condition);
        self
    }

    fn left_join_subquery(
        &mut self,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.left_join_subquery(sub, alias, condition);
        self
    }

    fn add_join_subquery(
        &mut self,
        join_type: qbey::JoinType,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner
            .add_join_subquery(join_type, sub, alias, condition);
        self
    }

    fn order_by(&mut self, clause: qbey::OrderByClause) -> &mut Self {
        self.inner.order_by(clause);
        self
    }

    fn order_by_expr(&mut self, raw: qbey::RawSql) -> &mut Self {
        self.inner.order_by_expr(raw);
        self
    }

    fn limit(&mut self, n: u64) -> &mut Self {
        self.inner.limit(n);
        self
    }

    fn offset(&mut self, n: u64) -> &mut Self {
        self.inner.offset(n);
        self
    }

    fn for_with(&mut self, clause: &str) -> &mut Self {
        self.inner.for_with(clause);
        self
    }
}

impl<V: Clone + std::fmt::Debug> MysqlQuery<V> {
    fn wrap(inner: qbey::SelectQuery<V>) -> Self {
        MysqlQuery {
            inner,
            index_hints: Vec::new(),
            set_operations: Vec::new(),
        }
    }

    /// Add a `FORCE INDEX (idx1, idx2, ...)` hint.
    pub fn force_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Force,
            scope: None,
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add a `USE INDEX (idx1, idx2, ...)` hint.
    pub fn use_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Use,
            scope: None,
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add an `IGNORE INDEX (idx1, idx2, ...)` hint.
    pub fn ignore_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Ignore,
            scope: None,
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add a `FORCE INDEX FOR {JOIN|ORDER BY|GROUP BY} (idx1, ...)` hint.
    pub fn force_index_for(&mut self, scope: IndexHintScope, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Force,
            scope: Some(scope),
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add a `USE INDEX FOR {JOIN|ORDER BY|GROUP BY} (idx1, ...)` hint.
    pub fn use_index_for(&mut self, scope: IndexHintScope, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Use,
            scope: Some(scope),
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Add an `IGNORE INDEX FOR {JOIN|ORDER BY|GROUP BY} (idx1, ...)` hint.
    pub fn ignore_index_for(&mut self, scope: IndexHintScope, indexes: &[&str]) -> &mut Self {
        self.index_hints.push(IndexHint {
            hint_type: IndexHintType::Ignore,
            scope: Some(scope),
            indexes: indexes.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    pub fn straight_join(
        &mut self,
        table: impl qbey::IntoJoinTable,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.add_join(
            qbey::JoinType::Custom("STRAIGHT_JOIN".to_string()),
            table,
            condition,
        );
        self
    }

    /// Add a STRAIGHT_JOIN with a subquery as the join target.
    pub fn straight_join_subquery(
        &mut self,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: qbey::JoinCondition,
    ) -> &mut Self {
        self.inner.add_join_subquery(
            qbey::JoinType::Custom("STRAIGHT_JOIN".to_string()),
            sub,
            alias,
            condition,
        );
        self
    }

    /// Returns the parts of this query for use in set operations.
    fn as_set_operation_parts(&self) -> Vec<(qbey::SetOp, MysqlQuery<V>)> {
        if self.set_operations.is_empty() {
            vec![(qbey::SetOp::Union, self.clone())] // SetOp is placeholder for the first part
        } else {
            self.set_operations.clone()
        }
    }

    fn combine(&self, op: qbey::SetOp, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        let mut parts = self.as_set_operation_parts();
        let other_parts = other.as_set_operation_parts();
        for (i, (other_op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((op.clone(), query));
            } else {
                parts.push((other_op, query));
            }
        }
        MysqlQuery {
            // inner is a dummy SelectQuery; for compound queries it only serves as a
            // container for union-level order_bys / limit / offset via Deref.
            inner: qbey::qbey_with(""),
            index_hints: Vec::new(),
            set_operations: parts,
        }
    }

    fn add_combine(&mut self, op: qbey::SetOp, other: &MysqlQuery<V>) {
        if self.set_operations.is_empty() {
            // Convert self into a compound query: move current state into
            // set_operations and reset self to an empty shell.
            let first = self.clone();
            *self = MysqlQuery::wrap(qbey::qbey_with(""));
            self.set_operations = vec![(qbey::SetOp::Union, first)];
        }
        let other_parts = other.as_set_operation_parts();
        for (i, (other_op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                self.set_operations.push((op.clone(), query));
            } else {
                self.set_operations.push((other_op, query));
            }
        }
    }

    pub fn union(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::Union, other)
    }

    pub fn union_all(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::UnionAll, other)
    }

    pub fn intersect(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::Intersect, other)
    }

    pub fn intersect_all(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::IntersectAll, other)
    }

    pub fn except(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::Except, other)
    }

    pub fn except_all(&self, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        self.combine(qbey::SetOp::ExceptAll, other)
    }

    pub fn add_union(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::Union, other);
        self
    }

    pub fn add_union_all(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::UnionAll, other);
        self
    }

    pub fn add_intersect(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::Intersect, other);
        self
    }

    pub fn add_intersect_all(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::IntersectAll, other);
        self
    }

    pub fn add_except(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::Except, other);
        self
    }

    pub fn add_except_all(&mut self, other: &MysqlQuery<V>) -> &mut Self {
        self.add_combine(qbey::SetOp::ExceptAll, other);
        self
    }

    /// Returns true if this query is a compound query (has set operations).
    pub fn has_set_operations(&self) -> bool {
        !self.set_operations.is_empty()
    }

    /// Returns the set operation parts for compound queries.
    pub fn set_operations(&self) -> &[(qbey::SetOp, MysqlQuery<V>)] {
        &self.set_operations
    }

    /// Build a SelectTree with MySQL-specific index hints applied.
    pub fn to_tree(&self) -> SelectTree<V> {
        let mut tree = self.inner.to_tree();
        self.apply_index_hints(&mut tree);
        tree
    }

    /// Convert into a SelectTree by moving fields, with MySQL-specific index hints applied.
    pub(crate) fn into_tree(self) -> SelectTree<V> {
        let mut tree = qbey::tree::SelectTree::from_query_owned(self.inner);
        apply_index_hints_to(&mut tree, &self.index_hints);
        tree
    }

    /// Build a SelectTree for a compound query.
    ///
    /// Each part's tree is built with MySQL index hints applied.
    /// The outer order_bys/limit/offset come from inner (set via Deref).
    fn to_compound_tree(&self) -> SelectTree<V> {
        use qbey::tree::SelectToken;
        let mut tokens = Vec::new();

        for (i, (op, mq)) in self.set_operations.iter().enumerate() {
            if i > 0 {
                tokens.push(SelectToken::SetOperator(op.clone()));
            }
            let sub = mq.to_tree();
            if sub.needs_parentheses() {
                tokens.push(SelectToken::OpenParen);
                tokens.push(SelectToken::SubSelect(Box::new(sub)));
                tokens.push(SelectToken::CloseParen);
            } else {
                tokens.push(SelectToken::SubSelect(Box::new(sub)));
            }
        }

        // Compound-level ORDER BY / LIMIT / OFFSET from inner
        if !self.inner.order_bys().is_empty() {
            tokens.push(SelectToken::OrderBy(self.inner.order_bys().to_vec()));
        }
        if let Some(n) = self.inner.limit_val() {
            tokens.push(SelectToken::Limit(n));
        }
        if let Some(n) = self.inner.offset_val() {
            tokens.push(SelectToken::Offset(n));
        }

        SelectTree { tokens }
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = if self.set_operations.is_empty() {
            self.to_tree()
        } else {
            self.to_compound_tree()
        };
        let ph = |n: usize| MySqlDialect.placeholder(n);
        let qi = |name: &str| MySqlDialect.quote_identifier(name);
        StandardSqlRenderer
            .render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, &MySqlDialect))
    }

    /// Convert this MySQL query builder into an UPDATE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_update(self) -> MysqlUpdateQuery<V> {
        MysqlUpdateQuery {
            inner: self.inner.into_update(),
            order_bys: Vec::new(),
            limit_val: None,
        }
    }

    /// Convert this MySQL query builder into a DELETE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_delete(self) -> MysqlDeleteQuery<V> {
        MysqlDeleteQuery {
            inner: self.inner.into_delete(),
            order_bys: Vec::new(),
            limit_val: None,
        }
    }

    /// Convert this MySQL query builder into an INSERT query builder.
    ///
    /// Consumes `self` and transfers the table name.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_insert(self) -> MysqlInsertQuery<V> {
        MysqlInsertQuery {
            inner: self.inner.into_insert(),
            on_duplicate_key_updates: Vec::new(),
        }
    }

    fn apply_index_hints(&self, tree: &mut SelectTree<V>) {
        apply_index_hints_to(tree, &self.index_hints);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qbey::{col, table};

    #[test]
    fn test_basic_to_sql() {
        let mut q = qbey("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT `id`, `name` FROM `employee` WHERE `name` = ?");
    }

    #[test]
    fn test_force_index() {
        let mut q = qbey("employee");
        q.force_index(&["idx_name"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_force_index_multiple() {
        let mut q = qbey("employee");
        q.force_index(&["idx_name", "idx_age"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name, idx_age) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_use_index() {
        let mut q = qbey("employee");
        q.use_index(&["idx_name"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` USE INDEX (idx_name) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_ignore_index() {
        let mut q = qbey("employee");
        q.ignore_index(&["idx_old"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` IGNORE INDEX (idx_old) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_force_index_for_join() {
        let mut q = qbey("employee");
        q.force_index_for(IndexHintScope::Join, &["idx_name"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` FORCE INDEX FOR JOIN (idx_name) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_use_index_for_order_by() {
        let mut q = qbey("employee");
        q.use_index_for(IndexHintScope::OrderBy, &["idx_name"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` USE INDEX FOR ORDER BY (idx_name) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_ignore_index_for_group_by() {
        let mut q = qbey("employee");
        q.ignore_index_for(IndexHintScope::GroupBy, &["idx_dept"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` IGNORE INDEX FOR GROUP BY (idx_dept) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_multiple_index_hints_combined() {
        let mut q = qbey("employee");
        q.use_index_for(IndexHintScope::Join, &["idx_a"]);
        q.use_index_for(IndexHintScope::OrderBy, &["idx_b"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` USE INDEX FOR JOIN (idx_a) USE INDEX FOR ORDER BY (idx_b) WHERE `name` = ?"
        );
    }

    #[test]
    fn test_delegates_core_methods() {
        let mut q = qbey("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));
        q.select(&["id", "name"]);
        q.order_by(col("name").asc());
        q.limit(10);
        q.offset(5);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `name` = ? AND `age` > ? ORDER BY `name` ASC LIMIT 10 OFFSET 5"
        );
    }

    #[test]
    fn test_union_all_with_force_index() {
        let mut q1 = qbey("employee");
        q1.force_index(&["idx_dept"]);
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = qbey("employee");
        q2.force_index(&["idx_dept"]);
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let uq = q1.union_all(&q2);

        let (sql, binds) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_dept) WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_dept) WHERE `dept` = ?"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("eng".to_string()),
                qbey::Value::String("sales".to_string()),
            ]
        );
    }

    #[test]
    fn test_union_with_order_by_and_limit() {
        let mut q1 = qbey("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = qbey("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut uq = q1.union_all(&q2);
        uq.order_by(col("name").asc());
        uq.limit(10);

        let (sql, _) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? ORDER BY `name` ASC LIMIT 10"
        );
    }

    #[test]
    fn test_union_with_add_union() {
        let mut q1 = qbey("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = qbey("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut q3 = qbey("contractor");
        q3.and_where(("dept", "eng"));
        q3.select(&["id", "name"]);

        let mut q4 = qbey("contractor");
        q4.and_where(("dept", "sales"));
        q4.select(&["id", "name"]);

        let uq2 = q3.union_all(&q4);
        let mut uq1 = q1.union_all(&q2);
        uq1.add_union_all(&uq2);

        let (sql, binds) = uq1.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ?"
        );
        assert_eq!(binds.len(), 4);
    }

    #[test]
    fn test_query_union_with_compound_query() {
        let mut q1 = qbey("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = qbey("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut q3 = qbey("contractor");
        q3.and_where(("dept", "eng"));
        q3.select(&["id", "name"]);

        let uq = q2.union_all(&q3);
        let result = q1.union_all(&uq);

        let (sql, binds) = result.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ?"
        );
        assert_eq!(binds.len(), 3);
    }

    #[test]
    fn test_straight_join() {
        let mut q = qbey("users");
        q.straight_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id`"
        );
    }

    #[test]
    fn test_in_subquery() {
        let mut sub = qbey("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = qbey("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `status` = ?)"
        );
        assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_in_subquery_with_force_index() {
        let mut sub = qbey("orders");
        sub.force_index(&["idx_status"]);
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = qbey("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` FORCE INDEX (idx_status) WHERE `status` = ?)"
        );
        assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_straight_join_with_alias() {
        let mut q = qbey("users");
        q.as_("u");
        q.straight_join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` AS `u` STRAIGHT_JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`"
        );
    }

    #[test]
    fn test_like_escape_backslash() {
        use qbey::LikeExpression;

        let mut q = qbey("users");
        q.and_where(col("name").like(LikeExpression::contains("test")));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT `id`, `name` FROM `users` WHERE `name` LIKE ? ESCAPE '\\'"#
        );
    }

    #[test]
    fn test_like_custom_escape_char() {
        use qbey::LikeExpression;

        let mut q = qbey("users");
        q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "test")));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT `id`, `name` FROM `users` WHERE `name` LIKE ? ESCAPE '!'"#
        );
    }

    #[test]
    fn test_not_like_escape_backslash() {
        use qbey::LikeExpression;

        let mut q = qbey("users");
        q.and_where(col("name").not_like(LikeExpression::contains("test")));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT `id`, `name` FROM `users` WHERE `name` NOT LIKE ? ESCAPE '\\'"#
        );
    }

    #[test]
    fn test_join_subquery() {
        let mut sub = qbey::qbey("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = qbey("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` INNER JOIN (SELECT `user_id`, `total` FROM `orders` WHERE `status` = ?) AS `o` ON `users`.`id` = `o`.`user_id`"
        );
        assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_update_basic() {
        let mut u = qbey("users").into_update();
        u.set(col("name"), "Alicia");
        u.and_where(col("id").eq(1));

        let (sql, binds) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("Alicia".to_string()),
                qbey::Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_multiple_sets() {
        let mut u = qbey("users").into_update();
        u.set(col("name"), "Alicia");
        u.set(col("age"), 31);
        u.and_where(col("id").eq(1));

        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            "UPDATE `users` SET `name` = ?, `age` = ? WHERE `id` = ?"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("Alicia".to_string()),
                qbey::Value::Int(31),
                qbey::Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_from_query_with_where() {
        let mut q = qbey("users");
        q.and_where(col("id").eq(1));
        let mut u = q.into_update();
        u.set(col("name"), "Alicia");

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
    }

    #[test]
    fn test_update_allow_without_where() {
        let mut u = qbey("users").into_update();
        u.set(col("age"), 99);
        u.allow_without_where();

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `age` = ?");
    }

    #[test]
    fn test_update_with_table_alias() {
        let mut q = qbey("users");
        q.as_("u");
        let mut u = q.into_update();
        u.set(col("name"), "Alicia");
        u.and_where(col("id").eq(1));

        let (sql, _) = u.to_sql();
        // MySQL does not support AS in UPDATE table alias
        assert_eq!(sql, "UPDATE `users` `u` SET `name` = ? WHERE `id` = ?");
    }

    #[test]
    fn test_update_with_order_by_and_limit() {
        let mut u = qbey("users").into_update();
        u.set(col("status"), "inactive");
        u.and_where(col("dept").eq("eng"));
        u.order_by(col("created_at").asc());
        u.limit(10);

        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            "UPDATE `users` SET `status` = ? WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("inactive".to_string()),
                qbey::Value::String("eng".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_limit_only() {
        let mut u = qbey("users").into_update();
        u.set(col("flagged"), true);
        u.allow_without_where();
        u.limit(100);

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `flagged` = ? LIMIT 100");
    }

    #[test]
    fn test_update_with_like() {
        let mut u = qbey("users").into_update();
        u.set(col("flagged"), true);
        u.and_where(col("name").like(qbey::LikeExpression::starts_with("test")));

        let (sql, binds) = u.to_sql();
        // MySQL doubles backslash in ESCAPE clause due to backslash_escape
        assert_eq!(
            sql,
            r"UPDATE `users` SET `flagged` = ? WHERE `name` LIKE ? ESCAPE '\\'"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::Bool(true),
                qbey::Value::String("test%".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_set_expr() {
        let mut u = qbey("users").into_update();
        u.set_expr(qbey::RawSql::new("`visit_count` = `visit_count` + 1"));
        u.and_where(col("id").eq(1));

        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            "UPDATE `users` SET `visit_count` = `visit_count` + 1 WHERE `id` = ?"
        );
        assert_eq!(binds, vec![qbey::Value::Int(1)]);
    }

    #[test]
    fn test_delete_basic() {
        let mut d = qbey("users").into_delete();
        d.and_where(col("id").eq(1));

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
        assert_eq!(binds, vec![qbey::Value::Int(1)]);
    }

    #[test]
    fn test_delete_from_query_with_where() {
        let mut q = qbey("users");
        q.and_where(col("id").eq(1));
        let d = q.into_delete();

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
    }

    #[test]
    fn test_delete_allow_without_where() {
        let mut d = qbey("users").into_delete();
        d.allow_without_where();

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users`");
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_delete_with_table_alias() {
        let mut q = qbey("users");
        q.as_("u");
        let mut d = q.into_delete();
        d.and_where(col("id").eq(1));

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` `u` WHERE `id` = ?");
    }

    #[test]
    fn test_delete_with_order_by_and_limit() {
        let mut d = qbey("users").into_delete();
        d.and_where(col("dept").eq("eng"));
        d.order_by(col("created_at").asc());
        d.limit(10);

        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM `users` WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10"
        );
        assert_eq!(binds, vec![qbey::Value::String("eng".to_string())]);
    }

    #[test]
    fn test_delete_with_limit_only() {
        let mut d = qbey("users").into_delete();
        d.allow_without_where();
        d.limit(100);

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` LIMIT 100");
    }

    #[test]
    fn test_delete_with_like() {
        let mut d = qbey("users").into_delete();
        d.and_where(col("name").like(qbey::LikeExpression::starts_with("test")));

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r"DELETE FROM `users` WHERE `name` LIKE ? ESCAPE '\\'");
        assert_eq!(binds, vec![qbey::Value::String("test%".to_string())]);
    }

    #[test]
    fn test_delete_with_or_where() {
        let mut d = qbey("users").into_delete();
        d.and_where(col("status").eq("pending"));
        d.or_where(col("status").eq("draft"));

        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM `users` WHERE `status` = ? OR `status` = ?"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("pending".to_string()),
                qbey::Value::String("draft".to_string()),
            ]
        );
    }

    #[test]
    fn test_order_by_expr() {
        let mut q = qbey("users");
        q.select(&["id", "name"]);
        q.order_by_expr(qbey::RawSql::new("RAND()"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT `id`, `name` FROM `users` ORDER BY RAND()");
    }

    #[test]
    fn test_order_by_expr_mixed_with_col() {
        let mut q = qbey("users");
        q.select(&["id", "name"]);
        q.order_by(col("name").asc());
        q.order_by_expr(qbey::RawSql::new("RAND()"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` ORDER BY `name` ASC, RAND()"
        );
    }

    #[test]
    fn test_update_order_by_expr() {
        let mut u = qbey("users").into_update();
        u.set(col("status"), "inactive");
        u.and_where(col("dept").eq("eng"));
        u.order_by_expr(qbey::RawSql::new("RAND()"));
        u.limit(10);

        let (sql, _) = u.to_sql();
        assert_eq!(
            sql,
            "UPDATE `users` SET `status` = ? WHERE `dept` = ? ORDER BY RAND() LIMIT 10"
        );
    }

    #[test]
    fn test_delete_order_by_expr() {
        let mut d = qbey("users").into_delete();
        d.and_where(col("dept").eq("eng"));
        d.order_by_expr(qbey::RawSql::new("RAND()"));
        d.limit(10);

        let (sql, _) = d.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM `users` WHERE `dept` = ? ORDER BY RAND() LIMIT 10"
        );
    }

    #[test]
    fn test_straight_join_subquery() {
        let mut sub = qbey::qbey("orders");
        sub.select(&["user_id", "total"]);

        let mut q = qbey("users");
        q.straight_join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN (SELECT `user_id`, `total` FROM `orders`) AS `o` ON `users`.`id` = `o`.`user_id`"
        );
    }

    #[test]
    fn test_intersect() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let q = q1.intersect(&q2);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` INTERSECT SELECT `dept` FROM `contractor`"
        );
    }

    #[test]
    fn test_intersect_all() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let q = q1.intersect_all(&q2);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` INTERSECT ALL SELECT `dept` FROM `contractor`"
        );
    }

    #[test]
    fn test_except() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let q = q1.except(&q2);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` EXCEPT SELECT `dept` FROM `contractor`"
        );
    }

    #[test]
    fn test_except_all() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let q = q1.except_all(&q2);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` EXCEPT ALL SELECT `dept` FROM `contractor`"
        );
    }

    #[test]
    fn test_intersect_with_order_by_and_limit() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let mut q = q1.intersect(&q2);
        q.order_by(col("dept").asc());
        q.limit(5);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` INTERSECT SELECT `dept` FROM `contractor` ORDER BY `dept` ASC LIMIT 5"
        );
    }

    #[test]
    fn test_except_with_order_by_and_limit() {
        let mut q1 = qbey("employee");
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.select(&["dept"]);

        let mut q = q1.except(&q2);
        q.order_by(col("dept").desc());
        q.limit(3);
        q.offset(1);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` EXCEPT SELECT `dept` FROM `contractor` ORDER BY `dept` DESC LIMIT 3 OFFSET 1"
        );
    }

    #[test]
    fn test_col_count() {
        let mut q = qbey("employee");
        q.select(&["dept"]);
        q.add_select(col("id").count().as_("cnt"));
        q.group_by(&["dept"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept`, COUNT(`id`) AS `cnt` FROM `employee` GROUP BY `dept`"
        );
    }

    #[test]
    fn test_col_count_with_table_qualified() {
        let mut q = qbey("employee");
        q.select(&["dept"]);
        q.add_select(table("employee").col("id").count().as_("cnt"));
        q.group_by(&["dept"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept`, COUNT(`employee`.`id`) AS `cnt` FROM `employee` GROUP BY `dept`"
        );
    }

    #[test]
    fn test_count_all() {
        let mut q = qbey("employee");
        q.select(&["dept"]);
        q.add_select(qbey::count_all().as_("cnt"));
        q.group_by(&["dept"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept`, COUNT(*) AS `cnt` FROM `employee` GROUP BY `dept`"
        );
    }

    #[test]
    fn test_count_one() {
        let mut q = qbey("employee");
        q.add_select(qbey::count_one().as_("cnt"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT COUNT(1) AS `cnt` FROM `employee`");
    }

    #[test]
    fn test_col_sum() {
        let mut q = qbey("orders");
        q.select(&["product"]);
        q.add_select(col("price").sum().as_("total"));
        q.group_by(&["product"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `product`, SUM(`price`) AS `total` FROM `orders` GROUP BY `product`"
        );
    }

    #[test]
    fn test_col_avg() {
        let mut q = qbey("orders");
        q.select(&["product"]);
        q.add_select(col("price").avg().as_("avg_price"));
        q.group_by(&["product"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `product`, AVG(`price`) AS `avg_price` FROM `orders` GROUP BY `product`"
        );
    }

    #[test]
    fn test_col_min() {
        let mut q = qbey("orders");
        q.select(&["product"]);
        q.add_select(col("price").min().as_("min_price"));
        q.group_by(&["product"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `product`, MIN(`price`) AS `min_price` FROM `orders` GROUP BY `product`"
        );
    }

    #[test]
    fn test_col_max() {
        let mut q = qbey("orders");
        q.select(&["product"]);
        q.add_select(col("price").max().as_("max_price"));
        q.group_by(&["product"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `product`, MAX(`price`) AS `max_price` FROM `orders` GROUP BY `product`"
        );
    }

    #[test]
    fn test_intersect_with_force_index() {
        let mut q1 = qbey("employee");
        q1.force_index(&["idx_dept"]);
        q1.select(&["dept"]);

        let mut q2 = qbey("contractor");
        q2.force_index(&["idx_dept"]);
        q2.select(&["dept"]);

        let q = q1.intersect(&q2);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `dept` FROM `employee` FORCE INDEX (idx_dept) INTERSECT SELECT `dept` FROM `contractor` FORCE INDEX (idx_dept)"
        );
    }

    #[test]
    fn test_insert_single_row() {
        let mut ins = qbey("employee").into_insert();
        ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
        let (sql, binds) = ins.to_sql();
        assert_eq!(sql, "INSERT INTO `employee` (`name`, `age`) VALUES (?, ?)");
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("Alice".to_string()),
                qbey::Value::Int(30)
            ]
        );
    }

    #[test]
    fn test_insert_multiple_rows() {
        let mut ins = qbey("employee").into_insert();
        ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
        ins.add_value(&[("name", "Bob".into()), ("age", 25.into())]);
        let (sql, binds) = ins.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO `employee` (`name`, `age`) VALUES (?, ?), (?, ?)"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::String("Alice".to_string()),
                qbey::Value::Int(30),
                qbey::Value::String("Bob".to_string()),
                qbey::Value::Int(25),
            ]
        );
    }

    #[test]
    fn test_insert_from_select() {
        let mut sub = qbey("old_employee");
        sub.select(&["name", "age"]);
        sub.and_where(col("active").eq(true));

        let mut ins = qbey("employee").into_insert();
        ins.from_select(sub);
        let (sql, binds) = ins.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO `employee` SELECT `name`, `age` FROM `old_employee` WHERE `active` = ?"
        );
        assert_eq!(binds, vec![qbey::Value::Bool(true)]);
    }

    #[test]
    fn test_insert_on_duplicate_key_update_with_value() {
        let mut ins = qbey("users").into_insert();
        ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
        ins.on_duplicate_key_update(col("name"), "Alice");
        let (sql, binds) = ins.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name` = ?"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::Int(1),
                qbey::Value::String("Alice".to_string()),
                qbey::Value::String("Alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_insert_on_duplicate_key_update_expr() {
        let mut ins = qbey("users").into_insert();
        ins.add_value(&[("id", 1.into()), ("age", 30.into())]);
        ins.on_duplicate_key_update_expr(qbey::RawSql::new("`age` = `age` + 1"));
        let (sql, binds) = ins.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO `users` (`id`, `age`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `age` = `age` + 1"
        );
        assert_eq!(binds, vec![qbey::Value::Int(1), qbey::Value::Int(30),]);
    }

    #[test]
    fn test_insert_on_duplicate_key_update_multiple() {
        let mut ins = qbey("users").into_insert();
        ins.add_value(&[
            ("id", 1.into()),
            ("name", "Alice".into()),
            ("age", 30.into()),
        ]);
        ins.on_duplicate_key_update_expr(qbey::RawSql::new("`age` = `age` + 1"));
        ins.on_duplicate_key_update(col("name"), "Alicia");
        let (sql, binds) = ins.to_sql();
        assert_eq!(
            sql,
            "INSERT INTO `users` (`id`, `name`, `age`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `age` = `age` + 1, `name` = ?"
        );
        assert_eq!(
            binds,
            vec![
                qbey::Value::Int(1),
                qbey::Value::String("Alice".to_string()),
                qbey::Value::Int(30),
                qbey::Value::String("Alicia".to_string()),
            ]
        );
    }

    #[test]
    fn test_insert_without_on_duplicate_key_update() {
        let mut ins = qbey("users").into_insert();
        ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
        let (sql, _) = ins.to_sql();
        assert_eq!(sql, "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)");
    }

    #[test]
    #[should_panic(expected = "duplicate column")]
    fn test_insert_on_duplicate_key_update_duplicate_column_panics() {
        let mut ins = qbey("users").into_insert();
        ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
        ins.on_duplicate_key_update(col("name"), "Alice");
        ins.on_duplicate_key_update(col("name"), "Bob");
    }
}
