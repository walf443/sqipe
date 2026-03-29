use qbey::Value;
use qbey::tree::SelectTree;
use qbey::{MySqlDialect, SelectQueryBuilder};

use crate::delete::MysqlDeleteQuery;
use crate::index_hint::{IndexHint, IndexHintScope, IndexHintType, apply_index_hints_to};
use crate::insert::MysqlInsertQuery;
use crate::update::MysqlUpdateQuery;

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

impl<V: Clone + std::fmt::Debug> SelectQueryBuilder<V> for MysqlQuery<V> {
    fn as_(&mut self, alias: &str) -> &mut Self {
        self.inner.as_(alias);
        self
    }

    fn distinct(&mut self) -> &mut Self {
        debug_assert!(
            self.set_operations.is_empty(),
            "distinct() has no effect on compound queries (UNION/INTERSECT/EXCEPT); call it on individual sub-queries instead"
        );
        self.inner.distinct();
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

    fn and_having(&mut self, cond: impl qbey::IntoWhereClause<V>) -> &mut Self {
        self.inner.and_having(cond);
        self
    }

    fn or_having(&mut self, cond: impl qbey::IntoWhereClause<V>) -> &mut Self {
        self.inner.or_having(cond);
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

    fn add_select_expr(&mut self, raw: qbey::RawSql<V>, alias: Option<&str>) -> &mut Self {
        self.inner.add_select_expr(raw, alias);
        self
    }

    fn group_by(&mut self, cols: &[impl Into<qbey::Col> + Clone]) -> &mut Self {
        self.inner.group_by(cols);
        self
    }

    fn join(
        &mut self,
        table: impl qbey::IntoJoinTable,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner.join(table, condition);
        self
    }

    fn left_join(
        &mut self,
        table: impl qbey::IntoJoinTable,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner.left_join(table, condition);
        self
    }

    fn add_join(
        &mut self,
        join_type: qbey::JoinType,
        table: impl qbey::IntoJoinTable,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner.add_join(join_type, table, condition);
        self
    }

    fn join_subquery(
        &mut self,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner.join_subquery(sub, alias, condition);
        self
    }

    fn left_join_subquery(
        &mut self,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner.left_join_subquery(sub, alias, condition);
        self
    }

    fn add_join_subquery(
        &mut self,
        join_type: qbey::JoinType,
        sub: impl qbey::IntoSelectTree<V>,
        alias: &str,
        condition: impl Into<qbey::JoinCondition>,
    ) -> &mut Self {
        self.inner
            .add_join_subquery(join_type, sub, alias, condition);
        self
    }

    fn order_by(&mut self, clause: qbey::OrderByClause) -> &mut Self {
        self.inner.order_by(clause);
        self
    }

    fn order_by_expr(&mut self, raw: qbey::RawSql<V>) -> &mut Self {
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

    fn for_with(&mut self, clause: &str) -> &mut Self {
        self.inner.for_with(clause);
        self
    }
}

impl<V: Clone + std::fmt::Debug> MysqlQuery<V> {
    pub(crate) fn wrap(inner: qbey::SelectQuery<V>) -> Self {
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
        condition: impl Into<qbey::JoinCondition>,
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
        condition: impl Into<qbey::JoinCondition>,
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
            let mut clone = self.clone();
            clone.inner.take_ctes(); // CTEs belong to the outer query, not individual parts
            vec![(qbey::SetOp::Union, clone)] // SetOp is placeholder for the first part
        } else {
            self.set_operations.clone()
        }
    }

    fn combine(&self, op: qbey::SetOp, other: &MysqlQuery<V>) -> MysqlQuery<V> {
        debug_assert!(
            !other.inner.has_ctes(),
            "CTEs on the right-hand side of a set operation are not supported; define CTEs on the outer query instead"
        );
        let mut parts = self.as_set_operation_parts();
        let other_parts = other.as_set_operation_parts();
        for (i, (other_op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((op.clone(), query));
            } else {
                parts.push((other_op, query));
            }
        }
        let mut inner: qbey::SelectQuery<V> = qbey::qbey_with("");
        inner.clone_ctes_from(&self.inner);
        MysqlQuery {
            // inner is a dummy SelectQuery; for compound queries it only serves as a
            // container for union-level order_bys / limit / offset via Deref.
            inner,
            index_hints: Vec::new(),
            set_operations: parts,
        }
    }

    fn add_combine(&mut self, op: qbey::SetOp, other: &MysqlQuery<V>) {
        debug_assert!(
            !other.inner.has_ctes(),
            "CTEs on the right-hand side of a set operation are not supported; define CTEs on the outer query instead"
        );
        if self.set_operations.is_empty() {
            // Convert self into a compound query: move current state into
            // set_operations and reset self to an empty shell.
            let ctes = self.inner.take_ctes();
            let mut first = self.clone();
            first.inner.take_ctes();
            *self = MysqlQuery::wrap(qbey::qbey_with(""));
            self.inner.set_ctes(ctes);
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
        self.clone().into_tree()
    }

    /// Consume this query and build a SelectTree by moving fields, with MySQL-specific index hints applied.
    pub fn into_tree(self) -> SelectTree<V> {
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

        // Emit WITH clause for compound queries
        if self.inner.has_ctes() {
            tokens.push(SelectToken::With(self.inner.ctes_to_entries()));
        }

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
        self.clone().into_sql()
    }

    /// Consume this query and build standard SQL with MySQL dialect.
    /// More efficient than `to_sql()` as it avoids cloning the query into a tree.
    /// Note: compound queries (UNION/INTERSECT/EXCEPT) still clone internally
    /// as `to_compound_tree()` requires borrowing the set operation parts.
    pub fn into_sql(self) -> (String, Vec<V>) {
        let tree = if self.set_operations.is_empty() {
            self.into_tree()
        } else {
            self.to_compound_tree()
        };
        tree.into_sql_with(&MySqlDialect)
    }

    /// Convert this MySQL query builder into an UPDATE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_update(self) -> MysqlUpdateQuery<V, qbey::WhereNotSet> {
        MysqlUpdateQuery::new(self.inner.into_update())
    }

    /// Convert this MySQL query builder into a DELETE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_delete(self) -> MysqlDeleteQuery<V, qbey::WhereNotSet> {
        MysqlDeleteQuery::new(self.inner.into_delete())
    }

    /// Convert this MySQL query builder into an INSERT query builder.
    ///
    /// Consumes `self` and transfers the table name.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn into_insert(self) -> MysqlInsertQuery<V> {
        MysqlInsertQuery::new(self.inner.into_insert())
    }
}
