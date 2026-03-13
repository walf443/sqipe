#[doc = include_str!("../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

use sqipe::Dialect;
use sqipe::Value;
use sqipe::renderer::pipe::PipeSqlRenderer;
use sqipe::renderer::standard::StandardSqlRenderer;
use sqipe::renderer::{RenderConfig, Renderer};
use sqipe::tree::SelectTree;
use std::ops::{Deref, DerefMut};

/// MySQL dialect: `?` placeholders and backtick identifier quoting.
pub struct MySQL;

impl sqipe::Dialect for MySQL {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn backslash_escape(&self) -> bool {
        true
    }
}

/// MySQL-specific query builder wrapping the core Query.
#[derive(Clone)]
pub struct MysqlQuery<V: Clone + std::fmt::Debug = Value> {
    inner: sqipe::Query<V>,
    force_indexes: Vec<String>,
    use_indexes: Vec<String>,
    ignore_indexes: Vec<String>,
}

/// A combined query built from UNION / UNION ALL on MysqlQuery.
pub struct MysqlUnionQuery<V: Clone + std::fmt::Debug = Value> {
    parts: Vec<(sqipe::SetOp, MysqlQuery<V>)>,
    order_bys: Vec<sqipe::OrderByClause>,
    limit_val: Option<u64>,
    offset_val: Option<u64>,
}

/// MySQL-specific UPDATE query builder.
///
/// Extends the core `UpdateQuery` with MySQL-specific features like
/// `ORDER BY` and `LIMIT` in UPDATE statements.
#[derive(Debug, Clone)]
pub struct MysqlUpdateQuery<V: Clone + std::fmt::Debug = Value> {
    inner: sqipe::UpdateQuery<V>,
    order_bys: Vec<sqipe::OrderByClause>,
    limit_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> MysqlUpdateQuery<V> {
    /// Add a SET clause: `` SET `col` = ? ``.
    ///
    /// Column names are quoted as identifiers but **not** parameterized,
    /// so never pass external (user-supplied) input as a column name.
    pub fn set(&mut self, col: sqipe::Col, val: impl Into<V>) -> &mut Self {
        self.inner.set(col, val);
        self
    }

    /// Add a raw SQL expression to the SET clause.
    ///
    /// Use [`sqipe::SetExpression::new()`] to create the expression, making it explicit
    /// that raw SQL is being injected.
    pub fn set_expr(&mut self, expr: sqipe::SetExpression) -> &mut Self {
        self.inner.set_expr(expr);
        self
    }

    /// Add an AND WHERE condition.
    pub fn and_where(&mut self, cond: impl sqipe::IntoWhereClause<V>) -> &mut Self {
        self.inner.and_where(cond);
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_where(&mut self, cond: impl sqipe::IntoWhereClause<V>) -> &mut Self {
        self.inner.or_where(cond);
        self
    }

    /// Explicitly allow UPDATE without WHERE clause.
    ///
    /// By default, calling [`to_sql()`](MysqlUpdateQuery::to_sql) without any WHERE
    /// conditions will panic. Call this method to opt in to full-table updates.
    pub fn without_where(&mut self) -> &mut Self {
        self.inner.without_where();
        self
    }

    /// Add an ORDER BY clause (MySQL extension).
    pub fn order_by(&mut self, clause: sqipe::OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
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
        tree.order_bys = self.order_bys.clone();
        tree.limit = self.limit_val;
        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySQL.quote_identifier(name);
        sqipe::renderer::update::render_update(
            &tree,
            &sqipe::renderer::RenderConfig::from_dialect(&ph, &qi, &MySQL),
        )
    }
}

/// MySQL-specific DELETE query builder.
///
/// Extends the core `DeleteQuery` with MySQL-specific features like
/// `ORDER BY` and `LIMIT` in DELETE statements.
#[derive(Debug, Clone)]
pub struct MysqlDeleteQuery<V: Clone + std::fmt::Debug = Value> {
    inner: sqipe::DeleteQuery<V>,
    order_bys: Vec<sqipe::OrderByClause>,
    limit_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> MysqlDeleteQuery<V> {
    /// Add an AND WHERE condition.
    pub fn and_where(&mut self, cond: impl sqipe::IntoWhereClause<V>) -> &mut Self {
        self.inner.and_where(cond);
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_where(&mut self, cond: impl sqipe::IntoWhereClause<V>) -> &mut Self {
        self.inner.or_where(cond);
        self
    }

    /// Explicitly allow DELETE without WHERE clause.
    ///
    /// By default, calling [`to_sql()`](MysqlDeleteQuery::to_sql) without any WHERE
    /// conditions will panic. Call this method to opt in to full-table deletes.
    pub fn without_where(&mut self) -> &mut Self {
        self.inner.without_where();
        self
    }

    /// Add an ORDER BY clause (MySQL extension).
    pub fn order_by(&mut self, clause: sqipe::OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
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
        tree.order_bys = self.order_bys.clone();
        tree.limit = self.limit_val;
        let ph = |_: usize| "?".to_string();
        let qi = |name: &str| MySQL.quote_identifier(name);
        sqipe::renderer::delete::render_delete(
            &tree,
            &sqipe::renderer::RenderConfig::from_dialect(&ph, &qi, &MySQL),
        )
    }
}

impl<V: Clone + std::fmt::Debug> Deref for MysqlQuery<V> {
    type Target = sqipe::Query<V>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<V: Clone + std::fmt::Debug> DerefMut for MysqlQuery<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<V: Clone + std::fmt::Debug> sqipe::IntoIncluded<V> for MysqlQuery<V> {
    fn into_in_clause(self, col: sqipe::ColRef) -> sqipe::WhereClause<V> {
        sqipe::WhereClause::InSubQuery {
            col,
            sub: Box::new(self.into_tree()),
        }
    }

    fn into_not_in_clause(self, col: sqipe::ColRef) -> sqipe::WhereClause<V> {
        sqipe::WhereClause::NotInSubQuery {
            col,
            sub: Box::new(self.into_tree()),
        }
    }
}

impl<V: Clone + std::fmt::Debug> sqipe::IntoSelectTree<V> for MysqlQuery<V> {
    fn into_select_tree(self) -> sqipe::tree::SelectTree<V> {
        self.into_tree()
    }
}

impl<V: Clone + std::fmt::Debug> sqipe::AsUnionParts for MysqlQuery<V> {
    type Query = MysqlQuery<V>;
    fn as_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery<V>)> {
        vec![(sqipe::SetOp::Union, self.clone())]
    }
}

impl<V: Clone + std::fmt::Debug> sqipe::AsUnionParts for MysqlUnionQuery<V> {
    type Query = MysqlQuery<V>;
    fn as_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery<V>)> {
        self.parts.clone()
    }
}

/// Create a MySQL-specific query builder for the given table.
pub fn sqipe(table: &str) -> MysqlQuery<Value> {
    MysqlQuery::wrap(sqipe::sqipe(table))
}

fn apply_index_hints_to<V: Clone>(
    tree: &mut SelectTree<V>,
    force_indexes: &[String],
    use_indexes: &[String],
    ignore_indexes: &[String],
) {
    if !force_indexes.is_empty() {
        tree.from
            .table_suffix
            .push(format!("FORCE INDEX ({})", force_indexes.join(", ")));
    }
    if !use_indexes.is_empty() {
        tree.from
            .table_suffix
            .push(format!("USE INDEX ({})", use_indexes.join(", ")));
    }
    if !ignore_indexes.is_empty() {
        tree.from
            .table_suffix
            .push(format!("IGNORE INDEX ({})", ignore_indexes.join(", ")));
    }
}

/// Create a MySQL-specific query that selects from a subquery.
pub fn sqipe_from_subquery(
    sub: impl sqipe::IntoSelectTree<Value>,
    alias: &str,
) -> MysqlQuery<Value> {
    MysqlQuery::wrap(sqipe::Query::from_subquery(sub, alias))
}

/// Create a MySQL-specific query that selects from a subquery with a custom value type.
pub fn sqipe_from_subquery_with<V: Clone + std::fmt::Debug>(
    sub: impl sqipe::IntoSelectTree<V>,
    alias: &str,
) -> MysqlQuery<V> {
    MysqlQuery::wrap(sqipe::Query::from_subquery(sub, alias))
}

/// Create a MySQL-specific query builder with a custom value type.
pub fn sqipe_with<V: Clone + std::fmt::Debug>(table: &str) -> MysqlQuery<V> {
    MysqlQuery::wrap(sqipe::sqipe_with(table))
}

impl<V: Clone + std::fmt::Debug> MysqlQuery<V> {
    fn wrap(inner: sqipe::Query<V>) -> Self {
        MysqlQuery {
            inner,
            force_indexes: Vec::new(),
            use_indexes: Vec::new(),
            ignore_indexes: Vec::new(),
        }
    }

    pub fn force_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.force_indexes = indexes.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn use_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.use_indexes = indexes.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn ignore_index(&mut self, indexes: &[&str]) -> &mut Self {
        self.ignore_indexes = indexes.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn straight_join(
        &mut self,
        table: impl sqipe::IntoJoinTable,
        condition: sqipe::JoinCondition,
    ) -> &mut Self {
        self.inner.add_join(
            sqipe::JoinType::Custom("STRAIGHT_JOIN".to_string()),
            table,
            condition,
        );
        self
    }

    /// Add a STRAIGHT_JOIN with a subquery as the join target.
    pub fn straight_join_subquery(
        &mut self,
        sub: impl sqipe::IntoSelectTree<V>,
        alias: &str,
        condition: sqipe::JoinCondition,
    ) -> &mut Self {
        self.inner.add_join_subquery(
            sqipe::JoinType::Custom("STRAIGHT_JOIN".to_string()),
            sub,
            alias,
            condition,
        );
        self
    }

    pub fn union<T: sqipe::AsUnionParts<Query = MysqlQuery<V>>>(
        &self,
        other: &T,
    ) -> MysqlUnionQuery<V> {
        let mut parts = vec![(sqipe::SetOp::Union, self.clone())];
        let other_parts = other.as_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((sqipe::SetOp::Union, query));
            } else {
                parts.push((op, query));
            }
        }
        MysqlUnionQuery {
            parts,
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
        }
    }

    pub fn union_all<T: sqipe::AsUnionParts<Query = MysqlQuery<V>>>(
        &self,
        other: &T,
    ) -> MysqlUnionQuery<V> {
        let mut parts = vec![(sqipe::SetOp::Union, self.clone())];
        let other_parts = other.as_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((sqipe::SetOp::UnionAll, query));
            } else {
                parts.push((op, query));
            }
        }
        MysqlUnionQuery {
            parts,
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
        }
    }

    /// Build a SelectTree with MySQL-specific index hints applied.
    pub fn to_tree(&self) -> SelectTree<V> {
        let mut tree = self.inner.to_tree();
        self.apply_index_hints(&mut tree);
        tree
    }

    /// Convert into a SelectTree by moving fields, with MySQL-specific index hints applied.
    pub(crate) fn into_tree(self) -> SelectTree<V> {
        let mut tree = sqipe::tree::SelectTree::from_query_owned(self.inner);
        apply_index_hints_to(
            &mut tree,
            &self.force_indexes,
            &self.use_indexes,
            &self.ignore_indexes,
        );
        tree
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        StandardSqlRenderer.render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, &MySQL))
    }

    /// Build pipe syntax SQL with MySQL dialect.
    pub fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        PipeSqlRenderer.render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, &MySQL))
    }

    /// Convert this MySQL query builder into an UPDATE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn update(self) -> MysqlUpdateQuery<V> {
        MysqlUpdateQuery {
            inner: self.inner.update(),
            order_bys: Vec::new(),
            limit_val: None,
        }
    }

    /// Convert this MySQL query builder into a DELETE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    /// The generated SQL uses MySQL dialect (backtick quoting, `?` placeholders).
    pub fn delete(self) -> MysqlDeleteQuery<V> {
        MysqlDeleteQuery {
            inner: self.inner.delete(),
            order_bys: Vec::new(),
            limit_val: None,
        }
    }

    fn apply_index_hints(&self, tree: &mut SelectTree<V>) {
        apply_index_hints_to(
            tree,
            &self.force_indexes,
            &self.use_indexes,
            &self.ignore_indexes,
        );
    }
}

impl<V: Clone + std::fmt::Debug> MysqlUnionQuery<V> {
    fn to_tree(&self) -> sqipe::tree::UnionTree<V> {
        let parts = self
            .parts
            .iter()
            .map(|(op, mq)| (op.clone(), mq.to_tree()))
            .collect();
        sqipe::tree::UnionTree {
            parts,
            order_bys: self.order_bys.clone(),
            limit: self.limit_val,
            offset: self.offset_val,
        }
    }
}

impl<V: Clone + std::fmt::Debug> sqipe::UnionQueryOps<V> for MysqlUnionQuery<V> {
    fn union<T: sqipe::AsUnionParts<Query = MysqlQuery<V>>>(&mut self, other: &T) -> &mut Self {
        let parts = other.as_union_parts();
        for (i, (op, query)) in parts.into_iter().enumerate() {
            if i == 0 {
                self.parts.push((sqipe::SetOp::Union, query));
            } else {
                self.parts.push((op, query));
            }
        }
        self
    }

    fn union_all<T: sqipe::AsUnionParts<Query = MysqlQuery<V>>>(&mut self, other: &T) -> &mut Self {
        let parts = other.as_union_parts();
        for (i, (op, query)) in parts.into_iter().enumerate() {
            if i == 0 {
                self.parts.push((sqipe::SetOp::UnionAll, query));
            } else {
                self.parts.push((op, query));
            }
        }
        self
    }

    fn order_by(&mut self, clause: sqipe::OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
        self
    }

    fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    fn offset(&mut self, n: u64) -> &mut Self {
        self.offset_val = Some(n);
        self
    }

    fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        StandardSqlRenderer.render_union(&tree, &RenderConfig::from_dialect(&ph, &qi, &MySQL))
    }

    fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        PipeSqlRenderer.render_union(&tree, &RenderConfig::from_dialect(&ph, &qi, &MySQL))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqipe::UnionQueryOps;
    use sqipe::{col, table};

    #[test]
    fn test_basic_to_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT `id`, `name` FROM `employee` WHERE `name` = ?");
    }

    #[test]
    fn test_force_index() {
        let mut q = sqipe("employee");
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
        let mut q = sqipe("employee");
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
        let mut q = sqipe("employee");
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
        let mut q = sqipe("employee");
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
    fn test_delegates_core_methods() {
        let mut q = sqipe("employee");
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
    fn test_force_index_pipe_sql() {
        let mut q = sqipe("employee");
        q.force_index(&["idx_name"]);
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM `employee` FORCE INDEX (idx_name) |> WHERE `name` = ? |> SELECT `id`, `name`"
        );
    }

    #[test]
    fn test_union_all_with_force_index() {
        let mut q1 = sqipe("employee");
        q1.force_index(&["idx_dept"]);
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
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
                sqipe::Value::String("eng".to_string()),
                sqipe::Value::String("sales".to_string()),
            ]
        );
    }

    #[test]
    fn test_union_with_order_by_and_limit() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
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
    fn test_union_pipe_sql_with_force_index() {
        let mut q1 = sqipe("employee");
        q1.force_index(&["idx_dept"]);
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.force_index(&["idx_dept"]);
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let uq = q1.union_all(&q2);

        let (sql, _) = uq.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM `employee` FORCE INDEX (idx_dept) |> WHERE `dept` = ? |> SELECT `id`, `name` |> UNION ALL FROM `employee` FORCE INDEX (idx_dept) |> WHERE `dept` = ? |> SELECT `id`, `name`"
        );
    }

    #[test]
    fn test_union_with_union_query() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut q3 = sqipe("contractor");
        q3.and_where(("dept", "eng"));
        q3.select(&["id", "name"]);

        let mut q4 = sqipe("contractor");
        q4.and_where(("dept", "sales"));
        q4.select(&["id", "name"]);

        let uq2 = q3.union_all(&q4);
        let mut uq1 = q1.union_all(&q2);
        uq1.union_all(&uq2);

        let (sql, binds) = uq1.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ?"
        );
        assert_eq!(binds.len(), 4);
    }

    #[test]
    fn test_query_union_with_union_query() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let mut q3 = sqipe("contractor");
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
        let mut q = sqipe("users");
        q.straight_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id`"
        );
    }

    #[test]
    fn test_straight_join_pipe() {
        let mut q = sqipe("users");
        q.straight_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM `users` |> STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id` |> SELECT `id`, `name`"
        );
    }

    #[test]
    fn test_in_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `status` = ?)"
        );
        assert_eq!(binds, vec![sqipe::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_in_subquery_with_force_index() {
        let mut sub = sqipe("orders");
        sub.force_index(&["idx_status"]);
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` FORCE INDEX (idx_status) WHERE `status` = ?)"
        );
        assert_eq!(binds, vec![sqipe::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_straight_join_with_alias() {
        let mut q = sqipe("users");
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
    fn test_cte_where_then_join() {
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "WITH `_cte_0` AS (SELECT * FROM `users` WHERE `age` > ?) SELECT `id`, `name` FROM `_cte_0` AS `users` INNER JOIN `orders` ON `users`.`id` = `orders`.`user_id`"
        );
        assert_eq!(binds, vec![sqipe::Value::Int(25)]);
    }

    #[test]
    fn test_cte_where_join_then_where() {
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(table("orders").col("total").gt(100));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "WITH `_cte_0` AS (SELECT * FROM `users` WHERE `age` > ?) SELECT `id`, `name` FROM `_cte_0` AS `users` INNER JOIN `orders` ON `users`.`id` = `orders`.`user_id` WHERE `orders`.`total` > ?"
        );
        assert_eq!(binds, vec![sqipe::Value::Int(25), sqipe::Value::Int(100)]);
    }

    #[test]
    fn test_like_escape_backslash() {
        use sqipe::LikeExpression;

        let mut q = sqipe("users");
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
        use sqipe::LikeExpression;

        let mut q = sqipe("users");
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
        use sqipe::LikeExpression;

        let mut q = sqipe("users");
        q.and_where(col("name").not_like(LikeExpression::contains("test")));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT `id`, `name` FROM `users` WHERE `name` NOT LIKE ? ESCAPE '\\'"#
        );
    }

    #[test]
    fn test_cte_pipe_sql_no_cte() {
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        // Pipe SQL should NOT generate CTE
        assert!(!sql.starts_with("WITH"));
        assert!(sql.contains("|> WHERE"));
        assert!(sql.contains("|> INNER JOIN"));
    }

    #[test]
    fn test_join_subquery() {
        let mut sub = sqipe::sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` INNER JOIN (SELECT `user_id`, `total` FROM `orders` WHERE `status` = ?) AS `o` ON `users`.`id` = `o`.`user_id`"
        );
        assert_eq!(binds, vec![sqipe::Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_update_basic() {
        let mut u = sqipe("users").update();
        u.set(col("name"), "Alicia");
        u.and_where(col("id").eq(1));

        let (sql, binds) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
        assert_eq!(
            binds,
            vec![
                sqipe::Value::String("Alicia".to_string()),
                sqipe::Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_multiple_sets() {
        let mut u = sqipe("users").update();
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
                sqipe::Value::String("Alicia".to_string()),
                sqipe::Value::Int(31),
                sqipe::Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_from_query_with_where() {
        let mut q = sqipe("users");
        q.and_where(col("id").eq(1));
        let mut u = q.update();
        u.set(col("name"), "Alicia");

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `name` = ? WHERE `id` = ?");
    }

    #[test]
    fn test_update_without_where() {
        let mut u = sqipe("users").update();
        u.set(col("age"), 99);
        u.without_where();

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `age` = ?");
    }

    #[test]
    fn test_update_with_table_alias() {
        let mut q = sqipe("users");
        q.as_("u");
        let mut u = q.update();
        u.set(col("name"), "Alicia");
        u.and_where(col("id").eq(1));

        let (sql, _) = u.to_sql();
        // MySQL does not support AS in UPDATE table alias
        assert_eq!(sql, "UPDATE `users` `u` SET `name` = ? WHERE `id` = ?");
    }

    #[test]
    fn test_update_with_order_by_and_limit() {
        let mut u = sqipe("users").update();
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
                sqipe::Value::String("inactive".to_string()),
                sqipe::Value::String("eng".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_limit_only() {
        let mut u = sqipe("users").update();
        u.set(col("flagged"), true);
        u.without_where();
        u.limit(100);

        let (sql, _) = u.to_sql();
        assert_eq!(sql, "UPDATE `users` SET `flagged` = ? LIMIT 100");
    }

    #[test]
    fn test_update_with_like() {
        let mut u = sqipe("users").update();
        u.set(col("flagged"), true);
        u.and_where(col("name").like(sqipe::LikeExpression::starts_with("test")));

        let (sql, binds) = u.to_sql();
        // MySQL doubles backslash in ESCAPE clause due to backslash_escape
        assert_eq!(
            sql,
            r"UPDATE `users` SET `flagged` = ? WHERE `name` LIKE ? ESCAPE '\\'"
        );
        assert_eq!(
            binds,
            vec![
                sqipe::Value::Bool(true),
                sqipe::Value::String("test%".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_set_expr() {
        let mut u = sqipe("users").update();
        u.set_expr(sqipe::SetExpression::new(
            "`visit_count` = `visit_count` + 1",
        ));
        u.and_where(col("id").eq(1));

        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            "UPDATE `users` SET `visit_count` = `visit_count` + 1 WHERE `id` = ?"
        );
        assert_eq!(binds, vec![sqipe::Value::Int(1)]);
    }

    #[test]
    fn test_delete_basic() {
        let mut d = sqipe("users").delete();
        d.and_where(col("id").eq(1));

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
        assert_eq!(binds, vec![sqipe::Value::Int(1)]);
    }

    #[test]
    fn test_delete_from_query_with_where() {
        let mut q = sqipe("users");
        q.and_where(col("id").eq(1));
        let d = q.delete();

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` WHERE `id` = ?");
    }

    #[test]
    fn test_delete_without_where() {
        let mut d = sqipe("users").delete();
        d.without_where();

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users`");
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_delete_with_table_alias() {
        let mut q = sqipe("users");
        q.as_("u");
        let mut d = q.delete();
        d.and_where(col("id").eq(1));

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` `u` WHERE `id` = ?");
    }

    #[test]
    fn test_delete_with_order_by_and_limit() {
        let mut d = sqipe("users").delete();
        d.and_where(col("dept").eq("eng"));
        d.order_by(col("created_at").asc());
        d.limit(10);

        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            "DELETE FROM `users` WHERE `dept` = ? ORDER BY `created_at` ASC LIMIT 10"
        );
        assert_eq!(binds, vec![sqipe::Value::String("eng".to_string())]);
    }

    #[test]
    fn test_delete_with_limit_only() {
        let mut d = sqipe("users").delete();
        d.without_where();
        d.limit(100);

        let (sql, _) = d.to_sql();
        assert_eq!(sql, "DELETE FROM `users` LIMIT 100");
    }

    #[test]
    fn test_delete_with_like() {
        let mut d = sqipe("users").delete();
        d.and_where(col("name").like(sqipe::LikeExpression::starts_with("test")));

        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r"DELETE FROM `users` WHERE `name` LIKE ? ESCAPE '\\'");
        assert_eq!(binds, vec![sqipe::Value::String("test%".to_string())]);
    }

    #[test]
    fn test_delete_with_or_where() {
        let mut d = sqipe("users").delete();
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
                sqipe::Value::String("pending".to_string()),
                sqipe::Value::String("draft".to_string()),
            ]
        );
    }

    #[test]
    fn test_straight_join_subquery() {
        let mut sub = sqipe::sqipe("orders");
        sub.select(&["user_id", "total"]);

        let mut q = sqipe("users");
        q.straight_join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN (SELECT `user_id`, `total` FROM `orders`) AS `o` ON `users`.`id` = `o`.`user_id`"
        );
    }
}
