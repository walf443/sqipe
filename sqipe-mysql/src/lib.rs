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

struct MySQL;

impl sqipe::Dialect for MySQL {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
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
    MysqlQuery {
        inner: sqipe::sqipe(table),
        force_indexes: Vec::new(),
        use_indexes: Vec::new(),
        ignore_indexes: Vec::new(),
    }
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

/// Create a MySQL-specific query builder with a custom value type.
pub fn sqipe_with<V: Clone + std::fmt::Debug>(table: &str) -> MysqlQuery<V> {
    MysqlQuery {
        inner: sqipe::sqipe_with(table),
        force_indexes: Vec::new(),
        use_indexes: Vec::new(),
        ignore_indexes: Vec::new(),
    }
}

impl<V: Clone + std::fmt::Debug> MysqlQuery<V> {
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
        StandardSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    /// Build pipe syntax SQL with MySQL dialect.
    pub fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        PipeSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
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
        StandardSqlRenderer.render_union(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        PipeSqlRenderer.render_union(&tree, &RenderConfig { ph: &ph, qi: &qi })
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
}
