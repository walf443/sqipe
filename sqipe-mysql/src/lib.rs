use sqipe::Dialect;
use sqipe::renderer::pipe::PipeSqlRenderer;
use sqipe::renderer::standard::StandardSqlRenderer;
use sqipe::renderer::{RenderConfig, Renderer};
use sqipe::tree::{SelectTree, UnionTree};
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
pub struct MysqlQuery {
    inner: sqipe::Query,
    force_indexes: Vec<String>,
    use_indexes: Vec<String>,
    ignore_indexes: Vec<String>,
}

/// A combined query built from UNION / UNION ALL on MysqlQuery.
pub struct MysqlUnionQuery {
    parts: Vec<(sqipe::SetOp, MysqlQuery)>,
    order_bys: Vec<sqipe::OrderByClause>,
    limit_val: Option<u64>,
    offset_val: Option<u64>,
}

impl Deref for MysqlQuery {
    type Target = sqipe::Query;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for MysqlQuery {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl sqipe::AsUnionParts for MysqlQuery {
    type Query = MysqlQuery;
    fn as_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery)> {
        vec![(sqipe::SetOp::Union, self.clone())]
    }
}

impl sqipe::AsUnionParts for MysqlUnionQuery {
    type Query = MysqlQuery;
    fn as_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery)> {
        self.parts.clone()
    }
}

/// Create a MySQL-specific query builder for the given table.
pub fn sqipe(table: &str) -> MysqlQuery {
    MysqlQuery {
        inner: sqipe::sqipe(table),
        force_indexes: Vec::new(),
        use_indexes: Vec::new(),
        ignore_indexes: Vec::new(),
    }
}

impl MysqlQuery {
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

    pub fn union<T: sqipe::AsUnionParts<Query = MysqlQuery>>(&self, other: &T) -> MysqlUnionQuery {
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

    pub fn union_all<T: sqipe::AsUnionParts<Query = MysqlQuery>>(
        &self,
        other: &T,
    ) -> MysqlUnionQuery {
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
    pub fn to_tree(&self) -> SelectTree {
        let mut tree = self.inner.to_tree();
        self.apply_index_hints(&mut tree);
        tree
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<sqipe::Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        StandardSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    /// Build pipe syntax SQL with MySQL dialect.
    pub fn to_pipe_sql(&self) -> (String, Vec<sqipe::Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        PipeSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    fn apply_index_hints(&self, tree: &mut SelectTree) {
        if !self.force_indexes.is_empty() {
            tree.from
                .table_suffix
                .push(format!("FORCE INDEX ({})", self.force_indexes.join(", ")));
        }
        if !self.use_indexes.is_empty() {
            tree.from
                .table_suffix
                .push(format!("USE INDEX ({})", self.use_indexes.join(", ")));
        }
        if !self.ignore_indexes.is_empty() {
            tree.from
                .table_suffix
                .push(format!("IGNORE INDEX ({})", self.ignore_indexes.join(", ")));
        }
    }
}

impl MysqlUnionQuery {
    fn to_tree(&self) -> UnionTree {
        let parts = self
            .parts
            .iter()
            .map(|(op, mq)| (op.clone(), mq.to_tree()))
            .collect();
        UnionTree {
            parts,
            order_bys: self.order_bys.clone(),
            limit: self.limit_val,
            offset: self.offset_val,
        }
    }
}

impl sqipe::UnionQueryOps for MysqlUnionQuery {
    fn union<T: sqipe::AsUnionParts<Query = MysqlQuery>>(&mut self, other: &T) -> &mut Self {
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

    fn union_all<T: sqipe::AsUnionParts<Query = MysqlQuery>>(&mut self, other: &T) -> &mut Self {
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

    fn to_sql(&self) -> (String, Vec<sqipe::Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| MySQL.placeholder(n);
        let qi = |name: &str| MySQL.quote_identifier(name);
        StandardSqlRenderer.render_union(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    fn to_pipe_sql(&self) -> (String, Vec<sqipe::Value>) {
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
    use sqipe::col;

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
}
