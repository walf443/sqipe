use std::ops::{Deref, DerefMut};
use sqipe::Dialect;

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
    table: String,
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

impl sqipe::IntoUnionParts for MysqlQuery {
    type Query = MysqlQuery;
    fn into_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery)> {
        vec![(sqipe::SetOp::Union, self.clone())]
    }
}

impl sqipe::IntoUnionParts for MysqlUnionQuery {
    type Query = MysqlQuery;
    fn into_union_parts(&self) -> Vec<(sqipe::SetOp, MysqlQuery)> {
        self.parts.clone()
    }
}

/// Create a MySQL-specific query builder for the given table.
pub fn sqipe(table: &str) -> MysqlQuery {
    MysqlQuery {
        inner: sqipe::sqipe(table),
        table: table.to_string(),
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

    pub fn union<T: sqipe::IntoUnionParts<Query = MysqlQuery>>(&self, other: &T) -> MysqlUnionQuery {
        let mut parts = vec![(sqipe::SetOp::Union, self.clone())];
        let other_parts = other.into_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((sqipe::SetOp::Union, query));
            } else {
                parts.push((op, query));
            }
        }
        MysqlUnionQuery { parts, order_bys: Vec::new(), limit_val: None, offset_val: None }
    }

    pub fn union_all<T: sqipe::IntoUnionParts<Query = MysqlQuery>>(&self, other: &T) -> MysqlUnionQuery {
        let mut parts = vec![(sqipe::SetOp::Union, self.clone())];
        let other_parts = other.into_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((sqipe::SetOp::UnionAll, query));
            } else {
                parts.push((op, query));
            }
        }
        MysqlUnionQuery { parts, order_bys: Vec::new(), limit_val: None, offset_val: None }
    }

    /// Build standard SQL with MySQL dialect.
    pub fn to_sql(&self) -> (String, Vec<sqipe::Value>) {
        let mut binds = Vec::new();
        let mut sql = self.build_body_with_hints(&mut binds);

        if let Some(order_by) = self.inner.to_order_by_with(&MySQL) {
            sql.push_str(&format!(" {}", order_by));
        }

        let (limit, offset) = self.inner.to_limit_offset();
        if let Some(l) = limit {
            sql.push_str(&format!(" {}", l));
        }
        if let Some(o) = offset {
            sql.push_str(&format!(" {}", o));
        }

        (sql, binds)
    }

    /// Build pipe syntax SQL with MySQL dialect.
    pub fn to_pipe_sql(&self) -> (String, Vec<sqipe::Value>) {
        let mut binds = Vec::new();
        let mut sql = self.build_pipe_body_with_hints(&mut binds);

        if let Some(order_by) = self.inner.to_order_by_with(&MySQL) {
            sql.push_str(&format!(" |> {}", order_by));
        }

        let (limit, offset) = self.inner.to_limit_offset();
        let mut limit_offset_parts = Vec::new();
        if let Some(l) = limit {
            limit_offset_parts.push(l);
        }
        if let Some(o) = offset {
            limit_offset_parts.push(o);
        }
        if !limit_offset_parts.is_empty() {
            sql.push_str(&format!(" |> {}", limit_offset_parts.join(" ")));
        }

        (sql, binds)
    }

    fn build_body_with_hints(&self, binds: &mut Vec<sqipe::Value>) -> String {
        let body = self.inner.to_sql_body_with(&MySQL, binds);
        self.inject_index_hints(body)
    }

    fn build_pipe_body_with_hints(&self, binds: &mut Vec<sqipe::Value>) -> String {
        let body = self.inner.to_pipe_sql_body_with(&MySQL, binds);
        self.inject_index_hints(body)
    }

    /// Build a full query for use as a UNION part with MySQL index hints.
    /// If the query has ORDER BY/LIMIT/OFFSET, wraps in parentheses.
    fn build_sql_union_part(&self, binds: &mut Vec<sqipe::Value>) -> String {
        let body = self.inner.to_sql_union_part_with(&MySQL, binds);
        self.inject_index_hints(body)
    }

    /// Build a full pipe query for use as a UNION part with MySQL index hints.
    /// If the query has ORDER BY/LIMIT/OFFSET, wraps in parentheses.
    fn build_pipe_sql_union_part(&self, binds: &mut Vec<sqipe::Value>) -> String {
        let body = self.inner.to_pipe_sql_union_part_with(&MySQL, binds);
        self.inject_index_hints(body)
    }

    fn inject_index_hints(&self, sql: String) -> String {
        let hints = self.build_index_hints();
        if hints.is_empty() {
            return sql;
        }
        let from_table = format!("FROM `{}`", self.table.replace('`', "``"));
        sql.replacen(&from_table, &format!("{} {}", from_table, hints), 1)
    }

    fn build_index_hints(&self) -> String {
        let mut parts = Vec::new();
        if !self.force_indexes.is_empty() {
            parts.push(format!("FORCE INDEX ({})", self.force_indexes.join(", ")));
        }
        if !self.use_indexes.is_empty() {
            parts.push(format!("USE INDEX ({})", self.use_indexes.join(", ")));
        }
        if !self.ignore_indexes.is_empty() {
            parts.push(format!("IGNORE INDEX ({})", self.ignore_indexes.join(", ")));
        }
        parts.join(" ")
    }
}

impl sqipe::UnionQueryOps for MysqlUnionQuery {
    fn union<T: sqipe::IntoUnionParts<Query = MysqlQuery>>(&mut self, other: &T) -> &mut Self {
        let parts = other.into_union_parts();
        for (i, (op, query)) in parts.into_iter().enumerate() {
            if i == 0 {
                self.parts.push((sqipe::SetOp::Union, query));
            } else {
                self.parts.push((op, query));
            }
        }
        self
    }

    fn union_all<T: sqipe::IntoUnionParts<Query = MysqlQuery>>(&mut self, other: &T) -> &mut Self {
        let parts = other.into_union_parts();
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
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, query)) in self.parts.iter().enumerate() {
            if i > 0 {
                let keyword = match op {
                    sqipe::SetOp::Union => "UNION",
                    sqipe::SetOp::UnionAll => "UNION ALL",
                };
                sql.push_str(&format!(" {} ", keyword));
            }
            sql.push_str(&query.build_sql_union_part(&mut binds));
        }

        self.append_order_limit(&mut sql);
        (sql, binds)
    }

    fn to_pipe_sql(&self) -> (String, Vec<sqipe::Value>) {
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, query)) in self.parts.iter().enumerate() {
            if i > 0 {
                let keyword = match op {
                    sqipe::SetOp::Union => "UNION",
                    sqipe::SetOp::UnionAll => "UNION ALL",
                };
                sql.push_str(&format!(" |> {} ", keyword));
            }
            sql.push_str(&query.build_pipe_sql_union_part(&mut binds));
        }

        self.append_pipe_order_limit(&mut sql);
        (sql, binds)
    }
}

impl MysqlUnionQuery {
    fn append_order_limit(&self, sql: &mut String) {
        if !self.order_bys.is_empty() {
            let clauses: Vec<String> = self
                .order_bys
                .iter()
                .map(|o| {
                    let dir = match o.dir {
                        sqipe::SortDir::Asc => "ASC",
                        sqipe::SortDir::Desc => "DESC",
                    };
                    format!("{} {}", MySQL.quote_identifier(&o.col), dir)
                })
                .collect();
            sql.push_str(&format!(" ORDER BY {}", clauses.join(", ")));
        }

        if let Some(limit) = self.limit_val {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = self.offset_val {
            sql.push_str(&format!(" OFFSET {}", offset));
        }
    }

    fn append_pipe_order_limit(&self, sql: &mut String) {
        if !self.order_bys.is_empty() {
            let clauses: Vec<String> = self
                .order_bys
                .iter()
                .map(|o| {
                    let dir = match o.dir {
                        sqipe::SortDir::Asc => "ASC",
                        sqipe::SortDir::Desc => "DESC",
                    };
                    format!("{} {}", MySQL.quote_identifier(&o.col), dir)
                })
                .collect();
            sql.push_str(&format!(" |> ORDER BY {}", clauses.join(", ")));
        }

        let mut limit_offset_parts = Vec::new();
        if let Some(limit) = self.limit_val {
            limit_offset_parts.push(format!("LIMIT {}", limit));
        }
        if let Some(offset) = self.offset_val {
            limit_offset_parts.push(format!("OFFSET {}", offset));
        }
        if !limit_offset_parts.is_empty() {
            sql.push_str(&format!(" |> {}", limit_offset_parts.join(" ")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqipe::col;
    use sqipe::UnionQueryOps;

    #[test]
    fn test_basic_to_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `employee` WHERE `name` = ?"
        );
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
