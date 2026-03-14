use crate::aggregate::AggregateExpr;
use crate::column::OrderByClause;
use crate::column::{Col, SelectItem, TableRef};
use crate::delete::DeleteQuery;
use crate::join::{JoinClause, JoinCondition, JoinType};
use crate::update::UpdateQuery;
use crate::value::Value;
use crate::where_clause::{IntoIncluded, IntoWhereClause, WhereClause, WhereEntry};

use crate::renderer::pipe::PipeSqlRenderer;
use crate::renderer::standard::StandardSqlRenderer;
use crate::renderer::{RenderConfig, Renderer};
use crate::tree::{SelectTree, UnionTree, default_quote_identifier};

use crate::Dialect;

#[derive(Debug, Clone)]
pub enum SetOp {
    Union,
    UnionAll,
}

/// Trait for types that can be used as a source in union operations.
pub trait AsUnionParts {
    type Query: Clone;
    fn as_union_parts(&self) -> Vec<(SetOp, Self::Query)>;
}

/// Common interface for union query builders.
pub trait UnionQueryOps<V: Clone + std::fmt::Debug = Value>: AsUnionParts {
    fn union<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn union_all<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn order_by(&mut self, clause: OrderByClause) -> &mut Self;
    fn order_by_expr(&mut self, raw: crate::RawSql) -> &mut Self {
        self.order_by(OrderByClause::Expr(raw))
    }
    fn limit(&mut self, n: u64) -> &mut Self;
    fn offset(&mut self, n: u64) -> &mut Self;
    fn to_sql(&self) -> (String, Vec<V>);
    fn to_pipe_sql(&self) -> (String, Vec<V>);
}

/// Trait for types that can be converted into a `SelectTree` for use as a FROM subquery.
///
/// Implement this trait to allow your custom query type (e.g., `MysqlQuery`)
/// to be passed to `sqipe_from_subquery_with()`.
pub trait IntoSelectTree<V: Clone> {
    /// Consume this query and produce a `SelectTree` AST node.
    fn into_select_tree(self) -> crate::tree::SelectTree<V>;
}

/// Trait for types that can specify a FROM table source.
pub trait IntoFromTable {
    fn into_from_table(self) -> (String, Option<String>);
}

impl IntoFromTable for &str {
    fn into_from_table(self) -> (String, Option<String>) {
        (self.to_string(), None)
    }
}

impl IntoFromTable for TableRef {
    fn into_from_table(self) -> (String, Option<String>) {
        (self.name, self.alias)
    }
}

/// Trait for types that can specify a join target table.
pub trait IntoJoinTable {
    fn into_join_table(self) -> (String, Option<String>);
}

impl<T: IntoFromTable> IntoJoinTable for T {
    fn into_join_table(self) -> (String, Option<String>) {
        self.into_from_table()
    }
}

/// The query builder, generic over the bind value type `V`.
#[derive(Debug, Clone)]
pub struct Query<V: Clone + std::fmt::Debug = Value> {
    /// Table name for table-based queries. Empty when using `from_subquery`.
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    /// When set, the query selects from this subquery instead of `table`.
    pub(crate) from_subquery: Option<Box<crate::tree::SelectTree<V>>>,
    pub(crate) selects: Vec<SelectItem>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) group_bys: Vec<String>,
    pub(crate) joins: Vec<JoinClause>,
    /// Subquery sources for joins, aligned with `joins` by index.
    pub(crate) join_subqueries: Vec<Option<Box<crate::tree::SelectTree<V>>>>,
    pub(crate) order_bys: Vec<OrderByClause>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
    /// Records the order of WHERE and JOIN operations for CTE generation.
    pub(crate) stage_order: Vec<crate::tree::StageRef>,
    /// Row-level locking clause (e.g., `"UPDATE"` → `FOR UPDATE`).
    pub(crate) lock_for: Option<String>,
}

/// A combined query built from UNION / UNION ALL operations.
#[derive(Debug, Clone)]
pub struct UnionQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) parts: Vec<(SetOp, Query<V>)>,
    pub(crate) order_bys: Vec<OrderByClause>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
}

impl<V: Clone + std::fmt::Debug> AsUnionParts for Query<V> {
    type Query = Query<V>;
    fn as_union_parts(&self) -> Vec<(SetOp, Query<V>)> {
        vec![(SetOp::Union, self.clone())] // SetOp is placeholder, caller overrides
    }
}

impl<V: Clone + std::fmt::Debug> AsUnionParts for UnionQuery<V> {
    type Query = Query<V>;
    fn as_union_parts(&self) -> Vec<(SetOp, Query<V>)> {
        self.parts.clone()
    }
}

/// Create a new query builder for the given table.
///
/// Accepts a table name (`&str`) or a [`TableRef`] (created with [`table()`]):
///
/// ```
/// use sqipe::{sqipe, table};
///
/// // Simple table name
/// let q = sqipe("users");
/// let (sql, _) = q.to_sql();
/// assert_eq!(sql, r#"SELECT * FROM "users""#);
///
/// // TableRef with alias
/// let q = sqipe(table("users").as_("u"));
/// let (sql, _) = q.to_sql();
/// assert_eq!(sql, r#"SELECT * FROM "users" AS "u""#);
/// ```
pub fn sqipe(table: impl IntoFromTable) -> Query<Value> {
    Query::new(table)
}

/// Create a new query builder with a custom value type.
pub fn sqipe_with<V: Clone + std::fmt::Debug>(table: impl IntoFromTable) -> Query<V> {
    Query::new(table)
}

/// Create a query that selects from a subquery instead of a table.
pub fn sqipe_from_subquery(sub: impl IntoSelectTree<Value>, alias: &str) -> Query<Value> {
    Query::from_subquery(sub, alias)
}

/// Create a query that selects from a subquery with a custom value type.
pub fn sqipe_from_subquery_with<V: Clone + std::fmt::Debug>(
    sub: impl IntoSelectTree<V>,
    alias: &str,
) -> Query<V> {
    Query::from_subquery(sub, alias)
}

impl<V: Clone + std::fmt::Debug> IntoSelectTree<V> for Query<V> {
    fn into_select_tree(self) -> crate::tree::SelectTree<V> {
        crate::tree::SelectTree::from_query_owned(self)
    }
}

/// `Debug` bound comes from `Query<V>` requiring `V: Debug`, not from this impl itself.
impl<V: Clone + std::fmt::Debug> IntoIncluded<V> for Query<V> {
    fn into_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::InSubQuery {
            col,
            sub: Box::new(crate::tree::SelectTree::from_query_owned(self)),
        }
    }

    fn into_not_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::NotInSubQuery {
            col,
            sub: Box::new(crate::tree::SelectTree::from_query_owned(self)),
        }
    }
}

fn resolve_join_condition(cond: &mut JoinCondition, join_table: &str) {
    match cond {
        JoinCondition::ColEq { right, .. } => {
            if right.table.is_none() {
                right.table = Some(join_table.to_string());
            }
        }
        JoinCondition::And(conditions) => {
            for c in conditions {
                resolve_join_condition(c, join_table);
            }
        }
        JoinCondition::Using(_) | JoinCondition::Expr(_) => {}
    }
}

impl<V: Clone + std::fmt::Debug> Query<V> {
    pub fn new(table: impl IntoFromTable) -> Self {
        let (name, alias) = table.into_from_table();
        Query {
            table: name,
            table_alias: alias,
            from_subquery: None,
            selects: Vec::new(),
            wheres: Vec::new(),
            havings: Vec::new(),
            aggregates: Vec::new(),
            group_bys: Vec::new(),
            joins: Vec::new(),
            join_subqueries: Vec::new(),
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
            stage_order: Vec::new(),
            lock_for: None,
        }
    }

    /// Create a query that selects from a subquery instead of a table.
    ///
    /// ```
    /// use sqipe::{sqipe, sqipe_from_subquery, col};
    ///
    /// let mut sub = sqipe("orders");
    /// sub.select(&["user_id", "amount"]);
    /// sub.and_where(col("status").eq("completed"));
    ///
    /// let mut q = sqipe_from_subquery(sub, "t");
    /// q.select(&["user_id"]);
    ///
    /// let (sql, binds) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
    /// );
    /// ```
    pub fn from_subquery(sub: impl IntoSelectTree<V>, alias: &str) -> Self {
        Query {
            table: String::new(),
            table_alias: Some(alias.to_string()),
            from_subquery: Some(Box::new(sub.into_select_tree())),
            selects: Vec::new(),
            wheres: Vec::new(),
            havings: Vec::new(),
            aggregates: Vec::new(),
            group_bys: Vec::new(),
            joins: Vec::new(),
            join_subqueries: Vec::new(),
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
            stage_order: Vec::new(),
            lock_for: None,
        }
    }

    pub fn as_(&mut self, alias: &str) -> &mut Self {
        self.table_alias = Some(alias.to_string());
        self
    }

    pub fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        if self.aggregates.is_empty() {
            self.stage_order
                .push(crate::tree::StageRef::Where(self.wheres.len()));
            self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        } else {
            self.havings.push(WhereEntry::And(cond.into_where_clause()));
        }
        self
    }

    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        if self.aggregates.is_empty() {
            self.stage_order
                .push(crate::tree::StageRef::Where(self.wheres.len()));
            self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        } else {
            self.havings.push(WhereEntry::Or(cond.into_where_clause()));
        }
        self
    }

    /// Append columns to the select list.
    ///
    /// Accepts `&[&str]` for simple column names or `&[Col]` for qualified/aliased columns.
    /// Can be called multiple times — each call appends to the existing list.
    pub fn select(&mut self, cols: &[impl Into<SelectItem> + Clone]) -> &mut Self {
        self.selects.extend(cols.iter().map(|c| c.clone().into()));
        self
    }

    /// Alias for [`select()`](Self::select).
    #[deprecated(
        since = "0.2.0",
        note = "Use `select()` instead, which now accepts both `&str` and `Col`."
    )]
    pub fn select_cols(&mut self, cols: &[Col]) -> &mut Self {
        self.select(cols)
    }

    /// Append a single column to the select list.
    pub fn add_select(&mut self, col: Col) -> &mut Self {
        self.selects.push(SelectItem::Col(col));
        self
    }

    /// Append a raw SQL expression to the select list.
    ///
    /// The expression is rendered as-is without quoting. Use this for
    /// expressions like `COUNT(*)`, `price * quantity`, etc.
    ///
    /// # Security
    ///
    /// The `raw` string is embedded directly into the generated SQL **without
    /// escaping or parameterization**. Never pass user-supplied input as `raw`;
    /// doing so opens the door to SQL injection. Only use hard-coded or
    /// application-controlled expressions.
    ///
    /// ```
    /// use sqipe::sqipe;
    ///
    /// let mut q = sqipe("users");
    /// q.add_select_expr("COUNT(*)", Some("cnt"));
    ///
    /// let (sql, _) = q.to_sql();
    /// assert_eq!(sql, r#"SELECT COUNT(*) AS "cnt" FROM "users""#);
    /// ```
    pub fn add_select_expr(&mut self, raw: &str, alias: Option<&str>) -> &mut Self {
        self.selects.push(SelectItem::Expr {
            raw: raw.to_string(),
            alias: alias.map(|a| a.to_string()),
        });
        self
    }

    pub fn aggregate(&mut self, exprs: &[AggregateExpr]) -> &mut Self {
        self.aggregates = exprs.to_vec();
        self
    }

    pub fn group_by(&mut self, cols: &[&str]) -> &mut Self {
        self.group_bys = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add an INNER JOIN clause.
    ///
    /// The order of `join` relative to `and_where` / `or_where` affects SQL generation.
    /// If `and_where` is called **before** `join`, standard SQL rendering wraps the
    /// preceding WHERE in a CTE so the filter is applied before the join.
    /// Pipe SQL always renders operations in call order without CTEs.
    pub fn join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = condition;
        resolve_join_condition(&mut condition, resolve_name);
        self.stage_order
            .push(crate::tree::StageRef::Join(self.joins.len()));
        self.joins.push(JoinClause {
            join_type: JoinType::Inner,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    /// Add a LEFT JOIN clause.
    ///
    /// See [`join`](Self::join) for how call order relative to `and_where` affects
    /// CTE generation in standard SQL.
    pub fn left_join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = condition;
        resolve_join_condition(&mut condition, resolve_name);
        self.stage_order
            .push(crate::tree::StageRef::Join(self.joins.len()));
        self.joins.push(JoinClause {
            join_type: JoinType::Left,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    /// Add a JOIN clause with a custom join type. Used by dialect crates for
    /// dialect-specific join types (e.g., STRAIGHT_JOIN in MySQL).
    ///
    /// See [`join`](Self::join) for how call order relative to `and_where` affects
    /// CTE generation in standard SQL.
    pub fn add_join(
        &mut self,
        join_type: JoinType,
        table: impl IntoJoinTable,
        condition: JoinCondition,
    ) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = condition;
        resolve_join_condition(&mut condition, resolve_name);
        self.stage_order
            .push(crate::tree::StageRef::Join(self.joins.len()));
        self.joins.push(JoinClause {
            join_type,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    /// Add an INNER JOIN with a subquery as the join target.
    ///
    /// ```
    /// use sqipe::{sqipe, col, table};
    ///
    /// let mut sub = sqipe("orders");
    /// sub.select(&["user_id", "total"]);
    /// sub.and_where(col("status").eq("shipped"));
    ///
    /// let mut q = sqipe("users");
    /// q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
    /// let (sql, _) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT * FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
    /// );
    /// ```
    pub fn join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        self.add_join_subquery(JoinType::Inner, sub, alias, condition)
    }

    /// Add a LEFT JOIN with a subquery as the join target.
    pub fn left_join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        self.add_join_subquery(JoinType::Left, sub, alias, condition)
    }

    /// Add a JOIN with a subquery and a custom join type.
    pub fn add_join_subquery(
        &mut self,
        join_type: JoinType,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        let tree = sub.into_select_tree();
        let mut condition = condition;
        resolve_join_condition(&mut condition, alias);
        self.stage_order
            .push(crate::tree::StageRef::Join(self.joins.len()));
        self.joins.push(JoinClause {
            join_type,
            table: String::new(),
            alias: Some(alias.to_string()),
            condition,
        });
        self.join_subqueries.push(Some(Box::new(tree)));
        self
    }

    pub fn union<T: AsUnionParts<Query = Query<V>>>(&self, other: &T) -> UnionQuery<V> {
        let mut parts = vec![(SetOp::Union, self.clone())];
        let other_parts = other.as_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((SetOp::Union, query));
            } else {
                parts.push((op, query));
            }
        }
        UnionQuery {
            parts,
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
        }
    }

    pub fn union_all<T: AsUnionParts<Query = Query<V>>>(&self, other: &T) -> UnionQuery<V> {
        let mut parts = vec![(SetOp::Union, self.clone())];
        let other_parts = other.as_union_parts();
        for (i, (op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((SetOp::UnionAll, query));
            } else {
                parts.push((op, query));
            }
        }
        UnionQuery {
            parts,
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
        }
    }

    pub fn order_by(&mut self, clause: OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
        self
    }

    /// Append a raw SQL expression to the ORDER BY clause.
    ///
    /// The expression is rendered as-is without quoting. Use this for
    /// expressions like `RAND()`, `id DESC NULLS FIRST`, etc.
    ///
    /// ```
    /// use sqipe::{sqipe, RawSql};
    ///
    /// let mut q = sqipe("users");
    /// q.select(&["id", "name"]);
    /// q.order_by_expr(RawSql::new("RAND()"));
    ///
    /// let (sql, _) = q.to_sql();
    /// assert_eq!(sql, r#"SELECT "id", "name" FROM "users" ORDER BY RAND()"#);
    /// ```
    pub fn order_by_expr(&mut self, raw: crate::RawSql) -> &mut Self {
        self.order_bys.push(OrderByClause::Expr(raw));
        self
    }

    pub fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    pub fn offset(&mut self, n: u64) -> &mut Self {
        self.offset_val = Some(n);
        self
    }

    /// Append a `FOR <clause>` locking clause to the generated SQL.
    ///
    /// This is the base method for row-level locking. Use [`for_update`](Self::for_update)
    /// for the common case.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut q = sqipe("users");
    /// q.select(&["id", "name"]);
    /// q.and_where(col("id").eq(1));
    /// q.for_with("NO KEY UPDATE");
    ///
    /// let (sql, _binds) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR NO KEY UPDATE"#
    /// );
    /// ```
    pub fn for_with(&mut self, clause: &str) -> &mut Self {
        debug_assert!(!clause.is_empty(), "lock clause must not be empty");
        self.lock_for = Some(clause.to_string());
        self
    }

    /// Append `FOR UPDATE` to the generated SQL.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut q = sqipe("users");
    /// q.select(&["id", "name"]);
    /// q.and_where(col("id").eq(1));
    /// q.for_update();
    ///
    /// let (sql, _binds) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE"#
    /// );
    /// ```
    pub fn for_update(&mut self) -> &mut Self {
        self.for_with("UPDATE")
    }

    /// Append `FOR UPDATE` with an option (e.g., `NOWAIT`, `SKIP LOCKED`).
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut q = sqipe("users");
    /// q.select(&["id", "name"]);
    /// q.and_where(col("id").eq(1));
    /// q.for_update_with("NOWAIT");
    ///
    /// let (sql, _binds) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE NOWAIT"#
    /// );
    /// ```
    pub fn for_update_with(&mut self, option: &str) -> &mut Self {
        self.for_with(&format!("UPDATE {}", option))
    }

    /// Build a SelectTree from this query.
    pub fn to_tree(&self) -> SelectTree<V> {
        SelectTree::from_query(self)
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        StandardSqlRenderer.render_select(&tree, &cfg)
    }

    /// Build pipe syntax SQL with `?` placeholders and double-quote identifiers.
    pub fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        PipeSqlRenderer.render_select(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        StandardSqlRenderer.render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
    }

    /// Build pipe syntax SQL with dialect-specific placeholders and quoting.
    pub fn to_pipe_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        PipeSqlRenderer.render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
    }
}

impl<V: Clone + std::fmt::Debug> UnionQueryOps<V> for UnionQuery<V> {
    fn union<T: AsUnionParts<Query = Query<V>>>(&mut self, other: &T) -> &mut Self {
        let parts = other.as_union_parts();
        for (i, (op, query)) in parts.into_iter().enumerate() {
            if i == 0 {
                self.parts.push((SetOp::Union, query));
            } else {
                self.parts.push((op, query));
            }
        }
        self
    }

    fn union_all<T: AsUnionParts<Query = Query<V>>>(&mut self, other: &T) -> &mut Self {
        let parts = other.as_union_parts();
        for (i, (op, query)) in parts.into_iter().enumerate() {
            if i == 0 {
                self.parts.push((SetOp::UnionAll, query));
            } else {
                self.parts.push((op, query));
            }
        }
        self
    }

    fn order_by(&mut self, clause: OrderByClause) -> &mut Self {
        self.order_bys.push(clause);
        self
    }

    fn order_by_expr(&mut self, raw: crate::RawSql) -> &mut Self {
        self.order_bys.push(OrderByClause::Expr(raw));
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
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        StandardSqlRenderer.render_union(&tree, &cfg)
    }

    fn to_pipe_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        PipeSqlRenderer.render_union(&tree, &cfg)
    }
}

impl<V: Clone + std::fmt::Debug> UnionQuery<V> {
    /// Build a UnionTree from this union query.
    pub fn to_tree(&self) -> UnionTree<V> {
        UnionTree::from_union_query(self)
    }

    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        StandardSqlRenderer.render_union(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
    }

    pub fn to_pipe_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        PipeSqlRenderer.render_union(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
    }

    /// Returns the parts for dialect wrappers to build SQL with custom rendering per part.
    pub fn parts(&self) -> &[(SetOp, Query<V>)] {
        &self.parts
    }
}

impl<V: Clone + std::fmt::Debug> Query<V> {
    /// Convert this SELECT query builder into an UPDATE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut q = sqipe("employee");
    /// q.and_where(col("id").eq(1));
    /// let mut u = q.into_update();
    /// u.set(col("name"), "Alice");
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    pub fn into_update(self) -> UpdateQuery<V> {
        assert!(
            self.joins.is_empty(),
            "Query has JOINs which are not supported in UPDATE and will be discarded"
        );
        assert!(
            self.aggregates.is_empty(),
            "Query has aggregates which are not supported in UPDATE and will be discarded"
        );
        assert!(
            self.order_bys.is_empty(),
            "Query has ORDER BY which is not supported in UPDATE and will be discarded"
        );
        assert!(
            self.limit_val.is_none(),
            "Query has LIMIT which is not supported in UPDATE and will be discarded"
        );
        UpdateQuery::new(self.table, self.table_alias, self.wheres)
    }

    /// Convert this SELECT query builder into a DELETE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut q = sqipe("employee");
    /// q.and_where(col("id").eq(1));
    /// let d = q.into_delete();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    /// ```
    pub fn into_delete(self) -> DeleteQuery<V> {
        assert!(
            self.joins.is_empty(),
            "Query has JOINs which are not supported in DELETE and will be discarded"
        );
        assert!(
            self.aggregates.is_empty(),
            "Query has aggregates which are not supported in DELETE and will be discarded"
        );
        assert!(
            self.order_bys.is_empty(),
            "Query has ORDER BY which is not supported in DELETE and will be discarded"
        );
        assert!(
            self.limit_val.is_none(),
            "Query has LIMIT which is not supported in DELETE and will be discarded"
        );
        DeleteQuery::new(self.table, self.table_alias, self.wheres)
    }
}
