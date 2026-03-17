use crate::column::OrderByClause;
use crate::column::{Col, SelectItem, TableRef};
use crate::delete::DeleteQuery;
use crate::insert::InsertQuery;
use crate::join::{JoinClause, JoinCondition, JoinType};
use crate::raw_sql::RawSql;
use crate::update::UpdateQuery;
use crate::value::Value;
use crate::where_clause::{IntoIncluded, IntoWhereClause, WhereClause, WhereEntry};

use crate::renderer::standard::StandardSqlRenderer;
use crate::renderer::{RenderConfig, Renderer};
use crate::tree::SelectTree;

use crate::Dialect;

/// SQL set operation type (UNION, INTERSECT, EXCEPT and their ALL variants).
#[derive(Debug, Clone)]
pub enum SetOp {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

/// Trait for types that can be converted into a `SelectTree` for use as a FROM subquery.
///
/// Implement this trait to allow your custom query type (e.g., `MysqlQuery`)
/// to be passed to `qbey_from_subquery_with()`.
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

/// Trait for SELECT query builder methods.
///
/// Implement this trait on dialect-specific SELECT wrappers to ensure they
/// expose the same builder API as the core [`SelectQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait SelectQueryBuilder<V: Clone + std::fmt::Debug> {
    /// Set a table alias.
    fn as_(&mut self, alias: &str) -> &mut Self;
    /// Enable SELECT DISTINCT.
    fn distinct(&mut self) -> &mut Self;
    /// Add an AND WHERE condition.
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an OR WHERE condition.
    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an AND HAVING condition.
    fn and_having(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an OR HAVING condition.
    fn or_having(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Shorthand for [`and_having`](Self::and_having).
    fn having(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.and_having(cond)
    }
    /// Append columns to the select list.
    ///
    /// Accepts `&[&str]` for simple column names or `&[Col]` for qualified/aliased columns.
    /// Can be called multiple times — each call appends to the existing list.
    fn select(&mut self, cols: &[impl Into<SelectItem> + Clone]) -> &mut Self;
    /// Append a single item to the select list.
    ///
    /// Accepts a `Col`, `SelectItem`, or any type that implements `Into<SelectItem>`.
    fn add_select(&mut self, item: impl Into<SelectItem>) -> &mut Self;
    /// Append a raw SQL expression to the select list.
    ///
    /// # Security
    ///
    /// The `raw` string is embedded directly into the generated SQL **without
    /// escaping or parameterization**. Never pass user-supplied input as `raw`;
    /// doing so opens the door to SQL injection. Only use hard-coded or
    /// application-controlled expressions.
    fn add_select_expr(&mut self, raw: RawSql<V>, alias: Option<&str>) -> &mut Self;
    /// Set the GROUP BY columns.
    fn group_by(&mut self, cols: &[&str]) -> &mut Self;
    /// Add an INNER JOIN clause.
    fn join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self;
    /// Add a LEFT JOIN clause.
    fn left_join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self;
    /// Add a JOIN clause with a custom join type. Used by dialect crates for
    /// dialect-specific join types (e.g., STRAIGHT_JOIN in MySQL).
    fn add_join(
        &mut self,
        join_type: JoinType,
        table: impl IntoJoinTable,
        condition: JoinCondition,
    ) -> &mut Self;
    /// Add an INNER JOIN with a subquery as the join target.
    fn join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self;
    /// Add a LEFT JOIN with a subquery as the join target.
    fn left_join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self;
    /// Add a JOIN with a subquery and a custom join type.
    fn add_join_subquery(
        &mut self,
        join_type: JoinType,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self;
    /// Add an ORDER BY clause.
    fn order_by(&mut self, clause: OrderByClause) -> &mut Self;
    /// Append a raw SQL expression to the ORDER BY clause.
    ///
    /// The expression is rendered as-is without quoting. Use this for
    /// expressions like `RAND()`, `id DESC NULLS FIRST`, etc.
    ///
    /// # Security
    ///
    /// Never pass user-supplied input as `raw`.
    fn order_by_expr(&mut self, raw: RawSql<V>) -> &mut Self;
    /// Set the LIMIT value.
    fn limit(&mut self, n: u64) -> &mut Self;
    /// Set the OFFSET value.
    fn offset(&mut self, n: u64) -> &mut Self;
    /// Append a `FOR <clause>` locking clause to the generated SQL.
    ///
    /// This is the base method for row-level locking. Use [`for_update`](Self::for_update)
    /// for the common case.
    fn for_with(&mut self, clause: &str) -> &mut Self;

    /// Append `FOR UPDATE` to the generated SQL.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
    ///
    /// let mut q = qbey("users");
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
    fn for_update(&mut self) -> &mut Self {
        self.for_with("UPDATE")
    }

    /// Append `FOR UPDATE` with an option (e.g., `NOWAIT`, `SKIP LOCKED`).
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
    ///
    /// let mut q = qbey("users");
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
    fn for_update_with(&mut self, option: &str) -> &mut Self {
        self.for_with(&format!("UPDATE {}", option))
    }
}

/// The SELECT query builder, generic over the bind value type `V`.
///
/// Supports both simple SELECT queries and compound queries with set operations
/// (UNION, INTERSECT, EXCEPT). When `set_operations` is non-empty, all parts
/// are stored there and `order_bys`/`limit_val`/`offset_val` apply to the
/// entire compound result.
#[derive(Debug, Clone)]
pub struct SelectQuery<V: Clone + std::fmt::Debug = Value> {
    /// Table name for table-based queries. Empty when using `from_subquery` or set operations.
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    /// When set, the query selects from this subquery instead of `table`.
    pub(crate) from_subquery: Option<Box<crate::tree::SelectTree<V>>>,
    pub(crate) distinct: bool,
    pub(crate) selects: Vec<SelectItem<V>>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub(crate) group_bys: Vec<String>,
    pub(crate) joins: Vec<JoinClause<V>>,
    /// Subquery sources for joins, aligned with `joins` by index.
    pub(crate) join_subqueries: Vec<Option<Box<crate::tree::SelectTree<V>>>>,
    pub(crate) order_bys: Vec<OrderByClause<V>>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
    /// Row-level locking clause (e.g., `"UPDATE"` → `FOR UPDATE`).
    pub(crate) lock_for: Option<String>,
    /// When non-empty, this query is a compound query (UNION, INTERSECT, EXCEPT).
    /// All parts are stored here; the outer `order_bys`/`limit_val`/`offset_val`
    /// apply to the entire compound result.
    pub(crate) set_operations: Vec<(SetOp, SelectQuery<V>)>,
}

/// Create a new query builder for the given table.
///
/// Accepts a table name (`&str`) or a [`TableRef`] (created with [`table()`]):
///
/// ```
/// use qbey::{qbey, table};
///
/// // Simple table name
/// let q = qbey("users");
/// let (sql, _) = q.to_sql();
/// assert_eq!(sql, r#"SELECT * FROM "users""#);
///
/// // TableRef with alias
/// let q = qbey(table("users").as_("u"));
/// let (sql, _) = q.to_sql();
/// assert_eq!(sql, r#"SELECT * FROM "users" AS "u""#);
/// ```
pub fn qbey(table: impl IntoFromTable) -> SelectQuery<Value> {
    SelectQuery::new(table)
}

/// Create a new query builder with a custom value type.
pub fn qbey_with<V: Clone + std::fmt::Debug>(table: impl IntoFromTable) -> SelectQuery<V> {
    SelectQuery::new(table)
}

/// Create a query that selects from a subquery instead of a table.
pub fn qbey_from_subquery(sub: impl IntoSelectTree<Value>, alias: &str) -> SelectQuery<Value> {
    SelectQuery::from_subquery(sub, alias)
}

/// Create a query that selects from a subquery with a custom value type.
pub fn qbey_from_subquery_with<V: Clone + std::fmt::Debug>(
    sub: impl IntoSelectTree<V>,
    alias: &str,
) -> SelectQuery<V> {
    SelectQuery::from_subquery(sub, alias)
}

impl<V: Clone + std::fmt::Debug> IntoSelectTree<V> for SelectQuery<V> {
    fn into_select_tree(self) -> crate::tree::SelectTree<V> {
        crate::tree::SelectTree::from_query_owned(self)
    }
}

/// `Debug` bound comes from `SelectQuery<V>` requiring `V: Debug`, not from this impl itself.
impl<V: Clone + std::fmt::Debug> IntoIncluded<V> for SelectQuery<V> {
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

fn resolve_join_condition<V: Clone>(cond: &mut JoinCondition<V>, join_table: &str) {
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

impl<V: Clone + std::fmt::Debug> SelectQueryBuilder<V> for SelectQuery<V> {
    fn as_(&mut self, alias: &str) -> &mut Self {
        self.table_alias = Some(alias.to_string());
        self
    }

    fn distinct(&mut self) -> &mut Self {
        debug_assert!(
            self.set_operations.is_empty(),
            "distinct() has no effect on compound queries (UNION/INTERSECT/EXCEPT); call it on individual sub-queries instead"
        );
        self.distinct = true;
        self
    }

    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    fn and_having(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.havings.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    fn or_having(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.havings.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    fn select(&mut self, cols: &[impl Into<SelectItem> + Clone]) -> &mut Self {
        self.selects.extend(
            cols.iter()
                .map(|c| SelectItem::from_default(c.clone().into())),
        );
        self
    }

    fn add_select(&mut self, item: impl Into<SelectItem>) -> &mut Self {
        self.selects.push(SelectItem::from_default(item.into()));
        self
    }

    fn add_select_expr(&mut self, raw: RawSql<V>, alias: Option<&str>) -> &mut Self {
        self.selects.push(SelectItem::Expr {
            raw,
            alias: alias.map(|a| a.to_string()),
        });
        self
    }

    fn group_by(&mut self, cols: &[&str]) -> &mut Self {
        self.group_bys = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    fn join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = JoinCondition::from_default(condition);
        resolve_join_condition(&mut condition, resolve_name);

        self.joins.push(JoinClause {
            join_type: JoinType::Inner,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    fn left_join(&mut self, table: impl IntoJoinTable, condition: JoinCondition) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = JoinCondition::from_default(condition);
        resolve_join_condition(&mut condition, resolve_name);

        self.joins.push(JoinClause {
            join_type: JoinType::Left,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    fn add_join(
        &mut self,
        join_type: JoinType,
        table: impl IntoJoinTable,
        condition: JoinCondition,
    ) -> &mut Self {
        let (name, alias) = table.into_join_table();
        let resolve_name = alias.as_deref().unwrap_or(&name);
        let mut condition = JoinCondition::from_default(condition);
        resolve_join_condition(&mut condition, resolve_name);

        self.joins.push(JoinClause {
            join_type,
            table: name,
            alias,
            condition,
        });
        self.join_subqueries.push(None);
        self
    }

    fn join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        self.add_join_subquery(JoinType::Inner, sub, alias, condition)
    }

    fn left_join_subquery(
        &mut self,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        self.add_join_subquery(JoinType::Left, sub, alias, condition)
    }

    fn add_join_subquery(
        &mut self,
        join_type: JoinType,
        sub: impl IntoSelectTree<V>,
        alias: &str,
        condition: JoinCondition,
    ) -> &mut Self {
        let tree = sub.into_select_tree();
        let mut condition = JoinCondition::from_default(condition);
        resolve_join_condition(&mut condition, alias);

        self.joins.push(JoinClause {
            join_type,
            table: String::new(),
            alias: Some(alias.to_string()),
            condition,
        });
        self.join_subqueries.push(Some(Box::new(tree)));
        self
    }

    fn order_by(&mut self, clause: OrderByClause) -> &mut Self {
        self.order_bys.push(OrderByClause::from_default(clause));
        self
    }

    fn order_by_expr(&mut self, raw: RawSql<V>) -> &mut Self {
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

    fn for_with(&mut self, clause: &str) -> &mut Self {
        debug_assert!(!clause.is_empty(), "lock clause must not be empty");
        self.lock_for = Some(clause.to_string());
        self
    }
}

impl<V: Clone + std::fmt::Debug> SelectQuery<V> {
    pub fn new(table: impl IntoFromTable) -> Self {
        let (name, alias) = table.into_from_table();
        SelectQuery {
            table: name,
            table_alias: alias,
            from_subquery: None,
            distinct: false,
            selects: Vec::new(),
            wheres: Vec::new(),
            havings: Vec::new(),
            group_bys: Vec::new(),
            joins: Vec::new(),
            join_subqueries: Vec::new(),
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
            lock_for: None,
            set_operations: Vec::new(),
        }
    }

    /// Create a query that selects from a subquery instead of a table.
    ///
    /// ```
    /// use qbey::{qbey, qbey_from_subquery, col, ConditionExpr, SelectQueryBuilder};
    ///
    /// let mut sub = qbey("orders");
    /// sub.select(&["user_id", "amount"]);
    /// sub.and_where(col("status").eq("completed"));
    ///
    /// let mut q = qbey_from_subquery(sub, "t");
    /// q.select(&["user_id"]);
    ///
    /// let (sql, binds) = q.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
    /// );
    /// ```
    pub fn from_subquery(sub: impl IntoSelectTree<V>, alias: &str) -> Self {
        SelectQuery {
            table: String::new(),
            table_alias: Some(alias.to_string()),
            from_subquery: Some(Box::new(sub.into_select_tree())),
            distinct: false,
            selects: Vec::new(),
            wheres: Vec::new(),
            havings: Vec::new(),
            group_bys: Vec::new(),
            joins: Vec::new(),
            join_subqueries: Vec::new(),
            order_bys: Vec::new(),
            limit_val: None,
            offset_val: None,
            lock_for: None,
            set_operations: Vec::new(),
        }
    }

    /// Create a new compound query by combining `self` and `other` with the given set operation.
    ///
    /// If `other` is already a compound query (has set_operations), its parts are flattened.
    fn combine(&self, op: SetOp, other: &SelectQuery<V>) -> SelectQuery<V> {
        let mut parts = self.as_set_operation_parts();
        let other_parts = other.as_set_operation_parts();
        for (i, (other_op, query)) in other_parts.into_iter().enumerate() {
            if i == 0 {
                parts.push((op.clone(), query));
            } else {
                parts.push((other_op, query));
            }
        }
        let mut q = SelectQuery::new("");
        q.set_operations = parts;
        q
    }

    /// Append `other` to this compound query with the given set operation (mutating).
    ///
    /// If `self` is not yet a compound query, it is converted into one.
    fn add_combine(&mut self, op: SetOp, other: &SelectQuery<V>) {
        if self.set_operations.is_empty() {
            // Convert self into a compound query: move current state into
            // set_operations and reset self to an empty shell.
            let first = self.clone();
            *self = SelectQuery::new("");
            self.set_operations = vec![(SetOp::Union, first)]; // first part's SetOp is placeholder
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

    /// Returns the parts of this query for use in set operations.
    /// If this is a compound query, returns its parts; otherwise returns self as a single part.
    fn as_set_operation_parts(&self) -> Vec<(SetOp, SelectQuery<V>)> {
        if self.set_operations.is_empty() {
            vec![(SetOp::Union, self.clone())] // SetOp is placeholder for the first part
        } else {
            self.set_operations.clone()
        }
    }

    pub fn union(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::Union, other)
    }

    pub fn union_all(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::UnionAll, other)
    }

    pub fn intersect(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::Intersect, other)
    }

    pub fn intersect_all(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::IntersectAll, other)
    }

    pub fn except(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::Except, other)
    }

    pub fn except_all(&self, other: &SelectQuery<V>) -> SelectQuery<V> {
        self.combine(SetOp::ExceptAll, other)
    }

    /// Append `other` with UNION to this compound query (mutating).
    pub fn add_union(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::Union, other);
        self
    }

    /// Append `other` with UNION ALL to this compound query (mutating).
    pub fn add_union_all(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::UnionAll, other);
        self
    }

    /// Append `other` with INTERSECT to this compound query (mutating).
    pub fn add_intersect(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::Intersect, other);
        self
    }

    /// Append `other` with INTERSECT ALL to this compound query (mutating).
    pub fn add_intersect_all(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::IntersectAll, other);
        self
    }

    /// Append `other` with EXCEPT to this compound query (mutating).
    pub fn add_except(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::Except, other);
        self
    }

    /// Append `other` with EXCEPT ALL to this compound query (mutating).
    pub fn add_except_all(&mut self, other: &SelectQuery<V>) -> &mut Self {
        self.add_combine(SetOp::ExceptAll, other);
        self
    }

    /// Returns true if this query is a compound query (has set operations).
    pub fn has_set_operations(&self) -> bool {
        !self.set_operations.is_empty()
    }

    /// Returns the set operation parts for compound queries.
    pub fn set_operations(&self) -> &[(SetOp, SelectQuery<V>)] {
        &self.set_operations
    }

    /// Returns the ORDER BY clauses.
    pub fn order_bys(&self) -> &[OrderByClause<V>] {
        &self.order_bys
    }

    /// Returns the LIMIT value.
    pub fn limit_val(&self) -> Option<u64> {
        self.limit_val
    }

    /// Returns the OFFSET value.
    pub fn offset_val(&self) -> Option<u64> {
        self.offset_val
    }

    /// Build a SelectTree from this query.
    pub fn to_tree(&self) -> SelectTree<V> {
        SelectTree::from_query(self)
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        StandardSqlRenderer.render_select(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
    }
}

impl<V: Clone + std::fmt::Debug> SelectQuery<V> {
    /// Convert this SELECT query builder into an UPDATE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder, UpdateQueryBuilder};
    ///
    /// let mut q = qbey("employee");
    /// q.and_where(col("id").eq(1));
    /// let mut u = q.into_update();
    /// u.set(col("name"), "Alice");
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    pub fn into_update(self) -> UpdateQuery<V> {
        assert!(
            self.set_operations.is_empty(),
            "Compound query (set operations) cannot be converted to UPDATE"
        );
        assert!(
            self.joins.is_empty(),
            "SelectQuery has JOINs which are not supported in UPDATE and will be discarded"
        );
        assert!(
            self.order_bys.is_empty(),
            "SelectQuery has ORDER BY which is not supported in UPDATE and will be discarded"
        );
        assert!(
            self.limit_val.is_none(),
            "SelectQuery has LIMIT which is not supported in UPDATE and will be discarded"
        );
        UpdateQuery::new(self.table, self.table_alias, self.wheres)
    }

    /// Convert this SELECT query builder into an INSERT query builder.
    ///
    /// Consumes `self` and transfers the table name. WHERE conditions, JOINs,
    /// ORDER BY, and LIMIT are not applicable to INSERT and will cause a panic
    /// if present.
    ///
    /// ```
    /// use qbey::{qbey, Value, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?)"#);
    /// ```
    pub fn into_insert(self) -> InsertQuery<V> {
        assert!(
            self.set_operations.is_empty(),
            "Compound query (set operations) cannot be converted to INSERT"
        );
        assert!(
            self.joins.is_empty(),
            "SelectQuery has JOINs which are not supported in INSERT"
        );
        assert!(
            self.wheres.is_empty(),
            "SelectQuery has WHERE which is not supported in INSERT"
        );
        assert!(
            self.order_bys.is_empty(),
            "SelectQuery has ORDER BY which is not supported in INSERT"
        );
        assert!(
            self.limit_val.is_none(),
            "SelectQuery has LIMIT which is not supported in INSERT"
        );
        InsertQuery::new(self.table)
    }

    /// Convert this SELECT query builder into a DELETE query builder.
    ///
    /// Consumes `self` and transfers the table name, alias, and WHERE conditions.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, SelectQueryBuilder};
    ///
    /// let mut q = qbey("employee");
    /// q.and_where(col("id").eq(1));
    /// let d = q.into_delete();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    /// ```
    pub fn into_delete(self) -> DeleteQuery<V> {
        assert!(
            self.set_operations.is_empty(),
            "Compound query (set operations) cannot be converted to DELETE"
        );
        assert!(
            self.joins.is_empty(),
            "SelectQuery has JOINs which are not supported in DELETE and will be discarded"
        );
        assert!(
            self.order_bys.is_empty(),
            "SelectQuery has ORDER BY which is not supported in DELETE and will be discarded"
        );
        assert!(
            self.limit_val.is_none(),
            "SelectQuery has LIMIT which is not supported in DELETE and will be discarded"
        );
        DeleteQuery::new(self.table, self.table_alias, self.wheres)
    }
}
