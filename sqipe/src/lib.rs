#[doc = include_str!("../../README.md")]
#[cfg(doctest)]
struct ReadmeDocTests;

/// Value represents a bind parameter value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Int(n as i64)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

/// Comparison operator.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl Op {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Gt => ">",
            Op::Lt => "<",
            Op::Gte => ">=",
            Op::Lte => "<=",
        }
    }
}

/// A table reference for building qualified column references.
#[derive(Debug, Clone)]
pub struct TableRef {
    name: String,
}

/// Create a table reference for qualified column names.
pub fn table(name: &str) -> TableRef {
    TableRef {
        name: name.to_string(),
    }
}

impl TableRef {
    pub fn col(&self, col: &str) -> QualifiedCol {
        QualifiedCol {
            table: self.name.clone(),
            col: col.to_string(),
        }
    }
}

/// A column qualified with a table name (e.g., `"users"."id"`).
#[derive(Debug, Clone)]
pub struct QualifiedCol {
    pub table: String,
    pub col: String,
}

/// A column reference in a JOIN condition, optionally qualified with a table name.
/// When `table` is `None`, the table name is inferred from the `join()` / `left_join()` call.
#[derive(Debug, Clone)]
pub struct JoinCol {
    pub table: Option<String>,
    pub col: String,
}

impl From<QualifiedCol> for JoinCol {
    fn from(qc: QualifiedCol) -> Self {
        JoinCol {
            table: Some(qc.table),
            col: qc.col,
        }
    }
}

impl From<&str> for JoinCol {
    fn from(col: &str) -> Self {
        JoinCol {
            table: None,
            col: col.to_string(),
        }
    }
}

/// A JOIN ON condition.
#[derive(Debug, Clone)]
pub enum JoinCondition {
    ColEq { left: QualifiedCol, right: JoinCol },
    And(Vec<JoinCondition>),
}

/// JOIN type.
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
}

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub condition: JoinCondition,
}

/// Column reference in WHERE conditions — either simple or table-qualified.
#[derive(Debug, Clone)]
pub enum ColRef {
    Simple(String),
    Qualified { table: String, col: String },
}

/// Trait for converting into a `ColRef`.
pub trait IntoColRef {
    fn into_col_ref(self) -> ColRef;
}

macro_rules! impl_col_methods {
    ($ty:ty) => {
        impl $ty {
            pub fn eq(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Eq,
                    val: val.into(),
                }
            }

            pub fn ne(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Ne,
                    val: val.into(),
                }
            }

            pub fn gt(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Gt,
                    val: val.into(),
                }
            }

            pub fn lt(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Lt,
                    val: val.into(),
                }
            }

            pub fn gte(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Gte,
                    val: val.into(),
                }
            }

            pub fn lte(self, val: impl Into<Value>) -> WhereClause {
                WhereClause::Condition {
                    col: self.into_col_ref(),
                    op: Op::Lte,
                    val: val.into(),
                }
            }

            pub fn between(self, low: impl Into<Value>, high: impl Into<Value>) -> WhereClause {
                WhereClause::Between {
                    col: self.into_col_ref(),
                    low: low.into(),
                    high: high.into(),
                }
            }

            /// Convert a Rust range into SQL conditions.
            ///
            /// - `20..=30` → `BETWEEN 20 AND 30`
            /// - `20..30`  → `col >= 20 AND col < 30`
            /// - `20..`    → `col >= 20`
            /// - `..30`    → `col < 30`
            /// - `..=30`   → `col <= 30`
            pub fn in_range<V: Into<Value>>(self, range: impl IntoRangeClause<V>) -> WhereClause {
                range.into_where_clause(self.into_col_ref())
            }
        }
    };
}

/// A column reference used to build conditions and order-by clauses.
#[derive(Debug, Clone)]
pub struct Col {
    name: String,
}

/// Create a column reference.
pub fn col(name: &str) -> Col {
    Col {
        name: name.to_string(),
    }
}

impl IntoColRef for Col {
    fn into_col_ref(self) -> ColRef {
        ColRef::Simple(self.name)
    }
}

impl_col_methods!(Col);

impl Col {
    pub fn asc(self) -> OrderByClause {
        OrderByClause {
            col: self.name,
            dir: SortDir::Asc,
        }
    }

    pub fn desc(self) -> OrderByClause {
        OrderByClause {
            col: self.name,
            dir: SortDir::Desc,
        }
    }
}

impl IntoColRef for QualifiedCol {
    fn into_col_ref(self) -> ColRef {
        ColRef::Qualified {
            table: self.table,
            col: self.col,
        }
    }
}

impl_col_methods!(QualifiedCol);

impl QualifiedCol {
    pub fn eq_col(self, other: impl Into<JoinCol>) -> JoinCondition {
        JoinCondition::ColEq {
            left: self,
            right: other.into(),
        }
    }
}

/// A WHERE condition tree.
#[derive(Debug, Clone)]
pub enum WhereClause {
    Condition {
        col: ColRef,
        op: Op,
        val: Value,
    },
    Between {
        col: ColRef,
        low: Value,
        high: Value,
    },
    Any(Vec<WhereClause>),
    All(Vec<WhereClause>),
}

/// Tuple shorthand: `("name", value)` becomes `col = value`.
impl<V: Into<Value>> From<(&str, V)> for WhereClause {
    fn from((col, val): (&str, V)) -> Self {
        WhereClause::Condition {
            col: ColRef::Simple(col.to_string()),
            op: Op::Eq,
            val: val.into(),
        }
    }
}

/// Trait for converting Rust range types into WhereClause.
pub trait IntoRangeClause<V: Into<Value>> {
    fn into_where_clause(self, col: ColRef) -> WhereClause;
}

use std::ops::{Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

/// `20..=30` → `col BETWEEN 20 AND 30`
impl<V: Into<Value>> IntoRangeClause<V> for RangeInclusive<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause {
        let (low, high) = self.into_inner();
        WhereClause::Between {
            col,
            low: low.into(),
            high: high.into(),
        }
    }
}

/// `20..30` → `col >= 20 AND col < 30`
impl<V: Into<Value>> IntoRangeClause<V> for Range<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause {
        WhereClause::All(vec![
            WhereClause::Condition {
                col: col.clone(),
                op: Op::Gte,
                val: self.start.into(),
            },
            WhereClause::Condition {
                col,
                op: Op::Lt,
                val: self.end.into(),
            },
        ])
    }
}

/// `20..` → `col >= 20`
impl<V: Into<Value>> IntoRangeClause<V> for RangeFrom<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause {
        WhereClause::Condition {
            col,
            op: Op::Gte,
            val: self.start.into(),
        }
    }
}

/// `..30` → `col < 30`
impl<V: Into<Value>> IntoRangeClause<V> for RangeTo<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause {
        WhereClause::Condition {
            col,
            op: Op::Lt,
            val: self.end.into(),
        }
    }
}

/// `..=30` → `col <= 30`
impl<V: Into<Value>> IntoRangeClause<V> for RangeToInclusive<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause {
        WhereClause::Condition {
            col,
            op: Op::Lte,
            val: self.end.into(),
        }
    }
}

/// Combine conditions with OR: `any(a, b)` => `(a OR b)`.
pub fn any(a: impl Into<WhereClause>, b: impl Into<WhereClause>) -> WhereClause {
    WhereClause::Any(vec![a.into(), b.into()])
}

/// Combine conditions with AND: `all(a, b)` => `(a AND b)`.
pub fn all(a: impl Into<WhereClause>, b: impl Into<WhereClause>) -> WhereClause {
    WhereClause::All(vec![a.into(), b.into()])
}

#[derive(Debug, Clone)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub col: String,
    pub dir: SortDir,
}

/// Aggregate expression builder.
pub mod aggregate {
    /// An aggregate expression that can be aliased with `.as_()`.
    #[derive(Debug, Clone)]
    pub struct AggregateExpr {
        pub(crate) expr: AggregateFunc,
        pub(crate) alias: Option<String>,
    }

    #[derive(Debug, Clone)]
    pub(crate) enum AggregateFunc {
        CountAll,
        Count(String),
        Sum(String),
        Avg(String),
        Min(String),
        Max(String),
        Expr(String),
    }

    impl AggregateExpr {
        pub fn as_(mut self, alias: &str) -> Self {
            self.alias = Some(alias.to_string());
            self
        }
    }

    pub fn count_all() -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::CountAll,
            alias: None,
        }
    }

    pub fn count(col: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Count(col.to_string()),
            alias: None,
        }
    }

    pub fn sum(col: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Sum(col.to_string()),
            alias: None,
        }
    }

    pub fn avg(col: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Avg(col.to_string()),
            alias: None,
        }
    }

    pub fn min(col: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Min(col.to_string()),
            alias: None,
        }
    }

    pub fn max(col: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Max(col.to_string()),
            alias: None,
        }
    }

    /// Raw SQL expression for dialect-specific aggregate functions.
    pub fn expr(raw: &str) -> AggregateExpr {
        AggregateExpr {
            expr: AggregateFunc::Expr(raw.to_string()),
            alias: None,
        }
    }
}

use aggregate::{AggregateExpr, AggregateFunc};

/// Trait for SQL dialect placeholder and quoting styles.
pub trait Dialect {
    fn placeholder(&self, index: usize) -> String;

    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WhereEntry {
    And(WhereClause),
    Or(WhereClause),
}

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
pub trait UnionQueryOps: AsUnionParts {
    fn union<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn union_all<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn order_by(&mut self, clause: OrderByClause) -> &mut Self;
    fn limit(&mut self, n: u64) -> &mut Self;
    fn offset(&mut self, n: u64) -> &mut Self;
    fn to_sql(&self) -> (String, Vec<Value>);
    fn to_pipe_sql(&self) -> (String, Vec<Value>);
}

pub mod renderer;
pub mod tree;

use renderer::pipe::PipeSqlRenderer;
use renderer::standard::StandardSqlRenderer;
use renderer::{RenderConfig, Renderer};
use tree::{SelectTree, UnionTree, default_quote_identifier};

/// The query builder.
#[derive(Debug, Clone)]
pub struct Query {
    pub(crate) table: String,
    pub(crate) selects: Vec<String>,
    pub(crate) wheres: Vec<WhereEntry>,
    pub(crate) havings: Vec<WhereEntry>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) group_bys: Vec<String>,
    pub(crate) joins: Vec<JoinClause>,
    pub(crate) order_bys: Vec<OrderByClause>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
}

/// A combined query built from UNION / UNION ALL operations.
#[derive(Debug, Clone)]
pub struct UnionQuery {
    pub(crate) parts: Vec<(SetOp, Query)>,
    pub(crate) order_bys: Vec<OrderByClause>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
}

impl AsUnionParts for Query {
    type Query = Query;
    fn as_union_parts(&self) -> Vec<(SetOp, Query)> {
        vec![(SetOp::Union, self.clone())] // SetOp is placeholder, caller overrides
    }
}

impl AsUnionParts for UnionQuery {
    type Query = Query;
    fn as_union_parts(&self) -> Vec<(SetOp, Query)> {
        self.parts.clone()
    }
}

/// Create a new query builder for the given table.
pub fn sqipe(table: &str) -> Query {
    Query {
        table: table.to_string(),
        selects: Vec::new(),
        wheres: Vec::new(),
        havings: Vec::new(),
        aggregates: Vec::new(),
        group_bys: Vec::new(),
        joins: Vec::new(),
        order_bys: Vec::new(),
        limit_val: None,
        offset_val: None,
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
    }
}

impl Query {
    pub fn and_where(&mut self, cond: impl Into<WhereClause>) -> &mut Self {
        if self.aggregates.is_empty() {
            self.wheres.push(WhereEntry::And(cond.into()));
        } else {
            self.havings.push(WhereEntry::And(cond.into()));
        }
        self
    }

    pub fn or_where(&mut self, cond: impl Into<WhereClause>) -> &mut Self {
        if self.aggregates.is_empty() {
            self.wheres.push(WhereEntry::Or(cond.into()));
        } else {
            self.havings.push(WhereEntry::Or(cond.into()));
        }
        self
    }

    pub fn select(&mut self, cols: &[&str]) -> &mut Self {
        self.selects = cols.iter().map(|s| s.to_string()).collect();
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

    pub fn join(&mut self, table: &str, condition: JoinCondition) -> &mut Self {
        let mut condition = condition;
        resolve_join_condition(&mut condition, table);
        self.joins.push(JoinClause {
            join_type: JoinType::Inner,
            table: table.to_string(),
            condition,
        });
        self
    }

    pub fn left_join(&mut self, table: &str, condition: JoinCondition) -> &mut Self {
        let mut condition = condition;
        resolve_join_condition(&mut condition, table);
        self.joins.push(JoinClause {
            join_type: JoinType::Left,
            table: table.to_string(),
            condition,
        });
        self
    }

    pub fn union<T: AsUnionParts<Query = Query>>(&self, other: &T) -> UnionQuery {
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

    pub fn union_all<T: AsUnionParts<Query = Query>>(&self, other: &T) -> UnionQuery {
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

    pub fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    pub fn offset(&mut self, n: u64) -> &mut Self {
        self.offset_val = Some(n);
        self
    }

    /// Build a SelectTree from this query.
    pub fn to_tree(&self) -> SelectTree {
        SelectTree::from_query(self)
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    pub fn to_sql(&self) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
        };
        StandardSqlRenderer.render_select(&tree, &cfg)
    }

    /// Build pipe syntax SQL with `?` placeholders and double-quote identifiers.
    pub fn to_pipe_sql(&self) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
        };
        PipeSqlRenderer.render_select(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        StandardSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    /// Build pipe syntax SQL with dialect-specific placeholders and quoting.
    pub fn to_pipe_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        PipeSqlRenderer.render_select(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }
}

impl UnionQueryOps for UnionQuery {
    fn union<T: AsUnionParts<Query = Query>>(&mut self, other: &T) -> &mut Self {
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

    fn union_all<T: AsUnionParts<Query = Query>>(&mut self, other: &T) -> &mut Self {
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

    fn limit(&mut self, n: u64) -> &mut Self {
        self.limit_val = Some(n);
        self
    }

    fn offset(&mut self, n: u64) -> &mut Self {
        self.offset_val = Some(n);
        self
    }

    fn to_sql(&self) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
        };
        StandardSqlRenderer.render_union(&tree, &cfg)
    }

    fn to_pipe_sql(&self) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
        };
        PipeSqlRenderer.render_union(&tree, &cfg)
    }
}

impl UnionQuery {
    /// Build a UnionTree from this union query.
    pub fn to_tree(&self) -> UnionTree {
        UnionTree::from_union_query(self)
    }

    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        StandardSqlRenderer.render_union(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    pub fn to_pipe_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<Value>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        PipeSqlRenderer.render_union(&tree, &RenderConfig { ph: &ph, qi: &qi })
    }

    /// Returns the parts for dialect wrappers to build SQL with custom rendering per part.
    pub fn parts(&self) -> &[(SetOp, Query)] {
        &self.parts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_select_to_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ?"
        );
        assert_eq!(binds, vec![Value::String("Alice".to_string())]);
    }

    #[test]
    fn test_basic_select_to_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"name\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_select_star_when_no_select() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"name\" = ?");
    }

    #[test]
    fn test_comparison_operators() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));
        q.and_where(col("age").lte(60));
        q.and_where(col("salary").lt(100000));
        q.and_where(col("level").gte(3));
        q.and_where(col("role").ne("intern"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ? AND \"age\" <= ? AND \"salary\" < ? AND \"level\" >= ? AND \"role\" != ?"
        );
    }

    #[test]
    fn test_or_where() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.or_where(col("role").eq("admin"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? OR \"role\" = ?"
        );
    }

    #[test]
    fn test_any_grouping() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(any(col("role").eq("admin"), col("role").eq("manager")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? AND (\"role\" = ? OR \"role\" = ?)"
        );
    }

    #[test]
    fn test_any_all_combined() {
        let mut q = sqipe("employee");
        q.and_where(any(
            all(col("role").eq("admin"), col("dept").eq("eng")),
            all(col("role").eq("manager"), col("dept").eq("sales")),
        ));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE (\"role\" = ? AND \"dept\" = ?) OR (\"role\" = ? AND \"dept\" = ?)"
        );
    }

    #[test]
    fn test_order_by() {
        let mut q = sqipe("employee");
        q.select(&["id", "name", "age"]);
        q.order_by(col("name").asc());
        q.order_by(col("age").desc());

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\", \"age\" FROM \"employee\" ORDER BY \"name\" ASC, \"age\" DESC"
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> SELECT \"id\", \"name\", \"age\" |> ORDER BY \"name\" ASC, \"age\" DESC"
        );
    }

    #[test]
    fn test_limit_offset() {
        let mut q = sqipe("employee");
        q.select(&["id", "name"]);
        q.limit(10);
        q.offset(20);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" LIMIT 10 OFFSET 20"
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> SELECT \"id\", \"name\" |> LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn test_method_chaining() {
        let (sql, _) = sqipe("employee")
            .and_where(("name", "Alice"))
            .and_where(col("age").gt(20))
            .select(&["id", "name"])
            .to_sql();

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = ? AND \"age\" > ?"
        );
    }

    #[test]
    fn test_aggregate_to_sql() {
        let mut q = sqipe("employee");
        q.aggregate(&[
            aggregate::count_all().as_("cnt"),
            aggregate::sum("salary").as_("total_salary"),
        ]);
        q.group_by(&["dept"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"dept\", COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" FROM \"employee\" GROUP BY \"dept\""
        );
    }

    #[test]
    fn test_aggregate_to_pipe_sql() {
        let mut q = sqipe("employee");
        q.aggregate(&[
            aggregate::count_all().as_("cnt"),
            aggregate::sum("salary").as_("total_salary"),
        ]);
        q.group_by(&["dept"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> AGGREGATE COUNT(*) AS \"cnt\", SUM(\"salary\") AS \"total_salary\" GROUP BY \"dept\""
        );
    }

    #[test]
    fn test_aggregate_without_group_by() {
        let mut q = sqipe("employee");
        q.aggregate(&[aggregate::count_all().as_("cnt")]);

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT COUNT(*) AS \"cnt\" FROM \"employee\"");

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(sql, "FROM \"employee\" |> AGGREGATE COUNT(*) AS \"cnt\"");
    }

    #[test]
    fn test_aggregate_with_where() {
        let mut q = sqipe("employee");
        q.and_where(col("active").eq(true));
        q.aggregate(&[aggregate::count_all().as_("cnt")]);
        q.group_by(&["dept"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" WHERE \"active\" = ? GROUP BY \"dept\""
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"active\" = ? |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"dept\""
        );
    }

    #[test]
    fn test_aggregate_all_functions() {
        let mut q = sqipe("employee");
        q.aggregate(&[
            aggregate::count_all().as_("cnt"),
            aggregate::count("id").as_("id_cnt"),
            aggregate::sum("salary").as_("total"),
            aggregate::avg("salary").as_("average"),
            aggregate::min("salary").as_("lowest"),
            aggregate::max("salary").as_("highest"),
        ]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT COUNT(*) AS \"cnt\", COUNT(\"id\") AS \"id_cnt\", SUM(\"salary\") AS \"total\", AVG(\"salary\") AS \"average\", MIN(\"salary\") AS \"lowest\", MAX(\"salary\") AS \"highest\" FROM \"employee\""
        );
    }

    #[test]
    fn test_aggregate_expr_raw() {
        let mut q = sqipe("employee");
        q.aggregate(&[
            aggregate::count_all().as_("cnt"),
            aggregate::expr("APPROX_COUNT_DISTINCT(user_id)").as_("approx_users"),
        ]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT COUNT(*) AS \"cnt\", APPROX_COUNT_DISTINCT(user_id) AS \"approx_users\" FROM \"employee\""
        );
    }

    #[test]
    fn test_union_all_to_sql() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let uq = q1.union_all(&q2);

        let (sql, binds) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ?"
        );
        assert_eq!(
            binds,
            vec![
                Value::String("eng".to_string()),
                Value::String("sales".to_string())
            ]
        );
    }

    #[test]
    fn test_union_to_sql() {
        let mut q1 = sqipe("employee");
        q1.select(&["dept"]);

        let mut q2 = sqipe("contractor");
        q2.select(&["dept"]);

        let uq = q1.union(&q2);

        let (sql, _) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT \"dept\" FROM \"employee\" UNION SELECT \"dept\" FROM \"contractor\""
        );
    }

    #[test]
    fn test_union_all_to_pipe_sql() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);

        let uq = q1.union_all(&q2);

        let (sql, _) = uq.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"dept\" = ? |> SELECT \"id\", \"name\" |> UNION ALL FROM \"employee\" |> WHERE \"dept\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_union_all_with_order_by_and_limit() {
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
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 10"
        );
    }

    #[test]
    fn test_union_query_merge() {
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

        let mut uq1 = q1.union_all(&q2);
        let uq2 = q3.union_all(&q4);
        uq1.union_all(&uq2);

        let (sql, _) = uq1.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"contractor\" WHERE \"dept\" = ? UNION ALL SELECT \"id\", \"name\" FROM \"contractor\" WHERE \"dept\" = ?"
        );
    }

    #[test]
    fn test_union_with_query_order_by_and_limit() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);
        q1.order_by(col("name").asc());
        q1.limit(5);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);
        q2.order_by(col("name").desc());
        q2.limit(3);

        let mut uq = q1.union_all(&q2);
        uq.order_by(col("id").asc());
        uq.limit(10);

        let (sql, _) = uq.to_sql();
        assert_eq!(
            sql,
            "(SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 5) UNION ALL (SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" DESC LIMIT 3) ORDER BY \"id\" ASC LIMIT 10"
        );
    }

    #[test]
    fn test_union_with_one_query_having_order_by() {
        let mut q1 = sqipe("employee");
        q1.and_where(("dept", "eng"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("employee");
        q2.and_where(("dept", "sales"));
        q2.select(&["id", "name"]);
        q2.order_by(col("name").asc());
        q2.limit(5);

        let uq = q1.union_all(&q2);

        let (sql, _) = uq.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? UNION ALL (SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = ? ORDER BY \"name\" ASC LIMIT 5)"
        );
    }

    #[test]
    fn test_binds_order() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));

        let (_, binds) = q.to_sql();
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(20)]
        );
    }

    #[test]
    fn test_having_via_and_where_after_aggregate() {
        let mut q = sqipe("employee");
        q.and_where(col("active").eq(true));
        q.aggregate(&[aggregate::count_all().as_("cnt")]);
        q.group_by(&["dept"]);
        q.and_where(col("cnt").gt(5));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"dept\", COUNT(*) AS \"cnt\" FROM \"employee\" WHERE \"active\" = ? GROUP BY \"dept\" HAVING \"cnt\" > ?"
        );
        assert_eq!(binds, vec![Value::Bool(true), Value::Int(5)]);
    }

    #[test]
    fn test_having_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(col("active").eq(true));
        q.aggregate(&[aggregate::count_all().as_("cnt")]);
        q.group_by(&["dept"]);
        q.and_where(col("cnt").gt(5));

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"active\" = ? |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"dept\" |> WHERE \"cnt\" > ?"
        );
        assert_eq!(binds, vec![Value::Bool(true), Value::Int(5)]);
    }

    #[test]
    fn test_between() {
        let mut q = sqipe("employee");
        q.and_where(col("age").between(20, 30));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_between_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(col("age").between(20, 30));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"age\" BETWEEN ? AND ? |> SELECT \"id\", \"name\""
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_between_with_other_conditions() {
        let mut q = sqipe("employee");
        q.and_where(("dept", "eng"));
        q.and_where(col("age").between(20, 30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"dept\" = ? AND \"age\" BETWEEN ? AND ?"
        );
        assert_eq!(
            binds,
            vec![
                Value::String("eng".to_string()),
                Value::Int(20),
                Value::Int(30)
            ]
        );
    }

    #[test]
    fn test_in_range_inclusive() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..=30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_in_range_exclusive() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"age\" >= ? AND \"age\" < ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_in_range_from() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(20..));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" >= ?");
        assert_eq!(binds, vec![Value::Int(20)]);
    }

    #[test]
    fn test_in_range_to() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(..30));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" < ?");
        assert_eq!(binds, vec![Value::Int(30)]);
    }

    #[test]
    fn test_inner_join_standard() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_inner_join_pipe() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_left_join_standard() {
        let mut q = sqipe("users");
        q.left_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_left_join_pipe() {
        let mut q = sqipe("users");
        q.left_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> LEFT JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_with_qualified_col() {
        // eq_col also accepts fully qualified QualifiedCol
        let mut q = sqipe("users");
        q.join(
            "orders",
            table("users")
                .col("id")
                .eq_col(table("orders").col("user_id")),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\""
        );
    }

    #[test]
    fn test_multiple_joins() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.left_join("addresses", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\""
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_with_where() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"name\" = ?"
        );
        assert_eq!(binds, vec![Value::String("Alice".to_string())]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> WHERE \"name\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_with_qualified_where() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(table("orders").col("status").eq("shipped"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"orders\".\"status\" = ?"
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> WHERE \"orders\".\"status\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_with_mixed_where() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(("name", "Alice"));
        q.and_where(table("orders").col("amount").gt(100));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"name\" = ? AND \"orders\".\"amount\" > ?"
        );
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(100)]
        );
    }

    #[test]
    fn test_join_with_aggregate_and_having() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.aggregate(&[aggregate::count_all().as_("cnt")]);
        q.group_by(&["name"]);
        q.and_where(col("cnt").gt(5));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"name\", COUNT(*) AS \"cnt\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" GROUP BY \"name\" HAVING \"cnt\" > ?"
        );
        assert_eq!(binds, vec![Value::Int(5)]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" |> AGGREGATE COUNT(*) AS \"cnt\" GROUP BY \"name\" |> WHERE \"cnt\" > ?"
        );
    }

    #[test]
    fn test_in_range_to_inclusive() {
        let mut q = sqipe("employee");
        q.and_where(col("age").in_range(..=30));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE \"age\" <= ?");
        assert_eq!(binds, vec![Value::Int(30)]);
    }
}
