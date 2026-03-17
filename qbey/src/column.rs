use crate::join::{JoinCol, JoinCondition};
use crate::like::LikeExpression;
use crate::raw_sql::RawSql;
use crate::value::Op;
use crate::where_clause::{IntoIncluded, IntoRangeClause, WhereClause};

#[derive(Debug, Clone)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum OrderByClause {
    /// A column reference with a sort direction.
    Col { col: Col, dir: SortDir },
    /// A raw SQL expression rendered as-is (e.g., `"RAND()"`, `"id DESC NULLS FIRST"`).
    Expr(RawSql),
}

/// Backwards-compatible alias — `ColRef` is now just `Col`.
pub type ColRef = Col;

/// A table reference for building qualified column references and join targets.
#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub(crate) alias: Option<String>,
}

/// Create a table reference for qualified column names.
pub fn table(name: &str) -> TableRef {
    TableRef {
        name: name.to_string(),
        alias: None,
    }
}

impl TableRef {
    pub fn col(&self, col: &str) -> Col {
        Col {
            table: Some(self.name.clone()),
            column: col.to_string(),
            alias: None,
            aggregate: None,
        }
    }

    /// Create multiple qualified column references at once.
    pub fn cols(&self, cols: &[&str]) -> Vec<Col> {
        cols.iter()
            .map(|c| Col {
                table: Some(self.name.clone()),
                column: c.to_string(),
                alias: None,
                aggregate: None,
            })
            .collect()
    }

    pub fn as_(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }
}

/// A column reference, optionally qualified with a table name and/or aliased.
///
/// - `col("name")` creates an unqualified column.
/// - `table("users").col("name")` creates a table-qualified column.
/// - `col("name").as_("n")` creates an aliased column.
///
/// Both forms support the same set of methods (e.g., `eq`, `asc`, `eq_col`).
#[derive(Debug, Clone)]
pub struct Col {
    pub table: Option<String>,
    pub column: String,
    pub alias: Option<String>,
    /// When set, this Col represents an aggregate function call (e.g., `COUNT(*)`, `SUM("price")`).
    /// The inner `Option<Box<Col>>` is the function argument (`None` means `*`).
    pub(crate) aggregate: Option<(SelectFunc, Option<Box<Col>>)>,
}

/// Create a column reference.
pub fn col(name: &str) -> Col {
    Col {
        table: None,
        column: name.to_string(),
        alias: None,
        aggregate: None,
    }
}

impl From<&str> for Col {
    fn from(name: &str) -> Self {
        Col {
            table: None,
            column: name.to_string(),
            alias: None,
            aggregate: None,
        }
    }
}

/// Trait for types that can produce WHERE/HAVING conditions.
///
/// Implemented by [`Col`] and [`SelectItem`]. Adding a new condition method
/// here automatically makes it available on both types — no manual delegation
/// needed.
pub trait ConditionExpr: Sized {
    /// Convert this expression into a `Col` for use in WHERE/HAVING clauses.
    fn into_condition_col(self) -> Col;

    /// Generate an equality (`=`) condition.
    fn eq<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Eq,
            val,
        }
    }

    /// Generate an inequality (`!=`) condition.
    fn ne<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Ne,
            val,
        }
    }

    /// Generate a greater-than (`>`) condition.
    fn gt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Gt,
            val,
        }
    }

    /// Generate a less-than (`<`) condition.
    fn lt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Lt,
            val,
        }
    }

    /// Generate a greater-than-or-equal (`>=`) condition.
    fn gte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Gte,
            val,
        }
    }

    /// Generate a less-than-or-equal (`<=`) condition.
    fn lte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_condition_col(),
            op: Op::Lte,
            val,
        }
    }

    /// Generate a `LIKE` condition.
    ///
    /// Accepts a [`LikeExpression`] to ensure safe pattern construction:
    /// - `col("name").like(LikeExpression::contains("foo"))` → `"name" LIKE '%foo%' ESCAPE '\'`
    /// - `col("name").like(LikeExpression::starts_with("foo"))` → `"name" LIKE 'foo%' ESCAPE '\'`
    /// - `col("name").like(LikeExpression::ends_with("foo"))` → `"name" LIKE '%foo' ESCAPE '\'`
    ///
    /// User input is automatically escaped (`%` and `_` are treated as literals).
    /// The `ESCAPE` clause is derived from the [`LikeExpression`].
    fn like(self, expr: LikeExpression) -> WhereClause<String> {
        let val = expr.to_pattern();
        WhereClause::Like {
            col: self.into_condition_col(),
            expr,
            val,
        }
    }

    /// Generate a `NOT LIKE` condition.
    ///
    /// Accepts a [`LikeExpression`] to ensure safe pattern construction:
    /// - `col("name").not_like(LikeExpression::contains("foo"))` → `"name" NOT LIKE '%foo%' ESCAPE '\'`
    ///
    /// User input is automatically escaped (`%` and `_` are treated as literals).
    /// The `ESCAPE` clause is derived from the [`LikeExpression`].
    fn not_like(self, expr: LikeExpression) -> WhereClause<String> {
        let val = expr.to_pattern();
        WhereClause::NotLike {
            col: self.into_condition_col(),
            expr,
            val,
        }
    }

    /// Generate an `IN (...)` condition.
    ///
    /// Accepts a slice of values or a `&SelectQuery` for subqueries:
    /// - `col("id").included(&[1, 2, 3])` → `"id" IN (?, ?, ?)`
    /// - `col("id").included(sub_query)` → `"id" IN (SELECT ...)`
    ///
    /// When a value list is empty, this produces `1 = 0` (always false) instead of
    /// invalid SQL. If you need to distinguish "no filter" from "match nothing",
    /// check that the slice is non-empty before calling this method.
    fn included<V: Clone>(self, source: impl IntoIncluded<V>) -> WhereClause<V> {
        source.into_in_clause(self.into_condition_col())
    }

    /// Generate a `NOT IN (...)` condition.
    ///
    /// Accepts a slice of values or a `SelectQuery` for subqueries:
    /// - `col("id").not_included(&[1, 2, 3])` → `"id" NOT IN (?, ?, ?)`
    /// - `col("id").not_included(sub_query)` → `"id" NOT IN (SELECT ...)`
    ///
    /// When a value list is empty, this produces `1 = 1` (always true) instead of
    /// invalid SQL.
    fn not_included<V: Clone>(self, source: impl IntoIncluded<V>) -> WhereClause<V> {
        source.into_not_in_clause(self.into_condition_col())
    }

    /// Generate a `BETWEEN` condition.
    fn between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::Between {
            col: self.into_condition_col(),
            low,
            high,
        }
    }

    /// Generate a `NOT BETWEEN` condition.
    fn not_between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::NotBetween {
            col: self.into_condition_col(),
            low,
            high,
        }
    }

    /// Convert a Rust range into SQL conditions.
    ///
    /// - `20..=30` → `BETWEEN 20 AND 30`
    /// - `20..30`  → `col >= 20 AND col < 30`
    /// - `20..`    → `col >= 20`
    /// - `..30`    → `col < 30`
    /// - `..=30`   → `col <= 30`
    fn in_range<V: Clone>(self, range: impl IntoRangeClause<V>) -> WhereClause<V> {
        range.into_where_clause(self.into_condition_col())
    }
}

impl ConditionExpr for Col {
    fn into_condition_col(self) -> Col {
        self
    }
}

/// `SelectItem::Col` and `SelectItem::Function` variants are supported.
///
/// # Panics
///
/// Panics if called on a `SelectItem::Expr` variant, which cannot be safely
/// converted to a column reference. Use [`RawSql`](crate::RawSql) in
/// WHERE/HAVING clauses through other means instead.
impl ConditionExpr for SelectItem {
    fn into_condition_col(self) -> Col {
        match self {
            SelectItem::Col(col) => col,
            SelectItem::Function { func, col, .. } => Col {
                table: None,
                column: String::new(),
                alias: None,
                aggregate: Some((func, col.map(Box::new))),
            },
            SelectItem::Expr { .. } => {
                panic!("cannot convert raw Expr SelectItem to Col for WHERE/HAVING")
            }
        }
    }
}

impl Col {
    pub fn as_(mut self, alias: &str) -> Col {
        self.alias = Some(alias.to_string());
        self
    }

    /// Create a `COUNT(col)` aggregate expression.
    ///
    /// - `col("id").count()` → `COUNT("id")`
    /// - `table("users").col("id").count()` → `COUNT("users"."id")`
    /// - `col("id").count().as_("cnt")` → `COUNT("id") AS "cnt"`
    pub fn count(self) -> SelectItem {
        SelectItem::Function {
            func: SelectFunc::Count,
            col: Some(self),
            alias: None,
        }
    }

    /// Create a `SUM(col)` aggregate expression.
    ///
    /// - `col("price").sum()` → `SUM("price")`
    /// - `col("price").sum().as_("total")` → `SUM("price") AS "total"`
    pub fn sum(self) -> SelectItem {
        SelectItem::Function {
            func: SelectFunc::Sum,
            col: Some(self),
            alias: None,
        }
    }

    /// Create an `AVG(col)` aggregate expression.
    ///
    /// - `col("price").avg()` → `AVG("price")`
    /// - `col("price").avg().as_("avg_price")` → `AVG("price") AS "avg_price"`
    pub fn avg(self) -> SelectItem {
        SelectItem::Function {
            func: SelectFunc::Avg,
            col: Some(self),
            alias: None,
        }
    }

    /// Create a `MIN(col)` aggregate expression.
    ///
    /// - `col("price").min()` → `MIN("price")`
    /// - `col("price").min().as_("min_price")` → `MIN("price") AS "min_price"`
    pub fn min(self) -> SelectItem {
        SelectItem::Function {
            func: SelectFunc::Min,
            col: Some(self),
            alias: None,
        }
    }

    /// Create a `MAX(col)` aggregate expression.
    ///
    /// - `col("price").max()` → `MAX("price")`
    /// - `col("price").max().as_("max_price")` → `MAX("price") AS "max_price"`
    pub fn max(self) -> SelectItem {
        SelectItem::Function {
            func: SelectFunc::Max,
            col: Some(self),
            alias: None,
        }
    }

    pub fn asc(self) -> OrderByClause {
        OrderByClause::Col {
            col: self,
            dir: SortDir::Asc,
        }
    }

    pub fn desc(self) -> OrderByClause {
        OrderByClause::Col {
            col: self,
            dir: SortDir::Desc,
        }
    }

    /// Create a JOIN ON condition comparing two columns.
    ///
    /// When `self` has no table qualifier (created via `col()`), the left side
    /// is rendered without a table prefix (e.g., `"id" = "orders"."user_id"`).
    pub fn eq_col(self, other: impl Into<JoinCol>) -> JoinCondition {
        JoinCondition::ColEq {
            left: self,
            right: other.into(),
        }
    }
}

/// An item in a SELECT list — either a column reference or a raw SQL expression.
#[derive(Debug, Clone)]
pub enum SelectItem {
    /// A column reference (optionally table-qualified and/or aliased).
    Col(Col),
    /// A raw SQL expression (e.g., `"COUNT(*)"`, `"price * quantity"`).
    ///
    /// **Warning:** `raw` is embedded into SQL without escaping.
    /// Never pass user-supplied input — see [`SelectQuery::add_select_expr`](crate::SelectQuery::add_select_expr).
    Expr {
        raw: crate::raw_sql::RawSql,
        alias: Option<String>,
    },
    /// A function applied to a column (e.g., `COUNT("id")`, `SUM("price")`).
    Function {
        func: SelectFunc,
        col: Option<Col>,
        alias: Option<String>,
    },
}

/// Supported SQL functions for SELECT items.
#[derive(Debug, Clone)]
pub enum SelectFunc {
    /// `COUNT(col)` or `COUNT(*)`.
    Count,
    /// `COUNT(1)`.
    CountOne,
    /// `SUM(col)`.
    Sum,
    /// `AVG(col)`.
    Avg,
    /// `MIN(col)`.
    Min,
    /// `MAX(col)`.
    Max,
}

impl SelectFunc {
    pub fn as_str(&self) -> &'static str {
        match self {
            SelectFunc::Count => "COUNT",
            SelectFunc::CountOne => "COUNT",
            SelectFunc::Sum => "SUM",
            SelectFunc::Avg => "AVG",
            SelectFunc::Min => "MIN",
            SelectFunc::Max => "MAX",
        }
    }
}

/// Create a `COUNT(*)` expression.
///
/// - `count_all()` → `COUNT(*)`
/// - `count_all().as_("cnt")` → `COUNT(*) AS "cnt"`
pub fn count_all() -> SelectItem {
    SelectItem::Function {
        func: SelectFunc::Count,
        col: None,
        alias: None,
    }
}

/// Create a `COUNT(1)` expression.
///
/// - `count_one()` → `COUNT(1)`
/// - `count_one().as_("cnt")` → `COUNT(1) AS "cnt"`
pub fn count_one() -> SelectItem {
    SelectItem::Function {
        func: SelectFunc::CountOne,
        col: None,
        alias: None,
    }
}

impl SelectItem {
    /// Add an alias to this select item.
    ///
    /// - `col("id").count().as_("cnt")` → `COUNT("id") AS "cnt"`
    pub fn as_(mut self, alias: &str) -> SelectItem {
        match &mut self {
            SelectItem::Col(col) => {
                col.alias = Some(alias.to_string());
            }
            SelectItem::Expr { alias: a, .. } => {
                *a = Some(alias.to_string());
            }
            SelectItem::Function { alias: a, .. } => {
                *a = Some(alias.to_string());
            }
        }
        self
    }
}

impl From<Col> for SelectItem {
    fn from(col: Col) -> Self {
        SelectItem::Col(col)
    }
}

impl<'a> From<&'a str> for SelectItem {
    fn from(s: &'a str) -> Self {
        SelectItem::Col(Col {
            table: None,
            column: s.to_string(),
            alias: None,
            aggregate: None,
        })
    }
}
