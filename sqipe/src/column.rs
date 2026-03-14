use crate::join::{JoinCol, JoinCondition};
use crate::like::LikeExpression;
use crate::value::Op;
use crate::where_clause::{IntoIncluded, IntoRangeClause, WhereClause};

#[derive(Debug, Clone)]
pub enum SortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub col: Col,
    pub dir: SortDir,
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
        }
    }

    /// Create multiple qualified column references at once.
    pub fn cols(&self, cols: &[&str]) -> Vec<Col> {
        cols.iter()
            .map(|c| Col {
                table: Some(self.name.clone()),
                column: c.to_string(),
                alias: None,
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
}

/// Create a column reference.
pub fn col(name: &str) -> Col {
    Col {
        table: None,
        column: name.to_string(),
        alias: None,
    }
}

impl Col {
    pub fn eq<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
            op: Op::Eq,
            val,
        }
    }

    pub fn ne<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
            op: Op::Ne,
            val,
        }
    }

    pub fn gt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
            op: Op::Gt,
            val,
        }
    }

    pub fn lt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
            op: Op::Lt,
            val,
        }
    }

    pub fn gte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
            op: Op::Gte,
            val,
        }
    }

    pub fn lte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self,
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
    pub fn like(self, expr: LikeExpression) -> WhereClause<String> {
        let val = expr.to_pattern();
        WhereClause::Like {
            col: self,
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
    pub fn not_like(self, expr: LikeExpression) -> WhereClause<String> {
        let val = expr.to_pattern();
        WhereClause::NotLike {
            col: self,
            expr,
            val,
        }
    }

    /// Generate an `IN (...)` condition.
    ///
    /// Accepts a slice of values or a `&Query` for subqueries:
    /// - `col("id").included(&[1, 2, 3])` → `"id" IN (?, ?, ?)`
    /// - `col("id").included(sub_query)` → `"id" IN (SELECT ...)`
    ///
    /// When a value list is empty, this produces `1 = 0` (always false) instead of
    /// invalid SQL. If you need to distinguish "no filter" from "match nothing",
    /// check that the slice is non-empty before calling this method.
    pub fn included<V: Clone>(self, source: impl IntoIncluded<V>) -> WhereClause<V> {
        source.into_in_clause(self)
    }

    /// Generate a `NOT IN (...)` condition.
    ///
    /// Accepts a slice of values or a `Query` for subqueries:
    /// - `col("id").not_included(&[1, 2, 3])` → `"id" NOT IN (?, ?, ?)`
    /// - `col("id").not_included(sub_query)` → `"id" NOT IN (SELECT ...)`
    ///
    /// When a value list is empty, this produces `1 = 1` (always true) instead of
    /// invalid SQL.
    pub fn not_included<V: Clone>(self, source: impl IntoIncluded<V>) -> WhereClause<V> {
        source.into_not_in_clause(self)
    }

    pub fn between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::Between {
            col: self,
            low,
            high,
        }
    }

    pub fn not_between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::NotBetween {
            col: self,
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
    pub fn in_range<V: Clone>(self, range: impl IntoRangeClause<V>) -> WhereClause<V> {
        range.into_where_clause(self)
    }

    pub fn as_(mut self, alias: &str) -> Col {
        self.alias = Some(alias.to_string());
        self
    }

    pub fn asc(self) -> OrderByClause {
        OrderByClause {
            col: self,
            dir: SortDir::Asc,
        }
    }

    pub fn desc(self) -> OrderByClause {
        OrderByClause {
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
    /// Never pass user-supplied input — see [`Query::add_select_expr`](crate::Query::add_select_expr).
    Expr { raw: String, alias: Option<String> },
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
        })
    }
}

/// Backwards-compatible alias for `Col`.
pub type QualifiedCol = Col;
