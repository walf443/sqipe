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
    pub col: ColRef,
    pub dir: SortDir,
}

/// Column reference — simple, table-qualified, or aliased.
#[derive(Debug, Clone)]
pub enum ColRef {
    Simple(String),
    Qualified { table: String, col: String },
    Aliased { col: Box<ColRef>, alias: String },
}

/// Trait for converting into a `ColRef`.
pub trait IntoColRef {
    fn into_col_ref(self) -> ColRef;
}

impl IntoColRef for ColRef {
    fn into_col_ref(self) -> ColRef {
        self
    }
}

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
        }
    }

    /// Create multiple qualified column references at once.
    pub fn cols(&self, cols: &[&str]) -> Vec<ColRef> {
        cols.iter()
            .map(|c| ColRef::Qualified {
                table: self.name.clone(),
                col: c.to_string(),
            })
            .collect()
    }

    pub fn as_(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }
}

/// A column reference, optionally qualified with a table name.
///
/// - `col("name")` creates an unqualified column.
/// - `table("users").col("name")` creates a table-qualified column.
///
/// Both forms support the same set of methods (e.g., `eq`, `asc`, `eq_col`).
#[derive(Debug, Clone)]
pub struct Col {
    pub table: Option<String>,
    pub column: String,
}

/// Create a column reference.
pub fn col(name: &str) -> Col {
    Col {
        table: None,
        column: name.to_string(),
    }
}

impl IntoColRef for Col {
    fn into_col_ref(self) -> ColRef {
        match self.table {
            Some(table) => ColRef::Qualified {
                table,
                col: self.column,
            },
            None => ColRef::Simple(self.column),
        }
    }
}

impl Col {
    pub fn eq<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
            op: Op::Eq,
            val,
        }
    }

    pub fn ne<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
            op: Op::Ne,
            val,
        }
    }

    pub fn gt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
            op: Op::Gt,
            val,
        }
    }

    pub fn lt<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
            op: Op::Lt,
            val,
        }
    }

    pub fn gte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
            op: Op::Gte,
            val,
        }
    }

    pub fn lte<V: Clone>(self, val: V) -> WhereClause<V> {
        WhereClause::Condition {
            col: self.into_col_ref(),
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
            col: self.into_col_ref(),
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
            col: self.into_col_ref(),
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
        source.into_in_clause(self.into_col_ref())
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
        source.into_not_in_clause(self.into_col_ref())
    }

    pub fn between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::Between {
            col: self.into_col_ref(),
            low,
            high,
        }
    }

    pub fn not_between<V: Clone>(self, low: V, high: V) -> WhereClause<V> {
        WhereClause::NotBetween {
            col: self.into_col_ref(),
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
        range.into_where_clause(self.into_col_ref())
    }

    pub fn as_(self, alias: &str) -> ColRef {
        ColRef::Aliased {
            col: Box::new(self.into_col_ref()),
            alias: alias.to_string(),
        }
    }

    pub fn asc(self) -> OrderByClause {
        OrderByClause {
            col: self.into_col_ref(),
            dir: SortDir::Asc,
        }
    }

    pub fn desc(self) -> OrderByClause {
        OrderByClause {
            col: self.into_col_ref(),
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

/// Backwards-compatible alias for `Col`.
pub type QualifiedCol = Col;
