use crate::column::Col;
use crate::raw_sql::RawSql;
use crate::value::Value;

/// A column reference in a JOIN condition, optionally qualified with a table name.
/// When `table` is `None`, the table name is inferred from the `join()` / `left_join()` call.
#[derive(Debug, Clone)]
pub struct JoinCol {
    pub table: Option<String>,
    pub col: String,
}

impl From<Col> for JoinCol {
    fn from(c: Col) -> Self {
        JoinCol {
            table: c.table,
            col: c.column,
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
pub enum JoinCondition<V: Clone = Value> {
    ColEq {
        left: Col,
        right: JoinCol,
    },
    And(Vec<JoinCondition<V>>),
    Using(Vec<String>),
    /// Raw SQL expression for arbitrary ON conditions (e.g., `"a.text LIKE b.pattern"`).
    Expr(RawSql<V>),
}

impl<V: Clone> JoinCondition<V> {
    /// Convert a `JoinCondition<Value>` into `JoinCondition<V>` for any `V`.
    pub fn from_default(cond: JoinCondition) -> Self {
        match cond {
            JoinCondition::ColEq { left, right } => JoinCondition::ColEq { left, right },
            JoinCondition::And(conditions) => JoinCondition::And(
                conditions
                    .into_iter()
                    .map(JoinCondition::from_default)
                    .collect(),
            ),
            JoinCondition::Using(cols) => JoinCondition::Using(cols),
            JoinCondition::Expr(raw) => {
                assert!(
                    raw.binds.is_empty(),
                    "Cannot convert JoinCondition::Expr with binds to a different value type"
                );
                JoinCondition::Expr(RawSql::new(raw.as_str()))
            }
        }
    }

    /// Transform all bind values in this condition.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> JoinCondition<U> {
        match self {
            JoinCondition::ColEq { left, right } => JoinCondition::ColEq { left, right },
            JoinCondition::And(conditions) => {
                JoinCondition::And(conditions.into_iter().map(|c| c.map_values(f)).collect())
            }
            JoinCondition::Using(cols) => JoinCondition::Using(cols),
            JoinCondition::Expr(raw) => JoinCondition::Expr(raw.map_values(f)),
        }
    }
}

/// Create a USING join condition with a single column.
pub fn using_col(col: &str) -> JoinCondition {
    JoinCondition::Using(vec![col.to_string()])
}

/// Create a USING join condition with multiple columns.
pub fn using_cols(cols: &[&str]) -> JoinCondition {
    JoinCondition::Using(cols.iter().map(|c| c.to_string()).collect())
}

/// Create a raw SQL ON condition for arbitrary join expressions.
///
/// The `raw` string is embedded directly into the SQL output without escaping.
/// **Never** interpolate user input into this string — doing so creates a SQL
/// injection vulnerability.
///
/// ```
/// use qbey::{qbey, join, RawSql, SelectQueryBuilder};
///
/// let mut q = qbey("texts");
/// q.join("patterns", join::on_expr(RawSql::new(r#""texts"."text" LIKE "patterns"."pattern""#)));
/// ```
pub fn on_expr<V: Clone>(raw: RawSql<V>) -> JoinCondition<V> {
    JoinCondition::Expr(raw)
}

/// JOIN type.
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    /// Dialect-specific join type (e.g., "STRAIGHT_JOIN" in MySQL).
    Custom(String),
}

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct JoinClause<V: Clone = Value> {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub condition: JoinCondition<V>,
}

impl<V: Clone> JoinClause<V> {
    /// Transform all bind values in this clause.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> JoinClause<U> {
        JoinClause {
            join_type: self.join_type,
            table: self.table,
            alias: self.alias,
            condition: self.condition.map_values(f),
        }
    }
}
