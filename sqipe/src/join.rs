use crate::column::Col;

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
pub enum JoinCondition {
    ColEq {
        left: Col,
        right: JoinCol,
    },
    And(Vec<JoinCondition>),
    Using(Vec<String>),
    /// Raw SQL expression for arbitrary ON conditions (e.g., `"a.text LIKE b.pattern"`).
    Expr(String),
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
/// use sqipe::{sqipe, join};
///
/// let mut q = sqipe("texts");
/// q.join("patterns", join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#));
/// ```
pub fn on_expr(raw: &str) -> JoinCondition {
    JoinCondition::Expr(raw.to_string())
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
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub condition: JoinCondition,
}
