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

/// A safe LIKE pattern expression.
///
/// Wildcards (`%`, `_`) in user input are escaped automatically, so the
/// resulting pattern matches the literal text.  Use the constructor methods
/// to add wildcards in controlled positions.
///
/// ```
/// use sqipe::LikeExpression;
///
/// assert_eq!(LikeExpression::contains("foo").to_pattern(), "%foo%");
/// assert_eq!(LikeExpression::starts_with("foo").to_pattern(), "foo%");
/// assert_eq!(LikeExpression::ends_with("foo").to_pattern(), "%foo");
/// assert_eq!(LikeExpression::contains("100%").to_pattern(), "%100\\%%");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct LikeExpression {
    pattern: String,
    escape_char: char,
}

impl LikeExpression {
    const DEFAULT_ESCAPE: char = '\\';

    /// Panics if `esc` is a LIKE wildcard (`%`, `_`) or a single quote (`'`).
    fn validate_escape_char(esc: char) {
        assert!(
            esc != '%' && esc != '_' && esc != '\'',
            "escape character must not be '%', '_', or '\\'' (got '{}')",
            esc
        );
    }

    /// Escape LIKE wildcards in user input using the given escape character.
    fn escape_with(input: &str, esc: char) -> String {
        let esc_s = esc.to_string();
        input
            .replace(&esc_s, &format!("{}{}", esc, esc))
            .replace('%', &format!("{}%", esc))
            .replace('_', &format!("{}_", esc))
    }

    /// Match rows that contain the given text anywhere.
    ///
    /// `LikeExpression::contains("foo")` → pattern `%foo%`
    pub fn contains(input: &str) -> Self {
        Self::contains_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that contain the given text anywhere, using a custom escape character.
    ///
    /// `LikeExpression::contains_escaped_by('!', "foo")` → pattern `%foo%`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn contains_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("%{}%", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Match rows that start with the given text.
    ///
    /// `LikeExpression::starts_with("foo")` → pattern `foo%`
    pub fn starts_with(input: &str) -> Self {
        Self::starts_with_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that start with the given text, using a custom escape character.
    ///
    /// `LikeExpression::starts_with_escaped_by('!', "foo")` → pattern `foo%`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn starts_with_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("{}%", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Match rows that end with the given text.
    ///
    /// `LikeExpression::ends_with("foo")` → pattern `%foo`
    pub fn ends_with(input: &str) -> Self {
        Self::ends_with_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that end with the given text, using a custom escape character.
    ///
    /// `LikeExpression::ends_with_escaped_by('!', "foo")` → pattern `%foo`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn ends_with_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("%{}", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Return the constructed LIKE pattern string.
    pub fn to_pattern(&self) -> String {
        self.pattern.clone()
    }

    /// Return the escape character used in this expression.
    pub fn escape_char(&self) -> char {
        self.escape_char
    }
}

/// A table reference for building qualified column references and join targets.
#[derive(Debug, Clone)]
pub struct TableRef {
    name: String,
    alias: Option<String>,
}

/// Create a table reference for qualified column names.
pub fn table(name: &str) -> TableRef {
    TableRef {
        name: name.to_string(),
        alias: None,
    }
}

impl TableRef {
    pub fn col(&self, col: &str) -> QualifiedCol {
        QualifiedCol {
            table: self.name.clone(),
            col: col.to_string(),
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
    ColEq {
        left: QualifiedCol,
        right: JoinCol,
    },
    And(Vec<JoinCondition>),
    Using(Vec<String>),
    /// Raw SQL expression for arbitrary ON conditions (e.g., `"a.text LIKE b.pattern"`).
    Expr(String),
}

/// JOIN condition helpers.
pub mod join {
    use super::JoinCondition;

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

macro_rules! impl_col_methods {
    ($ty:ty) => {
        impl $ty {
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
    pub fn as_(self, alias: &str) -> ColRef {
        ColRef::Aliased {
            col: Box::new(self.into_col_ref()),
            alias: alias.to_string(),
        }
    }

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
    pub fn as_(self, alias: &str) -> ColRef {
        ColRef::Aliased {
            col: Box::new(self.into_col_ref()),
            alias: alias.to_string(),
        }
    }

    pub fn eq_col(self, other: impl Into<JoinCol>) -> JoinCondition {
        JoinCondition::ColEq {
            left: self,
            right: other.into(),
        }
    }
}

/// A WHERE condition tree, generic over the bind value type.
///
/// `Debug` is implemented manually (not derived) because `InSubQuery` contains
/// `SelectTree<V>` which requires `V: Clone`. The derive macro would only add
/// `V: Debug`, but we also need `V: Clone` for the enum definition itself.
#[derive(Clone)]
pub enum WhereClause<V: Clone = Value> {
    Condition {
        col: ColRef,
        op: Op,
        val: V,
    },
    Between {
        col: ColRef,
        low: V,
        high: V,
    },
    NotBetween {
        col: ColRef,
        low: V,
        high: V,
    },
    In {
        col: ColRef,
        vals: Vec<V>,
    },
    InSubQuery {
        col: ColRef,
        sub: Box<tree::SelectTree<V>>,
    },
    NotIn {
        col: ColRef,
        vals: Vec<V>,
    },
    NotInSubQuery {
        col: ColRef,
        sub: Box<tree::SelectTree<V>>,
    },
    Like {
        col: ColRef,
        /// Preserved for ESCAPE clause rendering.
        expr: LikeExpression,
        /// Bind parameter value (always `expr.to_pattern()` at construction).
        val: V,
    },
    NotLike {
        col: ColRef,
        /// Preserved for ESCAPE clause rendering.
        expr: LikeExpression,
        /// Bind parameter value (always `expr.to_pattern()` at construction).
        val: V,
    },
    Any(Vec<WhereClause<V>>),
    All(Vec<WhereClause<V>>),
    Not(Box<WhereClause<V>>),
}

impl<V: Clone + std::fmt::Debug> std::fmt::Debug for WhereClause<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WhereClause::Condition { col, op, val } => f
                .debug_struct("Condition")
                .field("col", col)
                .field("op", op)
                .field("val", val)
                .finish(),
            WhereClause::Between { col, low, high } => f
                .debug_struct("Between")
                .field("col", col)
                .field("low", low)
                .field("high", high)
                .finish(),
            WhereClause::NotBetween { col, low, high } => f
                .debug_struct("NotBetween")
                .field("col", col)
                .field("low", low)
                .field("high", high)
                .finish(),
            WhereClause::In { col, vals } => f
                .debug_struct("In")
                .field("col", col)
                .field("vals", vals)
                .finish(),
            WhereClause::InSubQuery { col, sub } => f
                .debug_struct("InSubQuery")
                .field("col", col)
                .field("sub", sub)
                .finish(),
            WhereClause::NotIn { col, vals } => f
                .debug_struct("NotIn")
                .field("col", col)
                .field("vals", vals)
                .finish(),
            WhereClause::NotInSubQuery { col, sub } => f
                .debug_struct("NotInSubQuery")
                .field("col", col)
                .field("sub", sub)
                .finish(),
            WhereClause::Like { col, expr, val } => f
                .debug_struct("Like")
                .field("col", col)
                .field("expr", expr)
                .field("val", val)
                .finish(),
            WhereClause::NotLike { col, expr, val } => f
                .debug_struct("NotLike")
                .field("col", col)
                .field("expr", expr)
                .field("val", val)
                .finish(),
            WhereClause::Any(clauses) => f.debug_tuple("Any").field(clauses).finish(),
            WhereClause::All(clauses) => f.debug_tuple("All").field(clauses).finish(),
            WhereClause::Not(clause) => f.debug_tuple("Not").field(clause).finish(),
        }
    }
}

impl<V: Clone> std::ops::Not for WhereClause<V> {
    type Output = WhereClause<V>;

    fn not(self) -> Self::Output {
        WhereClause::Not(Box::new(self))
    }
}

impl<V: Clone> WhereClause<V> {
    /// Transform all bind values in this clause.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> WhereClause<U> {
        match self {
            WhereClause::Condition { col, op, val } => WhereClause::Condition {
                col,
                op,
                val: f(val),
            },
            WhereClause::Between { col, low, high } => WhereClause::Between {
                col,
                low: f(low),
                high: f(high),
            },
            WhereClause::NotBetween { col, low, high } => WhereClause::NotBetween {
                col,
                low: f(low),
                high: f(high),
            },
            WhereClause::In { col, vals } => WhereClause::In {
                col,
                vals: vals.into_iter().map(f).collect(),
            },
            WhereClause::InSubQuery { col, sub } => WhereClause::InSubQuery {
                col,
                sub: Box::new(sub.map_values(f)),
            },
            WhereClause::NotIn { col, vals } => WhereClause::NotIn {
                col,
                vals: vals.into_iter().map(f).collect(),
            },
            WhereClause::NotInSubQuery { col, sub } => WhereClause::NotInSubQuery {
                col,
                sub: Box::new(sub.map_values(f)),
            },
            WhereClause::Like { col, expr, val } => WhereClause::Like {
                col,
                expr,
                val: f(val),
            },
            WhereClause::NotLike { col, expr, val } => WhereClause::NotLike {
                col,
                expr,
                val: f(val),
            },
            WhereClause::Any(clauses) => {
                WhereClause::Any(clauses.into_iter().map(|c| c.map_values(f)).collect())
            }
            WhereClause::All(clauses) => {
                WhereClause::All(clauses.into_iter().map(|c| c.map_values(f)).collect())
            }
            WhereClause::Not(clause) => WhereClause::Not(Box::new(clause.map_values(f))),
        }
    }
}

/// Trait for types that can be converted into a `WhereClause<V>`.
pub trait IntoWhereClause<V: Clone> {
    fn into_where_clause(self) -> WhereClause<V>;
}

/// Convert `WhereClause<T>` to `WhereClause<V>` when `T: Into<V>`.
impl<V: Clone, T: Clone + Into<V>> IntoWhereClause<V> for WhereClause<T> {
    fn into_where_clause(self) -> WhereClause<V> {
        self.map_values(&|v| v.into())
    }
}

/// Tuple shorthand: `("name", value)` becomes `col = value`.
impl<V: Clone, T: Into<V>> IntoWhereClause<V> for (&str, T) {
    fn into_where_clause(self) -> WhereClause<V> {
        WhereClause::Condition {
            col: ColRef::Simple(self.0.to_string()),
            op: Op::Eq,
            val: self.1.into(),
        }
    }
}

/// Trait for converting Rust range types into WhereClause.
pub trait IntoRangeClause<V: Clone> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V>;
}

use std::ops::{Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

/// `20..=30` → `col BETWEEN 20 AND 30`
impl<V: Clone> IntoRangeClause<V> for RangeInclusive<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V> {
        let (low, high) = self.into_inner();
        WhereClause::Between { col, low, high }
    }
}

/// `20..30` → `col >= 20 AND col < 30`
impl<V: Clone> IntoRangeClause<V> for Range<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::All(vec![
            WhereClause::Condition {
                col: col.clone(),
                op: Op::Gte,
                val: self.start,
            },
            WhereClause::Condition {
                col,
                op: Op::Lt,
                val: self.end,
            },
        ])
    }
}

/// `20..` → `col >= 20`
impl<V: Clone> IntoRangeClause<V> for RangeFrom<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Gte,
            val: self.start,
        }
    }
}

/// `..30` → `col < 30`
impl<V: Clone> IntoRangeClause<V> for RangeTo<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Lt,
            val: self.end,
        }
    }
}

/// `..=30` → `col <= 30`
impl<V: Clone> IntoRangeClause<V> for RangeToInclusive<V> {
    fn into_where_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Lte,
            val: self.end,
        }
    }
}

/// Trait for types that can be converted into a `SelectTree` for use as a FROM subquery.
///
/// Implement this trait to allow your custom query type (e.g., `MysqlQuery`)
/// to be passed to `sqipe_from_subquery_with()`.
pub trait IntoSelectTree<V: Clone> {
    /// Consume this query and produce a `SelectTree` AST node.
    fn into_select_tree(self) -> tree::SelectTree<V>;
}

impl<V: Clone + std::fmt::Debug> IntoSelectTree<V> for Query<V> {
    fn into_select_tree(self) -> tree::SelectTree<V> {
        tree::SelectTree::from_query_owned(self)
    }
}

/// Trait for types that can be used as a source for `included` (IN clause).
///
/// Implemented for slices (value list) and `Query` (subquery).
pub trait IntoIncluded<V: Clone> {
    fn into_in_clause(self, col: ColRef) -> WhereClause<V>;
    fn into_not_in_clause(self, col: ColRef) -> WhereClause<V>;
}

impl<V: Clone> IntoIncluded<V> for &[V] {
    fn into_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::In {
            col,
            vals: self.to_vec(),
        }
    }

    fn into_not_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::NotIn {
            col,
            vals: self.to_vec(),
        }
    }
}

impl<V: Clone, const N: usize> IntoIncluded<V> for &[V; N] {
    fn into_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::In {
            col,
            vals: self.to_vec(),
        }
    }

    fn into_not_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::NotIn {
            col,
            vals: self.to_vec(),
        }
    }
}

/// `Debug` bound comes from `Query<V>` requiring `V: Debug`, not from this impl itself.
impl<V: Clone + std::fmt::Debug> IntoIncluded<V> for Query<V> {
    fn into_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::InSubQuery {
            col,
            sub: Box::new(tree::SelectTree::from_query_owned(self)),
        }
    }

    fn into_not_in_clause(self, col: ColRef) -> WhereClause<V> {
        WhereClause::NotInSubQuery {
            col,
            sub: Box::new(tree::SelectTree::from_query_owned(self)),
        }
    }
}

/// Combine conditions with OR: `any(a, b)` => `(a OR b)`.
pub fn any<V: Clone>(a: WhereClause<V>, b: WhereClause<V>) -> WhereClause<V> {
    WhereClause::Any(vec![a, b])
}

/// Combine conditions with AND: `all(a, b)` => `(a AND b)`.
pub fn all<V: Clone>(a: WhereClause<V>, b: WhereClause<V>) -> WhereClause<V> {
    WhereClause::All(vec![a, b])
}

/// Negate a condition: `not(a)` => `NOT (a)`.
pub fn not<V: Clone>(clause: WhereClause<V>) -> WhereClause<V> {
    WhereClause::Not(Box::new(clause))
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

    /// Whether backslashes must be doubled inside SQL string literals.
    /// MySQL requires this by default (when `NO_BACKSLASH_ESCAPES` is not set).
    fn backslash_escape(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WhereEntry<V: Clone = Value> {
    And(WhereClause<V>),
    Or(WhereClause<V>),
}

impl<V: Clone> WhereEntry<V> {
    pub(crate) fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> WhereEntry<U> {
        match self {
            WhereEntry::And(c) => WhereEntry::And(c.map_values(f)),
            WhereEntry::Or(c) => WhereEntry::Or(c.map_values(f)),
        }
    }
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
pub trait UnionQueryOps<V: Clone + std::fmt::Debug = Value>: AsUnionParts {
    fn union<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn union_all<T: AsUnionParts<Query = Self::Query>>(&mut self, other: &T) -> &mut Self;
    fn order_by(&mut self, clause: OrderByClause) -> &mut Self;
    fn limit(&mut self, n: u64) -> &mut Self;
    fn offset(&mut self, n: u64) -> &mut Self;
    fn to_sql(&self) -> (String, Vec<V>);
    fn to_pipe_sql(&self) -> (String, Vec<V>);
}

pub mod renderer;
pub mod tree;

use renderer::pipe::PipeSqlRenderer;
use renderer::standard::StandardSqlRenderer;
use renderer::{RenderConfig, Renderer};
use tree::{SelectTree, UnionTree, default_quote_identifier};

/// The query builder, generic over the bind value type `V`.
#[derive(Debug, Clone)]
pub struct Query<V: Clone + std::fmt::Debug = Value> {
    /// Table name for table-based queries. Empty when using `from_subquery`.
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    /// When set, the query selects from this subquery instead of `table`.
    pub(crate) from_subquery: Option<Box<tree::SelectTree<V>>>,
    pub(crate) selects: Vec<ColRef>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub(crate) aggregates: Vec<AggregateExpr>,
    pub(crate) group_bys: Vec<String>,
    pub(crate) joins: Vec<JoinClause>,
    /// Subquery sources for joins, aligned with `joins` by index.
    pub(crate) join_subqueries: Vec<Option<Box<tree::SelectTree<V>>>>,
    pub(crate) order_bys: Vec<OrderByClause>,
    pub(crate) limit_val: Option<u64>,
    pub(crate) offset_val: Option<u64>,
    /// Records the order of WHERE and JOIN operations for CTE generation.
    pub(crate) stage_order: Vec<tree::StageRef>,
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
pub fn sqipe(table: &str) -> Query<Value> {
    Query::new(table)
}

/// Create a new query builder with a custom value type.
pub fn sqipe_with<V: Clone + std::fmt::Debug>(table: &str) -> Query<V> {
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

/// Trait for types that can specify a join target table.
pub trait IntoJoinTable {
    fn into_join_table(self) -> (String, Option<String>);
}

impl IntoJoinTable for &str {
    fn into_join_table(self) -> (String, Option<String>) {
        (self.to_string(), None)
    }
}

impl IntoJoinTable for TableRef {
    fn into_join_table(self) -> (String, Option<String>) {
        (self.name, self.alias)
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
    pub fn new(table: &str) -> Self {
        Query {
            table: table.to_string(),
            table_alias: None,
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
                .push(tree::StageRef::Where(self.wheres.len()));
            self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        } else {
            self.havings.push(WhereEntry::And(cond.into_where_clause()));
        }
        self
    }

    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        if self.aggregates.is_empty() {
            self.stage_order
                .push(tree::StageRef::Where(self.wheres.len()));
            self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        } else {
            self.havings.push(WhereEntry::Or(cond.into_where_clause()));
        }
        self
    }

    pub fn select(&mut self, cols: &[&str]) -> &mut Self {
        self.selects = cols.iter().map(|s| ColRef::Simple(s.to_string())).collect();
        self
    }

    /// Set select columns from `ColRef` values (e.g., from `table("o").cols(&["id"])`).
    pub fn select_cols(&mut self, cols: &[ColRef]) -> &mut Self {
        self.selects = cols.to_vec();
        self
    }

    /// Append a single column to the select list.
    pub fn add_select(&mut self, col: impl IntoColRef) -> &mut Self {
        self.selects.push(col.into_col_ref());
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
            .push(tree::StageRef::Join(self.joins.len()));
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
            .push(tree::StageRef::Join(self.joins.len()));
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
            .push(tree::StageRef::Join(self.joins.len()));
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
            .push(tree::StageRef::Join(self.joins.len()));
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

/// An UPDATE query builder, generic over the bind value type `V`.
///
/// Created via [`Query::update()`] to convert a SELECT query builder into an UPDATE statement.
///
/// By default, WHERE clause is required. Calling `to_sql()` or `to_sql_with()` without
/// any WHERE conditions will panic to prevent accidental full-table updates.
/// Use [`allow_without_where()`](UpdateQuery::allow_without_where) to explicitly allow WHERE-less updates.
///
/// ```
/// use sqipe::{sqipe, col};
///
/// let mut u = sqipe("employee").update();
/// u.set(col("name"), "Alice");
/// u.and_where(col("id").eq(1));
/// let (sql, _) = u.to_sql();
/// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
/// ```
/// A raw SQL expression for use in SET clauses.
///
/// This type exists to make it explicit that the caller is injecting raw SQL.
/// The expression is inserted verbatim — it is **not** parameterized or quoted.
///
/// ```
/// use sqipe::{sqipe, col, SetExpression};
///
/// let mut u = sqipe("employee").update();
/// u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
/// u.and_where(col("id").eq(1));
/// let (sql, _) = u.to_sql();
/// assert_eq!(sql, r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#);
/// ```
#[derive(Debug, Clone)]
pub struct SetExpression(String);

impl SetExpression {
    /// Create a new raw SQL SET expression.
    pub fn new(expr: &str) -> Self {
        Self(expr.to_string())
    }
}

/// A single SET clause entry in an UPDATE statement.
#[derive(Debug, Clone)]
pub enum SetClause<V: Clone> {
    /// `"col" = ?` — identifier-quoted column with a bind value.
    Value(String, V),
    /// Raw SQL expression via [`SetExpression`].
    Expr(SetExpression),
}

#[derive(Debug, Clone)]
pub struct UpdateQuery<V: Clone + std::fmt::Debug = Value> {
    table: String,
    table_alias: Option<String>,
    sets: Vec<SetClause<V>>,
    wheres: Vec<WhereEntry<V>>,
    allow_without_where: bool,
}

impl<V: Clone + std::fmt::Debug> UpdateQuery<V> {
    /// Add a SET clause: `SET "col" = ?`.
    ///
    /// Use [`col()`] to create a column reference for the first argument.
    /// Column names are quoted as identifiers but **not** parameterized,
    /// so never pass external (user-supplied) input as a column name.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut u = sqipe("employee").update();
    /// u.set(col("name"), "Alice");
    /// u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    pub fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self {
        self.sets.push(SetClause::Value(col.name, val.into()));
        self
    }

    /// Add a raw SQL expression to the SET clause.
    ///
    /// Use [`SetExpression::new()`] to create the expression, making it explicit
    /// that raw SQL is being injected.
    ///
    /// ```
    /// use sqipe::{sqipe, col, SetExpression};
    ///
    /// let mut u = sqipe("employee").update();
    /// u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
    /// u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#);
    /// ```
    pub fn set_expr(&mut self, expr: SetExpression) -> &mut Self {
        self.sets.push(SetClause::Expr(expr));
        self
    }

    /// Add an AND WHERE condition.
    pub fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    /// Explicitly allow this UPDATE to have no WHERE clause.
    ///
    /// By default, `to_sql()` and `to_sql_with()` panic if no WHERE conditions are set,
    /// to prevent accidental full-table updates. Call this method to opt in to WHERE-less updates.
    ///
    /// ```
    /// use sqipe::{sqipe, col};
    ///
    /// let mut u = sqipe("employee").update();
    /// u.set(col("status"), "inactive");
    /// u.allow_without_where();
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "status" = ?"#);
    /// ```
    pub fn allow_without_where(&mut self) -> &mut Self {
        self.allow_without_where = true;
        self
    }

    /// Build an UpdateTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_tree(&self) -> tree::UpdateTree<V> {
        self.assert_where_present();
        tree::UpdateTree {
            table: self.table.clone(),
            table_alias: self.table_alias.clone(),
            sets: self.sets.clone(),
            wheres: self.wheres.clone(),
            order_bys: Vec::new(),
            limit: None,
        }
    }

    fn assert_where_present(&self) {
        assert!(
            self.allow_without_where || !self.wheres.is_empty(),
            "UPDATE without WHERE is dangerous and not allowed by default. \
             Use .allow_without_where() to explicitly allow full-table updates."
        );
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        renderer::update::render_update(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        renderer::update::render_update(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
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
    /// let mut u = q.update();
    /// u.set(col("name"), "Alice");
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    pub fn update(self) -> UpdateQuery<V> {
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
        UpdateQuery {
            table: self.table,
            table_alias: self.table_alias,
            sets: Vec::new(),
            wheres: self.wheres,
            allow_without_where: false,
        }
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
    /// let d = q.delete();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
    /// ```
    pub fn delete(self) -> DeleteQuery<V> {
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
        DeleteQuery {
            table: self.table,
            table_alias: self.table_alias,
            wheres: self.wheres,
            allow_without_where: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeleteQuery<V: Clone + std::fmt::Debug = Value> {
    table: String,
    table_alias: Option<String>,
    wheres: Vec<WhereEntry<V>>,
    allow_without_where: bool,
}

impl<V: Clone + std::fmt::Debug> DeleteQuery<V> {
    /// Add an AND WHERE condition.
    pub fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    /// Add an OR WHERE condition.
    pub fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    /// Explicitly allow this DELETE to have no WHERE clause.
    ///
    /// By default, `to_sql()` and `to_sql_with()` panic if no WHERE conditions are set,
    /// to prevent accidental full-table deletes. Call this method to opt in to WHERE-less deletes.
    ///
    /// ```
    /// use sqipe::sqipe;
    ///
    /// let mut d = sqipe("employee").delete();
    /// d.allow_without_where();
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee""#);
    /// ```
    pub fn allow_without_where(&mut self) -> &mut Self {
        self.allow_without_where = true;
        self
    }

    /// Build a DeleteTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_tree(&self) -> tree::DeleteTree<V> {
        self.assert_where_present();
        tree::DeleteTree {
            table: self.table.clone(),
            table_alias: self.table_alias.clone(),
            wheres: self.wheres.clone(),
            order_bys: Vec::new(),
            limit: None,
        }
    }

    fn assert_where_present(&self) {
        assert!(
            self.allow_without_where || !self.wheres.is_empty(),
            "DELETE without WHERE is dangerous and not allowed by default. \
             Use .allow_without_where() to explicitly allow full-table deletes."
        );
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        renderer::delete::render_delete(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        renderer::delete::render_delete(&tree, &RenderConfig::from_dialect(&ph, &qi, dialect))
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
    fn test_not_where() {
        let mut q = sqipe("employee");
        q.and_where(not(col("role").eq("admin")));

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
        assert_eq!(binds, vec![Value::String("admin".to_string())]);
    }

    #[test]
    fn test_not_where_with_and() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? AND NOT (\"role\" = ?)"
        );
    }

    #[test]
    fn test_not_where_with_or() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.or_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"name\" = ? OR NOT (\"role\" = ?)"
        );
    }

    #[test]
    fn test_not_with_any() {
        let mut q = sqipe("employee");
        q.and_where(not(any(col("role").eq("admin"), col("role").eq("manager"))));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE NOT ((\"role\" = ? OR \"role\" = ?))"
        );
    }

    #[test]
    fn test_not_operator() {
        let mut q = sqipe("employee");
        q.and_where(!col("role").eq("admin"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT * FROM \"employee\" WHERE NOT (\"role\" = ?)");
    }

    #[test]
    fn test_not_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(not(col("role").eq("admin")));

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE NOT (\"role\" = ?) |> SELECT *"
        );
    }

    #[test]
    fn test_not_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(not(col("role").eq("admin")));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"name\" = $1 AND NOT (\"role\" = $2)"
        );
        assert_eq!(
            binds,
            vec![
                Value::String("Alice".to_string()),
                Value::String("admin".to_string())
            ]
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
    fn test_not_between() {
        let mut q = sqipe("employee");
        q.and_where(col("age").not_between(20, 30));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"age\" NOT BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_not_between_pipe_sql() {
        let mut q = sqipe("employee");
        q.and_where(col("age").not_between(20, 30));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"employee\" |> WHERE \"age\" NOT BETWEEN ? AND ? |> SELECT \"id\", \"name\""
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_not_between_with_other_conditions() {
        let mut q = sqipe("employee");
        q.and_where(("dept", "eng"));
        q.and_where(col("age").not_between(20, 30));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT * FROM \"employee\" WHERE \"dept\" = ? AND \"age\" NOT BETWEEN ? AND ?"
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
    fn test_between_qualified_col() {
        let mut q = sqipe("employee");
        q.join("dept", table("employee").col("dept_id").eq_col("id"));
        q.and_where(table("employee").col("age").between(20, 30));
        q.select_cols(&table("employee").cols(&["id", "name"]));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"employee\".\"id\", \"employee\".\"name\" FROM \"employee\" INNER JOIN \"dept\" ON \"employee\".\"dept_id\" = \"dept\".\"id\" WHERE \"employee\".\"age\" BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_not_between_qualified_col() {
        let mut q = sqipe("employee");
        q.join("dept", table("employee").col("dept_id").eq_col("id"));
        q.and_where(table("employee").col("age").not_between(20, 30));
        q.select_cols(&table("employee").cols(&["id", "name"]));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"employee\".\"id\", \"employee\".\"name\" FROM \"employee\" INNER JOIN \"dept\" ON \"employee\".\"dept_id\" = \"dept\".\"id\" WHERE \"employee\".\"age\" NOT BETWEEN ? AND ?"
        );
        assert_eq!(binds, vec![Value::Int(20), Value::Int(30)]);
    }

    #[test]
    fn test_not_between_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut q = sqipe("employee");
        q.and_where(("dept", "eng"));
        q.and_where(col("age").not_between(20, 30));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employee\" WHERE \"dept\" = $1 AND \"age\" NOT BETWEEN $2 AND $3"
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

    #[test]
    fn test_join_using_standard() {
        let mut q = sqipe("users");
        q.join("orders", join::using_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\")"
        );
    }

    #[test]
    fn test_join_using_pipe() {
        let mut q = sqipe("users");
        q.join("orders", join::using_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> INNER JOIN \"orders\" USING (\"user_id\") |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_left_join_using() {
        let mut q = sqipe("users");
        q.left_join("addresses", join::using_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" LEFT JOIN \"addresses\" USING (\"user_id\")"
        );
    }

    #[test]
    fn test_join_using_multiple_columns() {
        let mut q = sqipe("users");
        q.join("orders", join::using_cols(&["user_id", "tenant_id"]));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\", \"tenant_id\")"
        );
    }

    #[test]
    fn test_join_using_with_on_mixed() {
        let mut q = sqipe("users");
        q.join("orders", join::using_col("user_id"));
        q.left_join("addresses", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" INNER JOIN \"orders\" USING (\"user_id\") LEFT JOIN \"addresses\" ON \"users\".\"id\" = \"addresses\".\"user_id\""
        );
    }

    #[test]
    fn test_from_alias() {
        let mut q = sqipe("users");
        q.as_("u");
        q.and_where(table("u").col("name").eq("Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" AS \"u\" WHERE \"u\".\"name\" = ?"
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" AS \"u\" |> WHERE \"u\".\"name\" = ? |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_join_with_aliases() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.and_where(table("o").col("status").eq("shipped"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\" WHERE \"o\".\"status\" = ?"
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_join_alias_pipe() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" AS \"u\" |> INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\" |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_self_join_with_aliases() {
        let mut q = sqipe("employees");
        q.as_("e");
        q.left_join(
            table("employees").as_("m"),
            table("e").col("manager_id").eq_col("id"),
        );
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"employees\" AS \"e\" LEFT JOIN \"employees\" AS \"m\" ON \"e\".\"manager_id\" = \"m\".\"id\""
        );
    }

    #[test]
    fn test_select_cols_qualified() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        let mut cols = table("u").cols(&["id", "name"]);
        cols.extend(table("o").cols(&["total"]));
        q.select_cols(&cols);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"u\".\"id\", \"u\".\"name\", \"o\".\"total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\""
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" AS \"u\" |> INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\" |> SELECT \"u\".\"id\", \"u\".\"name\", \"o\".\"total\""
        );
    }

    #[test]
    fn test_add_select() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id"]);
        q.add_select(table("o").col("total"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"o\".\"total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\""
        );
    }

    #[test]
    fn test_select_cols_mixed_simple_and_qualified() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);
        q.add_select(table("o").col("total"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\", \"o\".\"total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\""
        );
    }

    #[test]
    fn test_select_with_alias() {
        let mut q = sqipe("users");
        q.as_("u");
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id"]);
        q.add_select(table("o").col("total").as_("order_total"));

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"o\".\"total\" AS \"order_total\" FROM \"users\" AS \"u\" INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\""
        );

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" AS \"u\" |> INNER JOIN \"orders\" AS \"o\" ON \"u\".\"id\" = \"o\".\"user_id\" |> SELECT \"id\", \"o\".\"total\" AS \"order_total\""
        );
    }

    #[test]
    fn test_select_simple_col_with_alias() {
        let mut q = sqipe("users");
        q.add_select(col("name").as_("user_name"));

        let (sql, _) = q.to_sql();
        assert_eq!(sql, "SELECT \"name\" AS \"user_name\" FROM \"users\"");
    }

    #[test]
    fn test_in_clause() {
        let mut q = sqipe("users");
        q.and_where(col("status").included(&["active", "pending"]));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" IN (?, ?)"
        );
        assert_eq!(
            binds,
            vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string()),
            ]
        );
    }

    #[test]
    fn test_in_clause_with_integers() {
        let mut q = sqipe("users");
        q.and_where(col("age").included(&[25, 30, 35]));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" IN (?, ?, ?)"
        );
        assert_eq!(binds, vec![Value::Int(25), Value::Int(30), Value::Int(35)]);
    }

    #[test]
    fn test_in_clause_qualified_col() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(
            table("orders")
                .col("status")
                .included(&["shipped", "delivered"]),
        );
        q.select_cols(&table("users").cols(&["id", "name"]));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"orders\".\"status\" IN (?, ?)"
        );
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn test_in_clause_pipe_sql() {
        let mut q = sqipe("users");
        q.and_where(col("status").included(&["active", "pending"]));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"status\" IN (?, ?) |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_in_clause_empty() {
        let empty: &[&str] = &[];
        let mut q = sqipe("users");
        q.and_where(col("status").included(empty));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE 1 = 0");
        assert!(binds.is_empty());
    }

    #[test]
    fn test_in_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?)"
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_in_subquery_with_outer_binds() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("amount").gt(100));

        let mut q = sqipe("users");
        q.and_where(col("active").eq(true));
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"active\" = ? AND \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"amount\" > ?)"
        );
        assert_eq!(binds, vec![Value::Bool(true), Value::Int(100)]);
    }

    #[test]
    fn test_in_subquery_with_outer_binds_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(20));
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" > $1 AND \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = $2)"
        );
        assert_eq!(
            binds,
            vec![Value::Int(20), Value::String("shipped".to_string())]
        );
    }

    #[test]
    fn test_in_subquery_pipe_sql() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?) |> SELECT \"id\", \"name\""
        );
    }

    #[test]
    fn test_in_subquery_pipe_sql_with_outer_binds() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(20));
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"age\" > ? AND \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?) |> SELECT \"id\", \"name\""
        );
        assert_eq!(
            binds,
            vec![Value::Int(20), Value::String("shipped".to_string())]
        );
    }

    #[test]
    fn test_in_subquery_pipe_sql_with_outer_binds_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(20));
        q.and_where(col("id").included(sub));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_pipe_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"age\" > $1 AND \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = $2) |> SELECT \"id\", \"name\""
        );
        assert_eq!(
            binds,
            vec![Value::Int(20), Value::String("shipped".to_string())]
        );
    }

    #[test]
    fn test_in_subquery_qualified_col() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);

        let mut q = sqipe("users");
        q.and_where(table("users").col("id").included(sub));
        q.select(&["id"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\" FROM \"users\" WHERE \"users\".\"id\" IN (SELECT \"user_id\" FROM \"orders\")"
        );
    }

    #[test]
    fn test_in_subquery_nested() {
        let mut inner_sub = sqipe("line_items");
        inner_sub.select(&["order_id"]);
        inner_sub.and_where(col("quantity").gt(10));

        let mut outer_sub = sqipe("orders");
        outer_sub.select(&["user_id"]);
        outer_sub.and_where(col("id").included(inner_sub));

        let mut q = sqipe("users");
        q.and_where(col("id").included(outer_sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"id\" IN (SELECT \"user_id\" FROM \"orders\" WHERE \"id\" IN (SELECT \"order_id\" FROM \"line_items\" WHERE \"quantity\" > ?))"
        );
        assert_eq!(binds, vec![Value::Int(10)]);
    }

    #[test]
    fn test_not_in_clause() {
        let mut q = sqipe("users");
        q.and_where(col("status").not_included(&["inactive", "banned"]));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"status\" NOT IN (?, ?)"
        );
        assert_eq!(
            binds,
            vec![
                Value::String("inactive".to_string()),
                Value::String("banned".to_string()),
            ]
        );
    }

    #[test]
    fn test_not_in_clause_empty() {
        let empty: &[&str] = &[];
        let mut q = sqipe("users");
        q.and_where(col("status").not_included(empty));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(sql, "SELECT \"id\", \"name\" FROM \"users\" WHERE 1 = 1");
        assert!(binds.is_empty());
    }

    #[test]
    fn test_not_in_clause_pipe_sql() {
        let mut q = sqipe("users");
        q.and_where(col("status").not_included(&["inactive", "banned"]));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"status\" NOT IN (?, ?) |> SELECT \"id\", \"name\""
        );
        assert_eq!(
            binds,
            vec![
                Value::String("inactive".to_string()),
                Value::String("banned".to_string()),
            ]
        );
    }

    #[test]
    fn test_not_in_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("cancelled"));

        let mut q = sqipe("users");
        q.and_where(col("id").not_included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"id\" NOT IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?)"
        );
        assert_eq!(binds, vec![Value::String("cancelled".to_string())]);
    }

    #[test]
    fn test_not_in_subquery_pipe_sql() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("cancelled"));

        let mut q = sqipe("users");
        q.and_where(col("id").not_included(sub));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"id\" NOT IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = ?) |> SELECT \"id\", \"name\""
        );
        assert_eq!(binds, vec![Value::String("cancelled".to_string())]);
    }

    #[test]
    fn test_not_in_clause_with_integers() {
        let mut q = sqipe("users");
        q.and_where(col("age").not_included(&[25, 30, 35]));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\", \"name\" FROM \"users\" WHERE \"age\" NOT IN (?, ?, ?)"
        );
        assert_eq!(binds, vec![Value::Int(25), Value::Int(30), Value::Int(35)]);
    }

    #[test]
    fn test_not_in_clause_qualified_col() {
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(
            table("orders")
                .col("status")
                .not_included(&["shipped", "delivered"]),
        );
        q.select_cols(&table("users").cols(&["id", "name"]));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"users\".\"id\", \"users\".\"name\" FROM \"users\" INNER JOIN \"orders\" ON \"users\".\"id\" = \"orders\".\"user_id\" WHERE \"orders\".\"status\" NOT IN (?, ?)"
        );
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn test_not_in_subquery_qualified_col() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);

        let mut q = sqipe("users");
        q.and_where(table("users").col("id").not_included(sub));
        q.select(&["id"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            "SELECT \"id\" FROM \"users\" WHERE \"users\".\"id\" NOT IN (SELECT \"user_id\" FROM \"orders\")"
        );
    }

    #[test]
    fn test_not_in_subquery_pipe_sql_with_outer_binds_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(20));
        q.and_where(col("id").not_included(sub));
        q.select(&["id", "name"]);
        let (sql, binds) = q.to_pipe_sql_with(&PgDialect);

        assert_eq!(
            sql,
            "FROM \"users\" |> WHERE \"age\" > $1 AND \"id\" NOT IN (SELECT \"user_id\" FROM \"orders\" WHERE \"status\" = $2) |> SELECT \"id\", \"name\""
        );
        assert_eq!(
            binds,
            vec![Value::Int(20), Value::String("shipped".to_string())]
        );
    }

    // ── FROM subquery tests ──

    #[test]
    fn test_from_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t""#
        );
        assert_eq!(binds, vec![Value::String("completed".to_string())]);
    }

    #[test]
    fn test_from_subquery_with_outer_where() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);
        q.and_where(col("amount").gt(100));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" WHERE "amount" > ?"#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(100),]
        );
    }

    #[test]
    fn test_from_subquery_pipe_sql() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t" |> SELECT "user_id""#
        );
        assert_eq!(binds, vec![Value::String("completed".to_string())]);
    }

    #[test]
    fn test_from_subquery_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id"]);
        sub.and_where(col("status").eq("completed"));

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);
        q.and_where(col("user_id").gt(10));

        let (sql, binds) = q.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id" FROM "orders" WHERE "status" = $1) AS "t" WHERE "user_id" > $2"#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(10),]
        );
    }

    #[test]
    fn test_from_subquery_with_limit() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "amount"]);
        sub.limit(10);

        let mut q = sqipe_from_subquery(sub, "t");
        q.select(&["user_id"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM "orders" LIMIT 10) AS "t""#
        );
    }

    // ── CTE generation tests (WHERE before JOIN) ──

    #[test]
    fn test_cte_where_then_join_standard() {
        // Case 1: WHERE → JOIN should generate a CTE in standard SQL
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_where_then_join_pipe() {
        // Case 1: WHERE → JOIN should NOT generate CTE in pipe SQL
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "age" > ? |> INNER JOIN "orders" ON "users"."id" = "orders"."user_id" |> SELECT "id", "name""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_where_join_then_where() {
        // Case 2: WHERE → JOIN → WHERE → CTE + main WHERE
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(table("orders").col("total").gt(100));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" WHERE "orders"."total" > ?"#
        );
        assert_eq!(binds, vec![Value::Int(25), Value::Int(100)]);
    }

    #[test]
    fn test_cte_multiple_boundaries() {
        // Case 3: WHERE → JOIN → WHERE → JOIN → multiple CTEs
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(table("orders").col("total").gt(100));
        q.join("payments", table("orders").col("id").eq_col("order_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?), "_cte_1" AS (SELECT * FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" WHERE "orders"."total" > ?) SELECT "id", "name" FROM "_cte_1" AS "users" INNER JOIN "payments" ON "orders"."id" = "payments"."order_id""#
        );
        assert_eq!(binds, vec![Value::Int(25), Value::Int(100)]);
    }

    #[test]
    fn test_join_then_where_no_cte() {
        // JOIN → WHERE (normal case) should NOT generate CTE
        let mut q = sqipe("users");
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.and_where(table("orders").col("total").gt(100));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" WHERE "orders"."total" > ?"#
        );
    }

    #[test]
    fn test_cte_with_alias() {
        // WHERE → JOIN with aliased table
        let mut q = sqipe("users");
        q.as_("u");
        q.and_where(col("age").gt(25));
        q.join(
            table("orders").as_("o"),
            table("u").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" AS "u" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "u" INNER JOIN "orders" AS "o" ON "u"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_or_where_then_join() {
        // or_where → JOIN should also generate CTE
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.or_where(col("name").eq("Alice"));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ? OR "name" = ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id""#
        );
        assert_eq!(
            binds,
            vec![Value::Int(25), Value::String("Alice".to_string())]
        );
    }

    #[test]
    fn test_cte_left_join_after_where() {
        // WHERE → LEFT JOIN should generate CTE
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.left_join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" LEFT JOIN "orders" ON "users"."id" = "orders"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_add_join_after_where() {
        // WHERE → add_join (custom join type) should generate CTE
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.add_join(
            JoinType::Custom("CROSS JOIN".to_string()),
            "orders",
            table("users").col("id").eq_col("user_id"),
        );
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" CROSS JOIN "orders" ON "users"."id" = "orders"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_with_order_by_and_limit() {
        // CTE query with ORDER BY and LIMIT
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);
        q.order_by(col("name").asc());
        q.limit(10);
        q.offset(5);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" ORDER BY "name" ASC LIMIT 10 OFFSET 5"#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_cte_union_with_cte_member() {
        // UNION where one member has WHERE→JOIN should generate CTE
        let mut q1 = sqipe("users");
        q1.and_where(col("age").gt(25));
        q1.join("orders", table("users").col("id").eq_col("user_id"));
        q1.select(&["id", "name"]);

        let mut q2 = sqipe("users");
        q2.and_where(col("age").lt(20));
        q2.select(&["id", "name"]);

        let uq = q1.union(&q2);
        let (sql, binds) = uq.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" UNION SELECT "id", "name" FROM "users" WHERE "age" < ?"#
        );
        assert_eq!(binds, vec![Value::Int(25), Value::Int(20)]);
    }

    #[test]
    fn test_cte_with_aggregate() {
        // WHERE → JOIN with aggregate query
        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join("orders", table("users").col("id").eq_col("user_id"));
        q.aggregate(&[aggregate::count_all().as_("order_count")]);
        q.group_by(&["name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "name", COUNT(*) AS "order_count" FROM "_cte_0" AS "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id" GROUP BY "name""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_from_subquery_nested() {
        let mut inner = sqipe("orders");
        inner.select(&["user_id", "amount"]);
        inner.and_where(col("status").eq("completed"));

        let mut mid = sqipe_from_subquery(inner, "t1");
        mid.select(&["user_id", "amount"]);
        mid.and_where(col("amount").gt(100));

        let mut outer = sqipe_from_subquery(mid, "t2");
        outer.select(&["user_id"]);

        let (sql, binds) = outer.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "user_id" FROM (SELECT "user_id", "amount" FROM (SELECT "user_id", "amount" FROM "orders" WHERE "status" = ?) AS "t1" WHERE "amount" > ?) AS "t2""#
        );
        assert_eq!(
            binds,
            vec![Value::String("completed".to_string()), Value::Int(100),]
        );
    }

    #[test]
    fn test_like_contains() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_starts_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::starts_with("Ali")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("Ali%".to_string())]);
    }

    #[test]
    fn test_like_ends_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::ends_with("ice")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%ice".to_string())]);
    }

    #[test]
    fn test_not_like() {
        let mut q = sqipe("users");
        q.and_where(col("name").not_like(LikeExpression::contains("Bob")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" NOT LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%Bob%".to_string())]);
    }

    #[test]
    fn test_like_escapes_wildcards() {
        let mut q = sqipe("products");
        q.and_where(col("name").like(LikeExpression::contains("100%")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%100\\%%".to_string())]);
    }

    #[test]
    fn test_like_escapes_underscore() {
        let mut q = sqipe("products");
        q.and_where(col("name").like(LikeExpression::starts_with("a_b")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("a\\_b%".to_string())]);
    }

    #[test]
    fn test_like_qualified_col() {
        let mut q = sqipe("users");
        q.as_("u");
        q.and_where(table("u").col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" AS "u" WHERE "u"."name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_pipe_sql() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains("Ali")));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "name" LIKE ? ESCAPE '\' |> SELECT "id", "name""#
        );
        assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_char() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "100%")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("%100!%%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_starts_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::starts_with_escaped_by('!', "a_b")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("a!_b%".to_string())]);
    }

    #[test]
    fn test_like_custom_escape_ends_with() {
        let mut q = sqipe("users");
        q.and_where(col("name").like(LikeExpression::ends_with_escaped_by('!', "x%y")));

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
        );
        assert_eq!(binds, vec![Value::String("%x!%y".to_string())]);
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_percent_as_escape() {
        LikeExpression::contains_escaped_by('%', "foo");
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_underscore_as_escape() {
        LikeExpression::starts_with_escaped_by('_', "foo");
    }

    #[test]
    #[should_panic(expected = "escape character must not be")]
    fn test_like_rejects_single_quote_as_escape() {
        LikeExpression::ends_with_escaped_by('\'', "foo");
    }

    // ── join_subquery tests ──

    #[test]
    fn test_join_subquery_standard() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_left_join_subquery_standard() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);

        let mut q = sqipe("users");
        q.left_join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" LEFT JOIN (SELECT "user_id", "total" FROM "orders") AS "o" ON "users"."id" = "o"."user_id""#
        );
    }

    #[test]
    fn test_join_subquery_pipe_sql() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id" |> SELECT "id", "name""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    #[test]
    fn test_join_subquery_with_outer_where() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.and_where(col("age").gt(25));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id" WHERE "age" > ?"#
        );
        assert_eq!(
            binds,
            vec![Value::String("shipped".to_string()), Value::Int(25)]
        );
    }

    #[test]
    fn test_join_subquery_numbered_placeholders() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > $1) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = $2) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(
            binds,
            vec![Value::Int(25), Value::String("shipped".to_string())]
        );
    }

    #[test]
    fn test_cte_where_then_join_subquery() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);

        let mut q = sqipe("users");
        q.and_where(col("age").gt(25));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"WITH "_cte_0" AS (SELECT * FROM "users" WHERE "age" > ?) SELECT "id", "name" FROM "_cte_0" AS "users" INNER JOIN (SELECT "user_id", "total" FROM "orders") AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::Int(25)]);
    }

    #[test]
    fn test_join_subquery_mixed_with_table_join() {
        let mut sub = sqipe("orders");
        sub.select(&["user_id", "total"]);
        sub.and_where(col("status").eq("shipped"));

        let mut q = sqipe("users");
        q.join("profiles", table("users").col("id").eq_col("user_id"));
        q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
        q.select(&["id", "name"]);

        let (sql, binds) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" INNER JOIN "profiles" ON "users"."id" = "profiles"."user_id" INNER JOIN (SELECT "user_id", "total" FROM "orders" WHERE "status" = ?) AS "o" ON "users"."id" = "o"."user_id""#
        );
        assert_eq!(binds, vec![Value::String("shipped".to_string())]);
    }

    // ── JoinCondition::Expr tests ──

    #[test]
    fn test_join_condition_expr_standard() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."text" LIKE "patterns"."pattern""#
        );
    }

    #[test]
    fn test_join_condition_expr_pipe() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "texts" |> INNER JOIN "patterns" ON "texts"."text" LIKE "patterns"."pattern" |> SELECT "id", "text""#
        );
    }

    #[test]
    fn test_for_update() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update();

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update();

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_with_option() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update_with("NOWAIT");

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR UPDATE NOWAIT"#
        );
    }

    #[test]
    fn test_for_update_with_option_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_update_with("SKIP LOCKED");

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR UPDATE SKIP LOCKED"#
        );
    }

    #[test]
    fn test_for_with() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_with("NO KEY UPDATE");

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" WHERE "id" = ? FOR NO KEY UPDATE"#
        );
    }

    #[test]
    fn test_for_with_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.and_where(col("id").eq(1));
        q.for_with("SHARE");

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> WHERE "id" = ? |> SELECT "id", "name" FOR SHARE"#
        );
    }

    #[test]
    fn test_for_update_with_order_by_and_limit() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.order_by(col("id").asc());
        q.limit(10);
        q.for_update();

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "name" FROM "users" ORDER BY "id" ASC LIMIT 10 FOR UPDATE"#
        );
    }

    #[test]
    fn test_for_update_with_order_by_and_limit_pipe() {
        let mut q = sqipe("users");
        q.select(&["id", "name"]);
        q.order_by(col("id").asc());
        q.limit(10);
        q.for_update();

        let (sql, _) = q.to_pipe_sql();
        assert_eq!(
            sql,
            r#"FROM "users" |> SELECT "id", "name" |> ORDER BY "id" ASC |> LIMIT 10 FOR UPDATE"#
        );
    }

    #[test]
    fn test_join_condition_expr_inside_and() {
        let mut q = sqipe("texts");
        q.join(
            "patterns",
            JoinCondition::And(vec![
                table("texts").col("category").eq_col("category"),
                join::on_expr(r#""texts"."text" LIKE "patterns"."pattern""#),
            ]),
        );
        q.select(&["id", "text"]);

        let (sql, _) = q.to_sql();
        assert_eq!(
            sql,
            r#"SELECT "id", "text" FROM "texts" INNER JOIN "patterns" ON "texts"."category" = "patterns"."category" AND "texts"."text" LIKE "patterns"."pattern""#
        );
    }

    // ── UPDATE tests ──

    #[test]
    fn test_update_basic() {
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(1)]
        );
    }

    #[test]
    fn test_update_multiple_sets() {
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set(col("age"), 30);
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "name" = ?, "age" = ? WHERE "id" = ?"#
        );
        assert_eq!(
            binds,
            vec![
                Value::String("Alice".to_string()),
                Value::Int(30),
                Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_allow_without_where() {
        let mut u = sqipe("employee").update();
        u.set(col("status"), "inactive");
        u.allow_without_where();
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "status" = ?"#);
        assert_eq!(binds, vec![Value::String("inactive".to_string())]);
    }

    #[test]
    fn test_update_from_query_with_where() {
        let mut q = sqipe("employee");
        q.and_where(col("id").eq(1));
        let mut u = q.update();
        u.set(col("name"), "Alice");
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(1)]
        );
    }

    #[test]
    fn test_update_with_dialect() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set(col("age"), 30);
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "name" = $1, "age" = $2 WHERE "id" = $3"#
        );
        assert_eq!(
            binds,
            vec![
                Value::String("Alice".to_string()),
                Value::Int(30),
                Value::Int(1)
            ]
        );
    }

    #[test]
    fn test_update_with_complex_where() {
        let mut u = sqipe("employee").update();
        u.set(col("status"), "active");
        u.and_where(col("age").between(20, 60));
        u.and_where(col("role").included(&["admin", "manager"]));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "status" = ? WHERE "age" BETWEEN ? AND ? AND "role" IN (?, ?)"#
        );
        assert_eq!(
            binds,
            vec![
                Value::String("active".to_string()),
                Value::Int(20),
                Value::Int(60),
                Value::String("admin".to_string()),
                Value::String("manager".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_or_where() {
        let mut u = sqipe("employee").update();
        u.set(col("reviewed"), true);
        u.and_where(col("status").eq("pending"));
        u.or_where(col("status").eq("draft"));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "reviewed" = ? WHERE "status" = ? OR "status" = ?"#
        );
        assert_eq!(
            binds,
            vec![
                Value::Bool(true),
                Value::String("pending".to_string()),
                Value::String("draft".to_string()),
            ]
        );
    }

    #[test]
    fn test_update_with_like() {
        let mut u = sqipe("employee").update();
        u.set(col("flagged"), true);
        u.and_where(col("name").like(LikeExpression::starts_with("test")));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "flagged" = ? WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(
            binds,
            vec![Value::Bool(true), Value::String("test%".to_string()),]
        );
    }

    #[test]
    #[should_panic(expected = "UPDATE requires at least one SET clause")]
    fn test_update_empty_sets_panics() {
        let mut u = sqipe("employee").update();
        u.allow_without_where();
        let _ = u.to_sql();
    }

    #[test]
    #[should_panic(expected = "UPDATE without WHERE is dangerous")]
    fn test_update_no_where_panics() {
        let mut u = sqipe("employee").update();
        u.set(col("status"), "inactive");
        let _ = u.to_sql();
    }

    #[test]
    fn test_update_with_table_alias() {
        let mut q = sqipe("employee");
        q.as_("e");
        let mut u = q.update();
        u.set(col("name"), "Alice");
        u.and_where(col("id").eq(1));
        let (sql, _) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" "e" SET "name" = ? WHERE "id" = ?"#
        );
    }

    #[test]
    fn test_update_with_set_expr() {
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#
        );
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_update_with_set_and_set_expr_mixed() {
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "name" = ?, "visit_count" = "visit_count" + 1 WHERE "id" = ?"#
        );
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(1)]
        );
    }

    #[test]
    fn test_update_with_multiple_set_exprs() {
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.set_expr(SetExpression::new(r#""updated_at" = NOW()"#));
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1, "updated_at" = NOW() WHERE "id" = ?"#
        );
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_update_with_set_expr_allow_without_where() {
        let mut u = sqipe("employee").update();
        u.set_expr(SetExpression::new(r#""version" = "version" + 1"#));
        u.allow_without_where();
        let (sql, binds) = u.to_sql();
        assert_eq!(sql, r#"UPDATE "employee" SET "version" = "version" + 1"#);
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_update_with_set_expr_bind_order() {
        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.set(col("status"), "active");
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql();
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "name" = ?, "visit_count" = "visit_count" + 1, "status" = ? WHERE "id" = ?"#
        );
        assert_eq!(
            binds,
            vec![
                Value::String("Alice".to_string()),
                Value::String("active".to_string()),
                Value::Int(1),
            ]
        );
    }

    #[test]
    fn test_update_with_set_expr_dialect() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut u = sqipe("employee").update();
        u.set(col("name"), "Alice");
        u.set_expr(SetExpression::new(r#""visit_count" = "visit_count" + 1"#));
        u.and_where(col("id").eq(1));
        let (sql, binds) = u.to_sql_with(&PgDialect);
        assert_eq!(
            sql,
            r#"UPDATE "employee" SET "name" = $1, "visit_count" = "visit_count" + 1 WHERE "id" = $2"#
        );
        assert_eq!(
            binds,
            vec![Value::String("Alice".to_string()), Value::Int(1)]
        );
    }

    // ── DELETE tests ──

    #[test]
    fn test_delete_basic() {
        let mut d = sqipe("employee").delete();
        d.and_where(col("id").eq(1));
        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_delete_allow_without_where() {
        let mut d = sqipe("employee").delete();
        d.allow_without_where();
        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee""#);
        assert_eq!(binds, vec![]);
    }

    #[test]
    fn test_delete_from_query_with_where() {
        let mut q = sqipe("employee");
        q.and_where(col("id").eq(1));
        let d = q.delete();
        let (sql, binds) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ?"#);
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_delete_with_dialect() {
        struct PgDialect;
        impl Dialect for PgDialect {
            fn placeholder(&self, index: usize) -> String {
                format!("${}", index)
            }
        }

        let mut d = sqipe("employee").delete();
        d.and_where(col("id").eq(1));
        let (sql, binds) = d.to_sql_with(&PgDialect);
        assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = $1"#);
        assert_eq!(binds, vec![Value::Int(1)]);
    }

    #[test]
    fn test_delete_with_complex_where() {
        let mut d = sqipe("employee").delete();
        d.and_where(col("age").between(20, 60));
        d.and_where(col("role").included(&["admin", "manager"]));
        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            r#"DELETE FROM "employee" WHERE "age" BETWEEN ? AND ? AND "role" IN (?, ?)"#
        );
        assert_eq!(
            binds,
            vec![
                Value::Int(20),
                Value::Int(60),
                Value::String("admin".to_string()),
                Value::String("manager".to_string()),
            ]
        );
    }

    #[test]
    fn test_delete_with_or_where() {
        let mut d = sqipe("employee").delete();
        d.and_where(col("status").eq("pending"));
        d.or_where(col("status").eq("draft"));
        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            r#"DELETE FROM "employee" WHERE "status" = ? OR "status" = ?"#
        );
        assert_eq!(
            binds,
            vec![
                Value::String("pending".to_string()),
                Value::String("draft".to_string()),
            ]
        );
    }

    #[test]
    fn test_delete_with_like() {
        let mut d = sqipe("employee").delete();
        d.and_where(col("name").like(LikeExpression::starts_with("test")));
        let (sql, binds) = d.to_sql();
        assert_eq!(
            sql,
            r#"DELETE FROM "employee" WHERE "name" LIKE ? ESCAPE '\'"#
        );
        assert_eq!(binds, vec![Value::String("test%".to_string())]);
    }

    #[test]
    #[should_panic(expected = "DELETE without WHERE is dangerous")]
    fn test_delete_no_where_panics() {
        let d = sqipe("employee").delete();
        let _ = d.to_sql();
    }

    #[test]
    fn test_delete_with_table_alias() {
        let mut q = sqipe("employee");
        q.as_("e");
        let mut d = q.delete();
        d.and_where(col("id").eq(1));
        let (sql, _) = d.to_sql();
        assert_eq!(sql, r#"DELETE FROM "employee" "e" WHERE "id" = ?"#);
    }

    #[test]
    #[should_panic(expected = "JOINs which are not supported in DELETE")]
    fn test_delete_from_query_with_joins_panics() {
        let mut q = sqipe("employee");
        q.join("department", table("employee").col("dept_id").eq_col("id"));
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "aggregates which are not supported in DELETE")]
    fn test_delete_from_query_with_aggregates_panics() {
        let mut q = sqipe("employee");
        q.aggregate(&[aggregate::count_all()]);
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "ORDER BY which is not supported in DELETE")]
    fn test_delete_from_query_with_order_by_panics() {
        let mut q = sqipe("employee");
        q.order_by(OrderByClause {
            col: "id".to_string(),
            dir: SortDir::Asc,
        });
        let _ = q.delete();
    }

    #[test]
    #[should_panic(expected = "LIMIT which is not supported in DELETE")]
    fn test_delete_from_query_with_limit_panics() {
        let mut q = sqipe("employee");
        q.limit(10);
        let _ = q.delete();
    }
}
