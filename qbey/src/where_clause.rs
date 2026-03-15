use crate::column::Col;
use crate::like::LikeExpression;
use crate::value::{Op, Value};

use std::ops::{Range, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

/// A WHERE condition tree, generic over the bind value type.
///
/// `Debug` is implemented manually (not derived) because `InSubQuery` contains
/// `SelectTree<V>` which requires `V: Clone`. The derive macro would only add
/// `V: Debug`, but we also need `V: Clone` for the enum definition itself.
#[derive(Clone)]
pub enum WhereClause<V: Clone = Value> {
    Condition {
        col: Col,
        op: Op,
        val: V,
    },
    Between {
        col: Col,
        low: V,
        high: V,
    },
    NotBetween {
        col: Col,
        low: V,
        high: V,
    },
    In {
        col: Col,
        vals: Vec<V>,
    },
    InSubQuery {
        col: Col,
        sub: Box<crate::tree::SelectTree<V>>,
    },
    NotIn {
        col: Col,
        vals: Vec<V>,
    },
    NotInSubQuery {
        col: Col,
        sub: Box<crate::tree::SelectTree<V>>,
    },
    Like {
        col: Col,
        /// Preserved for ESCAPE clause rendering.
        expr: LikeExpression,
        /// Bind parameter value (always `expr.to_pattern()` at construction).
        val: V,
    },
    NotLike {
        col: Col,
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
            col: Col {
                table: None,
                column: self.0.to_string(),
                alias: None,
            },
            op: Op::Eq,
            val: self.1.into(),
        }
    }
}

/// Trait for converting Rust range types into WhereClause.
pub trait IntoRangeClause<V: Clone> {
    fn into_where_clause(self, col: Col) -> WhereClause<V>;
}

/// `20..=30` → `col BETWEEN 20 AND 30`
impl<V: Clone> IntoRangeClause<V> for RangeInclusive<V> {
    fn into_where_clause(self, col: Col) -> WhereClause<V> {
        let (low, high) = self.into_inner();
        WhereClause::Between { col, low, high }
    }
}

/// `20..30` → `col >= 20 AND col < 30`
impl<V: Clone> IntoRangeClause<V> for Range<V> {
    fn into_where_clause(self, col: Col) -> WhereClause<V> {
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
    fn into_where_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Gte,
            val: self.start,
        }
    }
}

/// `..30` → `col < 30`
impl<V: Clone> IntoRangeClause<V> for RangeTo<V> {
    fn into_where_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Lt,
            val: self.end,
        }
    }
}

/// `..=30` → `col <= 30`
impl<V: Clone> IntoRangeClause<V> for RangeToInclusive<V> {
    fn into_where_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::Condition {
            col,
            op: Op::Lte,
            val: self.end,
        }
    }
}

/// Trait for types that can be used as a source for `included` (IN clause).
///
/// Implemented for slices (value list) and `SelectQuery` (subquery).
pub trait IntoIncluded<V: Clone> {
    fn into_in_clause(self, col: Col) -> WhereClause<V>;
    fn into_not_in_clause(self, col: Col) -> WhereClause<V>;
}

impl<V: Clone> IntoIncluded<V> for &[V] {
    fn into_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::In {
            col,
            vals: self.to_vec(),
        }
    }

    fn into_not_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::NotIn {
            col,
            vals: self.to_vec(),
        }
    }
}

impl<V: Clone, const N: usize> IntoIncluded<V> for &[V; N] {
    fn into_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::In {
            col,
            vals: self.to_vec(),
        }
    }

    fn into_not_in_clause(self, col: Col) -> WhereClause<V> {
        WhereClause::NotIn {
            col,
            vals: self.to_vec(),
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
