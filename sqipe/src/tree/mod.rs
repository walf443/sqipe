use crate::{AggregateExpr, ColRef, JoinClause, OrderByClause, WhereEntry};

/// Default double-quote identifier quoting (SQL standard).
pub fn default_quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Records the order in which WHERE and JOIN operations were added to a query.
/// Used to detect CTE boundaries (WHERE before JOIN) during rendering.
#[derive(Debug, Clone)]
pub enum StageRef {
    /// A WHERE clause was added; the value is the index into `wheres`.
    Where(usize),
    /// A JOIN clause was added; the value is the index into `joins`.
    Join(usize),
}

/// The source of a FROM clause — either a table name or a subquery.
#[derive(Debug, Clone)]
pub enum FromSource<V: Clone = crate::Value> {
    /// A simple table name (e.g., `"users"`).
    Table(String),
    /// A subquery (e.g., `(SELECT ... FROM orders WHERE ...)`).
    Subquery(Box<SelectTree<V>>),
}

/// FROM clause with optional alias and dialect-specific modifiers.
#[derive(Debug, Clone)]
pub struct FromClause<V: Clone = crate::Value> {
    pub source: FromSource<V>,
    pub alias: Option<String>,
    /// Raw SQL fragments appended after the table/subquery (e.g., "FORCE INDEX (idx)").
    /// Dialect crates populate this via tree transformation.
    pub table_suffix: Vec<String>,
}

impl<V: Clone> FromClause<V> {
    /// Transform all bind values in this clause (only relevant for subquery sources).
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> FromClause<U> {
        FromClause {
            source: match self.source {
                FromSource::Table(t) => FromSource::Table(t),
                FromSource::Subquery(sq) => FromSource::Subquery(Box::new(sq.map_values(f))),
            },
            alias: self.alias,
            table_suffix: self.table_suffix,
        }
    }
}

/// What the SELECT clause looks like.
#[derive(Debug, Clone)]
pub enum SelectClause {
    /// SELECT * or SELECT col1, col2, ...
    Columns(Vec<ColRef>),
    /// Aggregate: SELECT group_cols..., agg_exprs...
    Aggregate {
        group_bys: Vec<String>,
        exprs: Vec<AggregateExpr>,
    },
}

/// AST for a single SELECT query, generic over bind value type.
#[derive(Debug, Clone)]
pub struct SelectTree<V: Clone = crate::Value> {
    pub from: FromClause<V>,
    pub joins: Vec<JoinClause>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub select: SelectClause,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// Records the order in which WHERE and JOIN operations were added.
    /// Used to detect CTE boundaries when WHERE appears before JOIN.
    pub stage_order: Vec<StageRef>,
}

/// AST for a UNION query, generic over bind value type.
#[derive(Debug, Clone)]
pub struct UnionTree<V: Clone = crate::Value> {
    pub parts: Vec<(crate::SetOp, SelectTree<V>)>,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

impl<V: Clone> SelectTree<V> {
    /// Transform all bind values in this tree.
    ///
    /// `select` is not mapped because `SelectClause` contains only column
    /// references and aggregate expressions, neither of which holds bind values.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> SelectTree<U> {
        SelectTree {
            from: self.from.map_values(f),
            joins: self.joins,
            wheres: self.wheres.into_iter().map(|w| w.map_values(f)).collect(),
            havings: self.havings.into_iter().map(|w| w.map_values(f)).collect(),
            select: self.select,
            order_bys: self.order_bys,
            limit: self.limit,
            offset: self.offset,
            stage_order: self.stage_order,
        }
    }
}

// ── Build tree from Query ──

impl<V: Clone + std::fmt::Debug> SelectTree<V> {
    pub fn from_query(query: &crate::Query<V>) -> Self {
        let select = if !query.aggregates.is_empty() {
            SelectClause::Aggregate {
                group_bys: query.group_bys.clone(),
                exprs: query.aggregates.clone(),
            }
        } else {
            SelectClause::Columns(query.selects.clone())
        };

        let source = match &query.from_subquery {
            Some(sq) => FromSource::Subquery(sq.clone()),
            None => FromSource::Table(query.table.clone()),
        };

        SelectTree {
            from: FromClause {
                source,
                alias: query.table_alias.clone(),
                table_suffix: Vec::new(),
            },
            joins: query.joins.clone(),
            wheres: query.wheres.clone(),
            havings: query.havings.clone(),
            select,
            order_bys: query.order_bys.clone(),
            limit: query.limit_val,
            offset: query.offset_val,
            stage_order: query.stage_order.clone(),
        }
    }

    /// Convert a Query into a SelectTree by moving fields instead of cloning.
    pub fn from_query_owned(query: crate::Query<V>) -> Self {
        let select = if !query.aggregates.is_empty() {
            SelectClause::Aggregate {
                group_bys: query.group_bys,
                exprs: query.aggregates,
            }
        } else {
            SelectClause::Columns(query.selects)
        };

        let source = match query.from_subquery {
            Some(sq) => FromSource::Subquery(sq),
            None => FromSource::Table(query.table),
        };

        SelectTree {
            from: FromClause {
                source,
                alias: query.table_alias,
                table_suffix: Vec::new(),
            },
            joins: query.joins,
            wheres: query.wheres,
            havings: query.havings,
            select,
            order_bys: query.order_bys,
            limit: query.limit_val,
            offset: query.offset_val,
            stage_order: query.stage_order,
        }
    }
}

impl<V: Clone + std::fmt::Debug> UnionTree<V> {
    pub fn from_union_query(union: &crate::UnionQuery<V>) -> Self {
        let parts = union
            .parts
            .iter()
            .map(|(op, q)| (op.clone(), SelectTree::from_query(q)))
            .collect();
        UnionTree {
            parts,
            order_bys: union.order_bys.clone(),
            limit: union.limit_val,
            offset: union.offset_val,
        }
    }
}
