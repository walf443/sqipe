use crate::{AggregateExpr, ColRef, JoinClause, OrderByClause, WhereEntry};

/// Default double-quote identifier quoting (SQL standard).
pub fn default_quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// FROM clause with optional dialect-specific modifiers appended after the table name.
#[derive(Debug, Clone)]
pub struct FromClause {
    pub table: String,
    pub alias: Option<String>,
    /// Raw SQL fragments appended after the table name (e.g., "FORCE INDEX (idx)").
    /// Dialect crates populate this via tree transformation.
    pub table_suffix: Vec<String>,
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
    pub from: FromClause,
    pub joins: Vec<JoinClause>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub select: SelectClause,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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
            from: self.from,
            joins: self.joins,
            wheres: self.wheres.into_iter().map(|w| w.map_values(f)).collect(),
            havings: self.havings.into_iter().map(|w| w.map_values(f)).collect(),
            select: self.select,
            order_bys: self.order_bys,
            limit: self.limit,
            offset: self.offset,
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

        SelectTree {
            from: FromClause {
                table: query.table.clone(),
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
