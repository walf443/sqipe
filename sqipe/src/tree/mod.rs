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

/// AST for a single SELECT query.
#[derive(Debug, Clone)]
pub struct SelectTree {
    pub from: FromClause,
    pub joins: Vec<JoinClause>,
    pub(crate) wheres: Vec<WhereEntry>,
    pub(crate) havings: Vec<WhereEntry>,
    pub select: SelectClause,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// AST for a UNION query.
#[derive(Debug, Clone)]
pub struct UnionTree {
    pub parts: Vec<(crate::SetOp, SelectTree)>,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// ── Build tree from Query ──

impl SelectTree {
    pub fn from_query(query: &crate::Query) -> Self {
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

impl UnionTree {
    pub fn from_union_query(union: &crate::UnionQuery) -> Self {
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
