use crate::{JoinClause, OrderByClause, SelectItem, WhereEntry};

/// Default double-quote identifier quoting (SQL standard).
pub fn default_quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
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
    Columns(Vec<SelectItem>),
}

/// AST for a SELECT query, generic over bind value type.
///
/// Also supports compound queries (UNION, INTERSECT, EXCEPT) via `set_operations`.
/// When `set_operations` is non-empty, all parts are stored there and the outer
/// `order_bys`/`limit`/`offset` apply to the entire compound result.
#[derive(Debug, Clone)]
pub struct SelectTree<V: Clone = crate::Value> {
    pub from: FromClause<V>,
    pub joins: Vec<JoinClause>,
    /// Subquery sources for joins, keyed by join index.
    /// When `join_subqueries[i]` is `Some(...)`, the renderer uses the subquery
    /// instead of `joins[i].table`.
    pub join_subqueries: Vec<Option<Box<SelectTree<V>>>>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) havings: Vec<WhereEntry<V>>,
    pub select: SelectClause,
    pub group_bys: Vec<String>,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// Row-level locking clause (e.g., `"UPDATE"` → `FOR UPDATE`).
    pub lock_for: Option<String>,
    /// When non-empty, this tree represents a compound query.
    /// All parts are stored here; the outer `order_bys`/`limit`/`offset`
    /// apply to the entire compound result.
    pub set_operations: Vec<(crate::SetOp, SelectTree<V>)>,
}

impl<V: Clone> SelectTree<V> {
    /// Transform all bind values in this tree.
    ///
    /// `select` is not mapped because `SelectClause` variants (column
    /// references, raw expressions, and function calls) do not hold bind values.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> SelectTree<U> {
        SelectTree {
            from: self.from.map_values(f),
            joins: self.joins,
            join_subqueries: self
                .join_subqueries
                .into_iter()
                .map(|opt| opt.map(|sq| Box::new(sq.map_values(f))))
                .collect(),
            wheres: self.wheres.into_iter().map(|w| w.map_values(f)).collect(),
            havings: self.havings.into_iter().map(|w| w.map_values(f)).collect(),
            select: self.select,
            group_bys: self.group_bys,
            order_bys: self.order_bys,
            limit: self.limit,
            offset: self.offset,
            lock_for: self.lock_for,
            set_operations: self
                .set_operations
                .into_iter()
                .map(|(op, t)| (op, t.map_values(f)))
                .collect(),
        }
    }
}

// ── Build tree from SelectQuery ──

impl<V: Clone + std::fmt::Debug> SelectTree<V> {
    pub fn from_query(query: &crate::SelectQuery<V>) -> Self {
        let select = SelectClause::Columns(query.selects.clone());

        let source = match &query.from_subquery {
            Some(sq) => FromSource::Subquery(sq.clone()),
            None => FromSource::Table(query.table.clone()),
        };

        debug_assert!(
            query.join_subqueries.len() <= query.joins.len(),
            "join_subqueries must not exceed joins length"
        );
        let mut join_subqueries = query.join_subqueries.clone();
        join_subqueries.resize_with(query.joins.len(), || None);

        SelectTree {
            from: FromClause {
                source,
                alias: query.table_alias.clone(),
                table_suffix: Vec::new(),
            },
            joins: query.joins.clone(),
            join_subqueries,
            wheres: query.wheres.clone(),
            havings: query.havings.clone(),
            select,
            group_bys: query.group_bys.clone(),
            order_bys: query.order_bys.clone(),
            limit: query.limit_val,
            offset: query.offset_val,
            lock_for: query.lock_for.clone(),
            set_operations: query
                .set_operations
                .iter()
                .map(|(op, q)| (op.clone(), SelectTree::from_query(q)))
                .collect(),
        }
    }

    /// Convert a SelectQuery into a SelectTree by moving fields instead of cloning.
    pub fn from_query_owned(query: crate::SelectQuery<V>) -> Self {
        let select = SelectClause::Columns(query.selects);

        let source = match query.from_subquery {
            Some(sq) => FromSource::Subquery(sq),
            None => FromSource::Table(query.table),
        };

        let join_count = query.joins.len();
        debug_assert!(
            query.join_subqueries.len() <= join_count,
            "join_subqueries must not exceed joins length"
        );
        let mut join_subqueries = query.join_subqueries;
        join_subqueries.resize_with(join_count, || None);

        let set_operations: Vec<_> = query
            .set_operations
            .into_iter()
            .map(|(op, q)| (op, SelectTree::from_query_owned(q)))
            .collect();

        SelectTree {
            from: FromClause {
                source,
                alias: query.table_alias,
                table_suffix: Vec::new(),
            },
            joins: query.joins,
            join_subqueries,
            wheres: query.wheres,
            havings: query.havings,
            select,
            group_bys: query.group_bys,
            order_bys: query.order_bys,
            limit: query.limit_val,
            offset: query.offset_val,
            lock_for: query.lock_for,
            set_operations,
        }
    }
}

/// AST for an UPDATE statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct UpdateTree<V: Clone = crate::Value> {
    pub table: String,
    pub table_alias: Option<String>,
    pub sets: Vec<crate::SetClause<V>>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
}

impl<V: Clone> UpdateTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> UpdateTree<U> {
        UpdateTree {
            table: self.table,
            table_alias: self.table_alias,
            sets: self
                .sets
                .into_iter()
                .map(|s| match s {
                    crate::SetClause::Value(col, val) => crate::SetClause::Value(col, f(val)),
                    crate::SetClause::Expr(e) => crate::SetClause::Expr(e),
                })
                .collect(),
            wheres: self.wheres.into_iter().map(|w| w.map_values(f)).collect(),
            order_bys: self.order_bys,
            limit: self.limit,
        }
    }
}

/// The source of rows for an INSERT statement.
#[derive(Debug, Clone)]
pub enum InsertTreeSource<V: Clone = crate::Value> {
    /// Explicit value rows.
    Values(Vec<Vec<V>>),
    /// A subquery (INSERT ... SELECT ...).
    Select(Box<SelectTree<V>>),
}

/// AST for an INSERT statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct InsertTree<V: Clone = crate::Value> {
    pub table: String,
    pub columns: Vec<String>,
    pub source: InsertTreeSource<V>,
}

impl<V: Clone> InsertTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> InsertTree<U> {
        InsertTree {
            table: self.table,
            columns: self.columns,
            source: match self.source {
                InsertTreeSource::Values(rows) => InsertTreeSource::Values(
                    rows.into_iter()
                        .map(|row| row.into_iter().map(&f).collect())
                        .collect(),
                ),
                InsertTreeSource::Select(sub) => {
                    InsertTreeSource::Select(Box::new(sub.map_values(f)))
                }
            },
        }
    }
}

/// AST for a DELETE statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct DeleteTree<V: Clone = crate::Value> {
    pub table: String,
    pub table_alias: Option<String>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub order_bys: Vec<OrderByClause>,
    pub limit: Option<u64>,
}

impl<V: Clone> DeleteTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> DeleteTree<U> {
        DeleteTree {
            table: self.table,
            table_alias: self.table_alias,
            wheres: self.wheres.into_iter().map(|w| w.map_values(f)).collect(),
            order_bys: self.order_bys,
            limit: self.limit,
        }
    }
}
