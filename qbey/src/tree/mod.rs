use crate::{JoinClause, OrderByClause, SelectItem, WhereEntry};

/// The source of a FROM clause — either a table name or a subquery.
#[derive(Debug, Clone)]
pub enum FromSource<V: Clone = crate::Value> {
    /// A simple table name (e.g., `"users"`).
    Table(String),
    /// A subquery (e.g., `(SELECT ... FROM orders WHERE ...)`).
    Subquery(Box<SelectTree<V>>),
}

/// FROM clause with optional alias.
#[derive(Debug, Clone)]
pub struct FromClause<V: Clone = crate::Value> {
    pub source: FromSource<V>,
    pub alias: Option<String>,
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
        }
    }
}

/// What the SELECT clause looks like.
#[derive(Debug, Clone)]
pub enum SelectClause {
    /// SELECT [DISTINCT] * or SELECT [DISTINCT] col1, col2, ...
    Columns {
        items: Vec<SelectItem>,
        distinct: bool,
    },
}

// ── Token enums ──

/// Token for SELECT query construction.
#[derive(Debug, Clone)]
pub enum SelectToken<V: Clone = crate::Value> {
    Select(SelectClause),
    From(FromClause<V>),
    Join {
        clause: JoinClause,
        subquery: Option<Box<SelectTree<V>>>,
    },
    Where(Vec<WhereEntry<V>>),
    GroupBy(Vec<String>),
    Having(Vec<WhereEntry<V>>),
    OrderBy(Vec<OrderByClause>),
    Limit(u64),
    Offset(u64),
    LockFor(String),
    /// Raw SQL fragment (no binds).
    Raw(String),
    /// A sub-SELECT within a compound query (UNION/INTERSECT/EXCEPT).
    SubSelect(Box<SelectTree<V>>),
    /// Open parenthesis `(`. Paired with `CloseParen`.
    /// Used to wrap sub-selects that need parentheses in compound queries.
    OpenParen,
    /// Close parenthesis `)`. Paired with `OpenParen`.
    CloseParen,
    /// Set operation keyword (UNION, UNION ALL, INTERSECT, EXCEPT, etc.).
    SetOperator(crate::SetOp),
}

/// Token for INSERT query construction.
///
/// **Token ordering**: `InsertInto` must appear before `Values` or `SelectSource`,
/// as the renderer extracts table/column metadata from it.
/// Typical sequences:
/// - `[InsertInto, Values, KeywordAssignments?]`
/// - `[InsertInto, SelectSource]`
#[derive(Debug, Clone)]
pub enum InsertToken<V: Clone = crate::Value> {
    /// INSERT INTO header with table name, columns, and expression columns.
    InsertInto {
        table: String,
        columns: Vec<String>,
        col_exprs: Vec<(String, String)>,
    },
    /// Explicit value rows for INSERT ... VALUES (...), (...).
    Values(Vec<Vec<V>>),
    /// A subquery source for INSERT ... SELECT ....
    SelectSource(Box<SelectTree<V>>),
    /// Raw SQL fragment (no binds).
    Raw(String),
    /// SET-style assignments (e.g., ON DUPLICATE KEY UPDATE).
    KeywordAssignments {
        keyword: String,
        sets: Vec<crate::SetClause<V>>,
    },
}

/// Token for UPDATE query construction.
#[derive(Debug, Clone)]
pub enum UpdateToken<V: Clone = crate::Value> {
    Update {
        table: String,
        alias: Option<String>,
    },
    Set(Vec<crate::SetClause<V>>),
    Where(Vec<WhereEntry<V>>),
    /// Raw SQL fragment (no binds). Dialect crates use this for
    /// dialect-specific clauses like ORDER BY and LIMIT.
    Raw(String),
}

/// Token for DELETE query construction.
#[derive(Debug, Clone)]
pub enum DeleteToken<V: Clone = crate::Value> {
    DeleteFrom {
        table: String,
        alias: Option<String>,
    },
    Where(Vec<WhereEntry<V>>),
    /// Raw SQL fragment (no binds). Dialect crates use this for
    /// dialect-specific clauses like ORDER BY and LIMIT.
    Raw(String),
}

/// AST for a SELECT query, generic over bind value type.
///
/// Represented as a list of tokens that the renderer processes in order.
/// Compound queries (UNION, INTERSECT, EXCEPT) are expressed as
/// `[SubSelect(A), SetOperator(Union), SubSelect(B), OrderBy(..), Limit(..)]`.
#[derive(Debug, Clone)]
pub struct SelectTree<V: Clone = crate::Value> {
    pub tokens: Vec<SelectToken<V>>,
}

impl<V: Clone> SelectTree<V> {
    /// Returns true if this tree contains tokens (ORDER BY, LIMIT, OFFSET, FOR)
    /// that require parentheses when used as a sub-select in compound queries.
    pub fn needs_parentheses(&self) -> bool {
        self.tokens.iter().any(|t| {
            matches!(
                t,
                SelectToken::OrderBy(_)
                    | SelectToken::Limit(_)
                    | SelectToken::Offset(_)
                    | SelectToken::LockFor(_)
            )
        })
    }

    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> SelectTree<U> {
        SelectTree {
            tokens: self
                .tokens
                .into_iter()
                .map(|token| match token {
                    SelectToken::Select(s) => SelectToken::Select(s),
                    SelectToken::From(from) => SelectToken::From(from.map_values(f)),
                    SelectToken::Join { clause, subquery } => SelectToken::Join {
                        clause,
                        subquery: subquery.map(|sq| Box::new(sq.map_values(f))),
                    },
                    SelectToken::Where(wheres) => {
                        SelectToken::Where(wheres.into_iter().map(|w| w.map_values(f)).collect())
                    }
                    SelectToken::GroupBy(g) => SelectToken::GroupBy(g),
                    SelectToken::Having(havings) => {
                        SelectToken::Having(havings.into_iter().map(|w| w.map_values(f)).collect())
                    }
                    SelectToken::OrderBy(o) => SelectToken::OrderBy(o),
                    SelectToken::Limit(n) => SelectToken::Limit(n),
                    SelectToken::Offset(n) => SelectToken::Offset(n),
                    SelectToken::LockFor(s) => SelectToken::LockFor(s),
                    SelectToken::Raw(s) => SelectToken::Raw(s),
                    SelectToken::SubSelect(sub) => {
                        SelectToken::SubSelect(Box::new(sub.map_values(f)))
                    }
                    SelectToken::OpenParen => SelectToken::OpenParen,
                    SelectToken::CloseParen => SelectToken::CloseParen,
                    SelectToken::SetOperator(op) => SelectToken::SetOperator(op),
                })
                .collect(),
        }
    }
}

// ── Build tree from SelectQuery ──

impl<V: Clone + std::fmt::Debug> SelectTree<V> {
    pub fn from_query(query: &crate::SelectQuery<V>) -> Self {
        Self::from_query_owned(query.clone())
    }

    /// Convert a SelectQuery into a SelectTree by moving fields instead of cloning.
    pub fn from_query_owned(query: crate::SelectQuery<V>) -> Self {
        if !query.set_operations.is_empty() {
            let mut tokens = Vec::new();

            for (i, (op, part)) in query.set_operations.into_iter().enumerate() {
                if i > 0 {
                    tokens.push(SelectToken::SetOperator(op));
                }
                let sub = SelectTree::from_query_owned(part);
                if sub.needs_parentheses() {
                    tokens.push(SelectToken::OpenParen);
                    tokens.push(SelectToken::SubSelect(Box::new(sub)));
                    tokens.push(SelectToken::CloseParen);
                } else {
                    tokens.push(SelectToken::SubSelect(Box::new(sub)));
                }
            }

            if !query.order_bys.is_empty() {
                tokens.push(SelectToken::OrderBy(query.order_bys));
            }
            if let Some(n) = query.limit_val {
                tokens.push(SelectToken::Limit(n));
            }
            if let Some(n) = query.offset_val {
                tokens.push(SelectToken::Offset(n));
            }

            return SelectTree { tokens };
        }

        let mut tokens = Vec::new();

        tokens.push(SelectToken::Select(SelectClause::Columns {
            items: query.selects,
            distinct: query.distinct,
        }));

        let source = match query.from_subquery {
            Some(sq) => FromSource::Subquery(sq),
            None => FromSource::Table(query.table),
        };
        tokens.push(SelectToken::From(FromClause {
            source,
            alias: query.table_alias,
        }));

        let join_count = query.joins.len();
        debug_assert!(
            query.join_subqueries.len() <= join_count,
            "join_subqueries must not exceed joins length"
        );
        let mut join_subqueries = query.join_subqueries;
        join_subqueries.resize_with(join_count, || None);

        for (i, join) in query.joins.into_iter().enumerate() {
            tokens.push(SelectToken::Join {
                clause: join,
                subquery: join_subqueries[i].take(),
            });
        }

        if !query.wheres.is_empty() {
            tokens.push(SelectToken::Where(query.wheres));
        }

        if !query.group_bys.is_empty() {
            tokens.push(SelectToken::GroupBy(query.group_bys));
        }

        if !query.havings.is_empty() {
            tokens.push(SelectToken::Having(query.havings));
        }

        if !query.order_bys.is_empty() {
            tokens.push(SelectToken::OrderBy(query.order_bys));
        }

        if let Some(n) = query.limit_val {
            tokens.push(SelectToken::Limit(n));
        }

        if let Some(n) = query.offset_val {
            tokens.push(SelectToken::Offset(n));
        }

        if let Some(lock) = query.lock_for {
            tokens.push(SelectToken::LockFor(lock));
        }

        SelectTree { tokens }
    }
}

/// AST for an UPDATE statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct UpdateTree<V: Clone = crate::Value> {
    pub tokens: Vec<UpdateToken<V>>,
}

impl<V: Clone> UpdateTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> UpdateTree<U> {
        UpdateTree {
            tokens: self
                .tokens
                .into_iter()
                .map(|token| match token {
                    UpdateToken::Update { table, alias } => UpdateToken::Update { table, alias },
                    UpdateToken::Set(sets) => UpdateToken::Set(
                        sets.into_iter()
                            .map(|s| match s {
                                crate::SetClause::Value(col, val) => {
                                    crate::SetClause::Value(col, f(val))
                                }
                                crate::SetClause::Expr(e) => crate::SetClause::Expr(e),
                            })
                            .collect(),
                    ),
                    UpdateToken::Where(wheres) => {
                        UpdateToken::Where(wheres.into_iter().map(|w| w.map_values(f)).collect())
                    }
                    UpdateToken::Raw(s) => UpdateToken::Raw(s),
                })
                .collect(),
        }
    }
}

/// AST for an INSERT statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct InsertTree<V: Clone = crate::Value> {
    pub tokens: Vec<InsertToken<V>>,
}

impl<V: Clone> InsertTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> InsertTree<U> {
        InsertTree {
            tokens: self
                .tokens
                .into_iter()
                .map(|token| match token {
                    InsertToken::InsertInto {
                        table,
                        columns,
                        col_exprs,
                    } => InsertToken::InsertInto {
                        table,
                        columns,
                        col_exprs,
                    },
                    InsertToken::Values(rows) => InsertToken::Values(
                        rows.into_iter()
                            .map(|row| row.into_iter().map(&f).collect())
                            .collect(),
                    ),
                    InsertToken::SelectSource(sub) => {
                        InsertToken::SelectSource(Box::new(sub.map_values(f)))
                    }
                    InsertToken::Raw(s) => InsertToken::Raw(s),
                    InsertToken::KeywordAssignments { keyword, sets } => {
                        InsertToken::KeywordAssignments {
                            keyword,
                            sets: sets
                                .into_iter()
                                .map(|s| match s {
                                    crate::SetClause::Value(col, val) => {
                                        crate::SetClause::Value(col, f(val))
                                    }
                                    crate::SetClause::Expr(e) => crate::SetClause::Expr(e),
                                })
                                .collect(),
                        }
                    }
                })
                .collect(),
        }
    }
}

/// AST for a DELETE statement, generic over bind value type.
#[derive(Debug, Clone)]
pub struct DeleteTree<V: Clone = crate::Value> {
    pub tokens: Vec<DeleteToken<V>>,
}

impl<V: Clone> DeleteTree<V> {
    /// Transform all bind values in this tree.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> DeleteTree<U> {
        DeleteTree {
            tokens: self
                .tokens
                .into_iter()
                .map(|token| match token {
                    DeleteToken::DeleteFrom { table, alias } => {
                        DeleteToken::DeleteFrom { table, alias }
                    }
                    DeleteToken::Where(wheres) => {
                        DeleteToken::Where(wheres.into_iter().map(|w| w.map_values(f)).collect())
                    }
                    DeleteToken::Raw(s) => DeleteToken::Raw(s),
                })
                .collect(),
        }
    }
}
