use crate::{
    Col, JoinClause, JoinCondition, JoinType, OrderByClause, SelectFunc, SelectItem, SortDir,
    WhereClause, WhereEntry, WindowSpec,
};

pub mod delete;
pub mod insert;
pub mod standard;
pub mod update;

use crate::tree::{FromClause, FromSource, SelectToken, SelectTree};

/// Configuration for rendering SQL from trees.
pub struct RenderConfig<'a> {
    pub ph: &'a dyn Fn(usize) -> String,
    pub qi: &'a dyn Fn(&str) -> String,
    /// When true, backslashes inside SQL string literals are doubled (`\\`).
    /// MySQL requires this by default (when `NO_BACKSLASH_ESCAPES` is not set).
    pub backslash_escape: bool,
}

impl<'a> RenderConfig<'a> {
    /// Build a `RenderConfig` from pre-built closures and a [`Dialect`](crate::Dialect).
    pub fn from_dialect(
        ph: &'a dyn Fn(usize) -> String,
        qi: &'a dyn Fn(&str) -> String,
        dialect: &dyn crate::Dialect,
    ) -> Self {
        Self {
            ph,
            qi,
            backslash_escape: dialect.backslash_escape(),
        }
    }
}

/// Trait for SQL rendering strategies.
pub trait Renderer {
    fn render_select<V: Clone>(&self, tree: &SelectTree<V>, cfg: &RenderConfig)
    -> (String, Vec<V>);
}

// ── Shared rendering helpers (crate-visible for standard module) ──

pub(super) fn set_op_keyword(op: &crate::SetOp) -> &'static str {
    match op {
        crate::SetOp::Union => "UNION",
        crate::SetOp::UnionAll => "UNION ALL",
        crate::SetOp::Intersect => "INTERSECT",
        crate::SetOp::IntersectAll => "INTERSECT ALL",
        crate::SetOp::Except => "EXCEPT",
        crate::SetOp::ExceptAll => "EXCEPT ALL",
    }
}

pub(super) fn render_wheres<V: Clone>(
    wheres: &[WhereEntry<V>],
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> Option<String> {
    if wheres.is_empty() {
        return None;
    }

    let single = wheres.len() == 1;
    let mut sql = String::new();

    for (i, entry) in wheres.iter().enumerate() {
        let (connector, clause) = match entry {
            WhereEntry::And(c) => ("AND", c),
            WhereEntry::Or(c) => ("OR", c),
        };

        if i > 0 {
            sql.push_str(&format!(" {} ", connector));
        }

        sql.push_str(&render_where_clause(clause, single, cfg, binds));
    }

    Some(sql)
}

pub(super) fn render_from<V: Clone>(
    from: &FromClause<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let mut s = match &from.source {
        FromSource::Table(table) => format!("FROM {}", (cfg.qi)(table)),
        FromSource::Subquery(sub) => {
            let sub_sql = render_subquery_sql(sub, cfg, binds);
            format!("FROM ({})", sub_sql)
        }
    };
    if let Some(alias) = &from.alias {
        s.push_str(&format!(" AS {}", (cfg.qi)(alias)));
    }
    s
}

fn render_join_col(col: &crate::JoinCol, cfg: &RenderConfig) -> String {
    match &col.table {
        Some(table) => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(&col.col)),
        None => (cfg.qi)(&col.col),
    }
}

pub(super) fn render_join_condition<V: Clone>(
    cond: &JoinCondition<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    match cond {
        JoinCondition::ColEq { left, right } => {
            let left_str = match &left.table {
                Some(table) => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(&left.column)),
                None => (cfg.qi)(&left.column),
            };
            format!("{} = {}", left_str, render_join_col(right, cfg))
        }
        JoinCondition::And(conditions) => {
            let parts: Vec<String> = conditions
                .iter()
                .map(|c| render_join_condition(c, cfg, binds))
                .collect();
            parts.join(" AND ")
        }
        JoinCondition::Using(_) => unreachable!("Using is handled in render_join"),
        JoinCondition::Expr(raw) => raw.render(cfg, binds),
    }
}

fn render_join_table(table: &str, alias: &Option<String>, cfg: &RenderConfig) -> String {
    match alias {
        Some(a) => format!("{} AS {}", (cfg.qi)(table), (cfg.qi)(a)),
        None => (cfg.qi)(table),
    }
}

/// Render a single JOIN token.
pub(super) fn render_join<V: Clone>(
    join: &JoinClause<V>,
    subquery: &Option<Box<SelectTree<V>>>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let keyword = match &join.join_type {
        JoinType::Inner => "INNER JOIN",
        JoinType::Left => "LEFT JOIN",
        JoinType::Custom(s) => s.as_str(),
    };
    let table = if let Some(sub) = subquery {
        let sub_sql = render_subquery_sql(sub, cfg, binds);
        match &join.alias {
            Some(a) => format!("({}) AS {}", sub_sql, (cfg.qi)(a)),
            None => format!("({})", sub_sql),
        }
    } else {
        render_join_table(&join.table, &join.alias, cfg)
    };
    if let JoinCondition::Using(cols) = &join.condition {
        let quoted: Vec<String> = cols.iter().map(|c| (cfg.qi)(c)).collect();
        return format!("{} {} USING ({})", keyword, table, quoted.join(", "));
    }
    format!(
        "{} {} ON {}",
        keyword,
        table,
        render_join_condition(&join.condition, cfg, binds)
    )
}

pub(super) fn render_select_columns<V: Clone>(
    items: &[SelectItem<V>],
    distinct: bool,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let keyword = if distinct {
        "SELECT DISTINCT"
    } else {
        "SELECT"
    };
    if items.is_empty() {
        format!("{} *", keyword)
    } else {
        let quoted: Vec<String> = items
            .iter()
            .map(|c| render_select_item(c, cfg, binds))
            .collect();
        format!("{} {}", keyword, quoted.join(", "))
    }
}

// ── Private helpers ──

fn render_col_ref(col: &Col, cfg: &RenderConfig) -> String {
    if let Some((func, inner_col)) = &col.aggregate {
        let arg = match (func, inner_col) {
            (SelectFunc::CountOne, _) => "1".to_string(),
            (_, Some(inner)) => render_col_ref(inner, cfg),
            (_, None) => "*".to_string(),
        };
        return format!("{}({})", func.as_str(), arg);
    }
    debug_assert!(
        !col.column.is_empty(),
        "Col has no column name and no aggregate function"
    );
    match &col.table {
        Some(table) => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(&col.column)),
        None => (cfg.qi)(&col.column),
    }
}

fn render_select_item<V: Clone>(
    item: &SelectItem<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    match item {
        SelectItem::Col(col) => {
            let base = render_col_ref(col, cfg);
            match &col.alias {
                Some(alias) => format!("{} AS {}", base, (cfg.qi)(alias)),
                None => base,
            }
        }
        SelectItem::Expr { raw, alias } => {
            let rendered = raw.render(cfg, binds);
            match alias {
                Some(alias) => format!("{} AS {}", rendered, (cfg.qi)(alias)),
                None => rendered,
            }
        }
        SelectItem::Function { func, col, alias } => {
            let arg = match (func, col) {
                (SelectFunc::CountOne, _) => "1".to_string(),
                (_, Some(col)) => render_col_ref(col, cfg),
                (_, None) => "*".to_string(),
            };
            let base = format!("{}({})", func.as_str(), arg);
            match alias {
                Some(alias) => format!("{} AS {}", base, (cfg.qi)(alias)),
                None => base,
            }
        }
        SelectItem::WindowFunction {
            func,
            col,
            window,
            alias,
        } => {
            let arg = match col {
                Some(col) => render_col_ref(col, cfg),
                None => String::new(),
            };
            let func_call = format!("{}({})", func.as_str(), arg);
            let base = match &window.name {
                Some(name) => format!("{} OVER {}", func_call, (cfg.qi)(name)),
                None => {
                    let over_clause = render_window_spec(window, cfg, binds);
                    format!("{} OVER ({})", func_call, over_clause)
                }
            };
            match alias {
                Some(alias) => format!("{} AS {}", base, (cfg.qi)(alias)),
                None => base,
            }
        }
    }
}

fn render_window_spec<V: Clone>(
    spec: &WindowSpec<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let mut parts = Vec::new();

    if !spec.partition_by.is_empty() {
        let cols: Vec<String> = spec
            .partition_by
            .iter()
            .map(|c| render_col_ref(c, cfg))
            .collect();
        parts.push(format!("PARTITION BY {}", cols.join(", ")));
    }

    if !spec.order_by.is_empty() {
        let clauses: Vec<String> = spec
            .order_by
            .iter()
            .map(|o| match o {
                OrderByClause::Col { col, dir } => {
                    let dir_str = match dir {
                        SortDir::Asc => "ASC",
                        SortDir::Desc => "DESC",
                    };
                    format!("{} {}", render_col_ref(col, cfg), dir_str)
                }
                OrderByClause::Expr(raw) => raw.render(cfg, binds),
            })
            .collect();
        parts.push(format!("ORDER BY {}", clauses.join(", ")));
    }

    parts.join(" ")
}

/// Render the SELECT clause as a string.
pub(super) fn render_select_clause<V: Clone>(
    select: &crate::tree::SelectClause<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    use crate::tree::SelectClause;

    match select {
        SelectClause::Columns { items, distinct } => {
            render_select_columns(items, *distinct, cfg, binds)
        }
    }
}

/// Render a SelectTree as standard SQL for use in subqueries.
/// Uses the shared binds accumulator so placeholder indices are correct.
pub(super) fn render_subquery_sql<V: Clone>(
    tree: &SelectTree<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let mut parts = Vec::new();
    render_select_tokens(&tree.tokens, cfg, binds, &mut parts);
    parts.join(" ")
}

/// Core token-walking logic shared by render_select and render_subquery_sql.
pub(super) fn render_select_tokens<V: Clone>(
    tokens: &[SelectToken<V>],
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
    parts: &mut Vec<String>,
) {
    let mut open_parens: usize = 0;
    for token in tokens {
        // OpenParen/CloseParen are handled specially to avoid spaces around parens.
        match token {
            SelectToken::OpenParen => {
                open_parens += 1;
                continue;
            }
            SelectToken::CloseParen => {
                debug_assert!(
                    !parts.is_empty(),
                    "CloseParen with no preceding parts to attach to"
                );
                if let Some(last) = parts.last_mut() {
                    last.push(')');
                }
                continue;
            }
            _ => {}
        }

        let rendered = match token {
            SelectToken::Select(clause) => Some(render_select_clause(clause, cfg, binds)),
            SelectToken::From(from) => Some(render_from(from, cfg, binds)),
            SelectToken::Join { clause, subquery } => {
                Some(render_join(clause, subquery, cfg, binds))
            }
            SelectToken::Where(wheres) => {
                render_wheres(wheres, cfg, binds).map(|where_sql| format!("WHERE {}", where_sql))
            }
            SelectToken::GroupBy(cols) => {
                if cols.is_empty() {
                    None
                } else {
                    let quoted: Vec<String> = cols.iter().map(|c| (cfg.qi)(c)).collect();
                    Some(format!("GROUP BY {}", quoted.join(", ")))
                }
            }
            SelectToken::Having(havings) => render_wheres(havings, cfg, binds)
                .map(|having_sql| format!("HAVING {}", having_sql)),
            SelectToken::OrderBy(obs) => render_order_by(obs, cfg, binds),
            SelectToken::Limit(n) => Some(format!("LIMIT {}", n)),
            SelectToken::Offset(n) => Some(format!("OFFSET {}", n)),
            SelectToken::LockFor(s) => Some(format!("FOR {}", s)),
            SelectToken::Raw(s) => Some(s.clone()),
            SelectToken::SubSelect(sub) => {
                let mut sub_parts = Vec::new();
                render_select_tokens(&sub.tokens, cfg, binds, &mut sub_parts);
                Some(sub_parts.join(" "))
            }
            SelectToken::SetOperator(op) => Some(set_op_keyword(op).to_string()),
            SelectToken::Window(defs) => {
                if defs.is_empty() {
                    None
                } else {
                    let parts: Vec<String> = defs
                        .iter()
                        .map(|(name, spec)| {
                            let spec_sql = render_window_spec(spec, cfg, binds);
                            format!("{} AS ({})", (cfg.qi)(name), spec_sql)
                        })
                        .collect();
                    Some(format!("WINDOW {}", parts.join(", ")))
                }
            }
            SelectToken::With(ctes) => {
                if ctes.is_empty() {
                    None
                } else {
                    let has_recursive = ctes.iter().any(|cte| cte.recursive);
                    let keyword = if has_recursive {
                        "WITH RECURSIVE"
                    } else {
                        "WITH"
                    };
                    let defs: Vec<String> = ctes
                        .iter()
                        .map(|cte| {
                            let col_list = if cte.columns.is_empty() {
                                String::new()
                            } else {
                                let quoted: Vec<String> =
                                    cte.columns.iter().map(|c| (cfg.qi)(c)).collect();
                                format!(" ({})", quoted.join(", "))
                            };
                            let sub_sql = render_subquery_sql(&cte.subquery, cfg, binds);
                            format!("{}{} AS ({})", (cfg.qi)(&cte.name), col_list, sub_sql)
                        })
                        .collect();
                    Some(format!("{} {}", keyword, defs.join(", ")))
                }
            }
            SelectToken::OpenParen | SelectToken::CloseParen => unreachable!(),
        };

        if let Some(mut s) = rendered {
            // Prepend pending open parentheses
            if open_parens > 0 {
                let prefix: String = std::iter::repeat_n('(', open_parens).collect();
                s = format!("{}{}", prefix, s);
                open_parens = 0;
            }
            parts.push(s);
        }
    }
    debug_assert_eq!(
        open_parens, 0,
        "unclosed OpenParen: {} open paren(s) remaining",
        open_parens
    );
}

fn render_where_clause<V: Clone>(
    clause: &WhereClause<V>,
    is_top_level: bool,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    match clause {
        WhereClause::Condition { col, op, val } => {
            binds.push(val.clone());
            let placeholder = (cfg.ph)(binds.len());
            format!(
                "{} {} {}",
                render_col_ref(col, cfg),
                op.as_str(),
                placeholder
            )
        }
        WhereClause::Between { col, low, high } => {
            binds.push(low.clone());
            let ph_low = (cfg.ph)(binds.len());
            binds.push(high.clone());
            let ph_high = (cfg.ph)(binds.len());
            format!(
                "{} BETWEEN {} AND {}",
                render_col_ref(col, cfg),
                ph_low,
                ph_high
            )
        }
        WhereClause::NotBetween { col, low, high } => {
            binds.push(low.clone());
            let ph_low = (cfg.ph)(binds.len());
            binds.push(high.clone());
            let ph_high = (cfg.ph)(binds.len());
            format!(
                "{} NOT BETWEEN {} AND {}",
                render_col_ref(col, cfg),
                ph_low,
                ph_high
            )
        }
        WhereClause::In { col: _, vals } if vals.is_empty() => "1 = 0".to_string(),
        WhereClause::In { col, vals } => {
            let placeholders: Vec<String> = vals
                .iter()
                .map(|v| {
                    binds.push(v.clone());
                    (cfg.ph)(binds.len())
                })
                .collect();
            format!(
                "{} IN ({})",
                render_col_ref(col, cfg),
                placeholders.join(", ")
            )
        }
        WhereClause::InSubQuery { col, sub } => {
            let sub_sql = render_subquery_sql(sub, cfg, binds);
            format!("{} IN ({})", render_col_ref(col, cfg), sub_sql)
        }
        WhereClause::NotIn { col: _, vals } if vals.is_empty() => "1 = 1".to_string(),
        WhereClause::NotIn { col, vals } => {
            let placeholders: Vec<String> = vals
                .iter()
                .map(|v| {
                    binds.push(v.clone());
                    (cfg.ph)(binds.len())
                })
                .collect();
            format!(
                "{} NOT IN ({})",
                render_col_ref(col, cfg),
                placeholders.join(", ")
            )
        }
        WhereClause::NotInSubQuery { col, sub } => {
            let sub_sql = render_subquery_sql(sub, cfg, binds);
            format!("{} NOT IN ({})", render_col_ref(col, cfg), sub_sql)
        }
        WhereClause::Like { col, expr, val } | WhereClause::NotLike { col, expr, val } => {
            binds.push(val.clone());
            let placeholder = (cfg.ph)(binds.len());
            let keyword = if matches!(clause, WhereClause::Like { .. }) {
                "LIKE"
            } else {
                "NOT LIKE"
            };
            let esc = expr.escape_char();
            let escaped = if cfg.backslash_escape && esc == '\\' {
                "\\\\".to_string()
            } else {
                esc.to_string()
            };
            format!(
                "{} {} {} ESCAPE '{}'",
                render_col_ref(col, cfg),
                keyword,
                placeholder,
                escaped
            )
        }
        WhereClause::Any(clauses) => {
            let parts: Vec<String> = clauses
                .iter()
                .map(|c| render_where_clause(c, false, cfg, binds))
                .collect();
            let joined = parts.join(" OR ");
            if is_top_level {
                joined
            } else {
                format!("({})", joined)
            }
        }
        WhereClause::All(clauses) => {
            let parts: Vec<String> = clauses
                .iter()
                .map(|c| render_where_clause(c, false, cfg, binds))
                .collect();
            let joined = parts.join(" AND ");
            if is_top_level {
                joined
            } else {
                format!("({})", joined)
            }
        }
        WhereClause::Not(clause) => {
            let inner = render_where_clause(clause, false, cfg, binds);
            format!("NOT ({})", inner)
        }
    }
}

/// Render an ORDER BY clause from a slice of `OrderByClause`.
///
/// Returns `None` if the slice is empty; otherwise returns `Some("ORDER BY ...")`.
/// Exposed publicly so dialect crates can reuse this for dialect-specific
/// ORDER BY support (e.g., MySQL's ORDER BY in UPDATE/DELETE).
pub fn render_order_by<V: Clone>(
    order_bys: &[OrderByClause<V>],
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> Option<String> {
    if order_bys.is_empty() {
        return None;
    }
    let clauses: Vec<String> = order_bys
        .iter()
        .map(|o| match o {
            OrderByClause::Col { col, dir } => {
                let dir_str = match dir {
                    SortDir::Asc => "ASC",
                    SortDir::Desc => "DESC",
                };
                format!("{} {}", render_col_ref(col, cfg), dir_str)
            }
            OrderByClause::Expr(raw) => raw.render(cfg, binds),
        })
        .collect();
    Some(format!("ORDER BY {}", clauses.join(", ")))
}
