use crate::{
    AggregateExpr, AggregateFunc, Col, JoinClause, JoinCondition, JoinType, OrderByClause,
    SelectItem, SortDir, WhereClause, WhereEntry,
};

pub mod delete;
pub mod pipe;
pub mod standard;
pub mod update;

use crate::tree::{FromClause, FromSource, SelectTree, UnionTree};

/// Configuration for rendering SQL from trees.
pub struct RenderConfig<'a> {
    pub ph: &'a dyn Fn(usize) -> String,
    pub qi: &'a dyn Fn(&str) -> String,
    /// When true, backslashes inside SQL string literals are doubled (`\\`).
    /// MySQL requires this because `\` is an escape character in string literals
    /// by default (when `NO_BACKSLASH_ESCAPES` is not set).
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
    fn render_union<V: Clone>(&self, tree: &UnionTree<V>, cfg: &RenderConfig) -> (String, Vec<V>);
}

// ── Shared rendering helpers (crate-visible for standard/pipe modules) ──

pub(super) fn set_op_keyword(op: &crate::SetOp) -> &'static str {
    match op {
        crate::SetOp::Union => "UNION",
        crate::SetOp::UnionAll => "UNION ALL",
    }
}

pub(super) fn append_order_by(
    sql: &mut String,
    order_bys: &[OrderByClause],
    cfg: &RenderConfig,
    sep: &str,
) {
    if let Some(clause) = render_order_by(order_bys, cfg) {
        sql.push_str(sep);
        sql.push_str(&clause);
    }
}

/// Append LIMIT/OFFSET as separate space-separated clauses (standard SQL style).
pub(super) fn append_limit_offset_flat(sql: &mut String, limit: Option<u64>, offset: Option<u64>) {
    let (l, o) = render_limit_offset(limit, offset);
    if let Some(l) = l {
        sql.push_str(&format!(" {}", l));
    }
    if let Some(o) = o {
        sql.push_str(&format!(" {}", o));
    }
}

/// Append LIMIT/OFFSET as a single pipe stage (pipe SQL style).
pub(super) fn append_limit_offset_pipe(sql: &mut String, limit: Option<u64>, offset: Option<u64>) {
    let (l, o) = render_limit_offset(limit, offset);
    let mut lo_parts = Vec::new();
    if let Some(l) = l {
        lo_parts.push(l);
    }
    if let Some(o) = o {
        lo_parts.push(o);
    }
    if !lo_parts.is_empty() {
        sql.push_str(&format!(" |> {}", lo_parts.join(" ")));
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

pub(super) fn render_aggregate_expr(expr: &AggregateExpr, cfg: &RenderConfig) -> String {
    let func_str = match &expr.expr {
        AggregateFunc::CountAll => "COUNT(*)".to_string(),
        AggregateFunc::Count(col) => format!("COUNT({})", (cfg.qi)(col)),
        AggregateFunc::Sum(col) => format!("SUM({})", (cfg.qi)(col)),
        AggregateFunc::Avg(col) => format!("AVG({})", (cfg.qi)(col)),
        AggregateFunc::Min(col) => format!("MIN({})", (cfg.qi)(col)),
        AggregateFunc::Max(col) => format!("MAX({})", (cfg.qi)(col)),
        AggregateFunc::Expr(raw) => raw.to_string(),
    };
    match &expr.alias {
        Some(alias) => format!("{} AS {}", func_str, (cfg.qi)(alias)),
        None => func_str,
    }
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
    for suffix in &from.table_suffix {
        s.push(' ');
        s.push_str(suffix);
    }
    s
}

fn render_join_col(col: &crate::JoinCol, cfg: &RenderConfig) -> String {
    match &col.table {
        Some(table) => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(&col.col)),
        None => (cfg.qi)(&col.col),
    }
}

pub(super) fn render_join_condition(cond: &JoinCondition, cfg: &RenderConfig) -> String {
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
                .map(|c| render_join_condition(c, cfg))
                .collect();
            parts.join(" AND ")
        }
        JoinCondition::Using(_) => unreachable!("Using is handled in render_joins"),
        JoinCondition::Expr(raw) => raw.to_string(),
    }
}

fn render_join_table(table: &str, alias: &Option<String>, cfg: &RenderConfig) -> String {
    match alias {
        Some(a) => format!("{} AS {}", (cfg.qi)(table), (cfg.qi)(a)),
        None => (cfg.qi)(table),
    }
}

pub(super) fn render_joins<V: Clone>(
    joins: &[JoinClause],
    join_subqueries: &[Option<Box<SelectTree<V>>>],
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> Vec<String> {
    joins
        .iter()
        .enumerate()
        .map(|(i, j)| {
            let keyword = match &j.join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Custom(s) => s.as_str(),
            };
            let table = if let Some(Some(sub)) = join_subqueries.get(i) {
                let sub_sql = render_subquery_sql(sub, cfg, binds);
                match &j.alias {
                    Some(a) => format!("({}) AS {}", sub_sql, (cfg.qi)(a)),
                    None => format!("({})", sub_sql),
                }
            } else {
                render_join_table(&j.table, &j.alias, cfg)
            };
            if let JoinCondition::Using(cols) = &j.condition {
                let quoted: Vec<String> = cols.iter().map(|c| (cfg.qi)(c)).collect();
                return format!("{} {} USING ({})", keyword, table, quoted.join(", "));
            }
            format!(
                "{} {} ON {}",
                keyword,
                table,
                render_join_condition(&j.condition, cfg)
            )
        })
        .collect()
}

pub(super) fn render_select_columns(items: &[SelectItem], cfg: &RenderConfig) -> String {
    if items.is_empty() {
        "SELECT *".to_string()
    } else {
        let quoted: Vec<String> = items.iter().map(|c| render_select_item(c, cfg)).collect();
        format!("SELECT {}", quoted.join(", "))
    }
}

// ── Private helpers ──

fn render_col_ref(col: &Col, cfg: &RenderConfig) -> String {
    match &col.table {
        Some(table) => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(&col.column)),
        None => (cfg.qi)(&col.column),
    }
}

fn render_select_item(item: &SelectItem, cfg: &RenderConfig) -> String {
    match item {
        SelectItem::Col(col) => {
            let base = render_col_ref(col, cfg);
            match &col.alias {
                Some(alias) => format!("{} AS {}", base, (cfg.qi)(alias)),
                None => base,
            }
        }
        SelectItem::Expr { raw, alias } => match alias {
            Some(alias) => format!("{} AS {}", raw, (cfg.qi)(alias)),
            None => raw.to_string(),
        },
    }
}

/// Render the SELECT clause (columns or aggregate expressions) as a string.
/// Shared by `render_select_core` and CTE-aware rendering in `standard.rs`.
pub(super) fn render_select_clause(
    select: &crate::tree::SelectClause,
    cfg: &RenderConfig,
) -> String {
    use crate::tree::SelectClause;

    match select {
        SelectClause::Columns(cols) => render_select_columns(cols, cfg),
        SelectClause::Aggregate { group_bys, exprs } => {
            let mut items = Vec::new();
            for col in group_bys {
                items.push((cfg.qi)(col));
            }
            for expr in exprs {
                items.push(render_aggregate_expr(expr, cfg));
            }
            format!("SELECT {}", items.join(", "))
        }
    }
}

/// Render the core of a SELECT statement (SELECT, FROM, JOINs, WHERE, GROUP BY, HAVING)
/// without ORDER BY / LIMIT / OFFSET. Shared by StandardSqlRenderer and subquery rendering.
pub(super) fn render_select_core<V: Clone>(
    tree: &SelectTree<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let mut parts = Vec::new();

    parts.push(render_select_clause(&tree.select, cfg));

    parts.push(render_from(&tree.from, cfg, binds));

    for join_sql in render_joins(&tree.joins, &tree.join_subqueries, cfg, binds) {
        parts.push(join_sql);
    }

    if let Some(where_sql) = render_wheres(&tree.wheres, cfg, binds) {
        parts.push(format!("WHERE {}", where_sql));
    }

    if let crate::tree::SelectClause::Aggregate { group_bys, .. } = &tree.select
        && !group_bys.is_empty()
    {
        let cols: Vec<String> = group_bys.iter().map(|c| (cfg.qi)(c)).collect();
        parts.push(format!("GROUP BY {}", cols.join(", ")));
    }

    if let Some(having_sql) = render_wheres(&tree.havings, cfg, binds) {
        parts.push(format!("HAVING {}", having_sql));
    }

    parts.join(" ")
}

/// Render a SelectTree as standard SQL for use in subqueries.
/// Uses the shared binds accumulator so placeholder indices are correct.
///
/// Always renders standard SQL (not pipe syntax) because subqueries appear
/// inside parentheses where pipe syntax would be invalid.
fn render_subquery_sql<V: Clone>(
    tree: &SelectTree<V>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> String {
    let mut sql = render_select_core(tree, cfg, binds);
    append_order_by(&mut sql, &tree.order_bys, cfg, " ");
    append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
    sql
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

fn render_order_by(order_bys: &[OrderByClause], cfg: &RenderConfig) -> Option<String> {
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
            OrderByClause::Expr(raw) => raw.to_string(),
        })
        .collect();
    Some(format!("ORDER BY {}", clauses.join(", ")))
}

fn render_limit_offset(
    limit: Option<u64>,
    offset: Option<u64>,
) -> (Option<String>, Option<String>) {
    (
        limit.map(|n| format!("LIMIT {}", n)),
        offset.map(|n| format!("OFFSET {}", n)),
    )
}

pub fn append_lock_clause(sql: &mut String, lock_for: Option<&str>) {
    if let Some(clause) = lock_for {
        sql.push_str(" FOR ");
        sql.push_str(clause);
    }
}
