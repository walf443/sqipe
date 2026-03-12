use crate::{
    AggregateExpr, AggregateFunc, ColRef, JoinClause, JoinCondition, JoinType, OrderByClause,
    SortDir, Value, WhereClause, WhereEntry,
};

pub mod pipe;
pub mod standard;

use crate::tree::{FromClause, SelectTree, UnionTree};

/// Configuration for rendering SQL from trees.
pub struct RenderConfig<'a> {
    pub ph: &'a dyn Fn(usize) -> String,
    pub qi: &'a dyn Fn(&str) -> String,
}

/// Trait for SQL rendering strategies.
pub trait Renderer {
    fn render_select(&self, tree: &SelectTree, cfg: &RenderConfig) -> (String, Vec<Value>);
    fn render_union(&self, tree: &UnionTree, cfg: &RenderConfig) -> (String, Vec<Value>);
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

pub(super) fn render_wheres(
    wheres: &[WhereEntry],
    cfg: &RenderConfig,
    binds: &mut Vec<Value>,
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
        AggregateFunc::Expr(raw) => raw.clone(),
    };
    match &expr.alias {
        Some(alias) => format!("{} AS {}", func_str, (cfg.qi)(alias)),
        None => func_str,
    }
}

pub(super) fn render_from(from: &FromClause, cfg: &RenderConfig) -> String {
    let mut s = format!("FROM {}", (cfg.qi)(&from.table));
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
            format!(
                "{}.{} = {}",
                (cfg.qi)(&left.table),
                (cfg.qi)(&left.col),
                render_join_col(right, cfg)
            )
        }
        JoinCondition::And(conditions) => {
            let parts: Vec<String> = conditions
                .iter()
                .map(|c| render_join_condition(c, cfg))
                .collect();
            parts.join(" AND ")
        }
        JoinCondition::Using(_) => unreachable!("Using is handled in render_joins"),
    }
}

fn render_join_table(table: &str, alias: &Option<String>, cfg: &RenderConfig) -> String {
    match alias {
        Some(a) => format!("{} AS {}", (cfg.qi)(table), (cfg.qi)(a)),
        None => (cfg.qi)(table),
    }
}

pub(super) fn render_joins(joins: &[JoinClause], cfg: &RenderConfig) -> Vec<String> {
    joins
        .iter()
        .map(|j| {
            let keyword = match j.join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
            };
            let table = render_join_table(&j.table, &j.alias, cfg);
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

pub(super) fn render_select_columns(cols: &[String], cfg: &RenderConfig) -> String {
    if cols.is_empty() {
        "SELECT *".to_string()
    } else {
        let quoted: Vec<String> = cols.iter().map(|c| (cfg.qi)(c)).collect();
        format!("SELECT {}", quoted.join(", "))
    }
}

// ── Private helpers ──

fn render_col_ref(col: &ColRef, cfg: &RenderConfig) -> String {
    match col {
        ColRef::Simple(name) => (cfg.qi)(name),
        ColRef::Qualified { table, col } => format!("{}.{}", (cfg.qi)(table), (cfg.qi)(col)),
    }
}

fn render_where_clause(
    clause: &WhereClause,
    is_top_level: bool,
    cfg: &RenderConfig,
    binds: &mut Vec<Value>,
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
    }
}

fn render_order_by(order_bys: &[OrderByClause], cfg: &RenderConfig) -> Option<String> {
    if order_bys.is_empty() {
        return None;
    }
    let clauses: Vec<String> = order_bys
        .iter()
        .map(|o| {
            let dir = match o.dir {
                SortDir::Asc => "ASC",
                SortDir::Desc => "DESC",
            };
            format!("{} {}", (cfg.qi)(&o.col), dir)
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
