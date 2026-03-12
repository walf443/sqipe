pub(crate) mod pipe;
pub(crate) mod standard;

use crate::{
    AggregateExpr, AggregateFunc, OrderByClause, Query, SortDir, Value,
    WhereClause, WhereEntry,
};

/// Internal config for SQL generation.
pub(crate) struct SqlConfig<'a> {
    pub ph: &'a dyn Fn(usize) -> String,
    pub qi: &'a dyn Fn(&str) -> String,
}

/// Default double-quote identifier quoting (SQL standard).
pub(crate) fn default_quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Trait for SQL building strategies (standard vs pipe).
pub(crate) trait SqlBuilder {
    /// Build the core body (no ORDER BY / LIMIT).
    fn build_core(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String;

    /// Build a query as a union part (wraps in parens if ORDER BY/LIMIT present).
    fn build_union_part(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String;
}

// ── Shared clause renderers ──

pub(crate) fn build_select_clause(selects: &[String], cfg: &SqlConfig) -> String {
    if selects.is_empty() {
        "SELECT *".to_string()
    } else {
        let cols: Vec<String> = selects.iter().map(|c| (cfg.qi)(c)).collect();
        format!("SELECT {}", cols.join(", "))
    }
}

pub(crate) fn render_aggregate_expr(expr: &AggregateExpr, cfg: &SqlConfig) -> String {
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

pub(crate) fn build_aggregate_select(
    group_bys: &[String],
    aggregates: &[AggregateExpr],
    cfg: &SqlConfig,
) -> String {
    let mut items = Vec::new();
    for col in group_bys {
        items.push((cfg.qi)(col));
    }
    for expr in aggregates {
        items.push(render_aggregate_expr(expr, cfg));
    }
    format!("SELECT {}", items.join(", "))
}

pub(crate) fn build_aggregate_pipe(
    aggregates: &[AggregateExpr],
    group_bys: &[String],
    cfg: &SqlConfig,
) -> String {
    let agg_exprs: Vec<String> = aggregates
        .iter()
        .map(|e| render_aggregate_expr(e, cfg))
        .collect();
    let mut clause = format!("AGGREGATE {}", agg_exprs.join(", "));
    if let Some(group_by) = build_group_by_clause(group_bys, cfg) {
        clause.push_str(&format!(" {}", group_by));
    }
    clause
}

pub(crate) fn build_group_by_clause(group_bys: &[String], cfg: &SqlConfig) -> Option<String> {
    if group_bys.is_empty() {
        return None;
    }
    let cols: Vec<String> = group_bys.iter().map(|c| (cfg.qi)(c)).collect();
    Some(format!("GROUP BY {}", cols.join(", ")))
}

pub(crate) fn build_order_by_clause(
    order_bys: &[OrderByClause],
    cfg: &SqlConfig,
) -> Option<String> {
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

pub(crate) fn build_limit_offset(
    limit_val: Option<u64>,
    offset_val: Option<u64>,
) -> (Option<String>, Option<String>) {
    (
        limit_val.map(|n| format!("LIMIT {}", n)),
        offset_val.map(|n| format!("OFFSET {}", n)),
    )
}

pub(crate) fn build_where(
    wheres: &[WhereEntry],
    cfg: &SqlConfig,
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

        let is_top_level = single;
        sql.push_str(&render_where_clause(clause, is_top_level, cfg, binds));
    }

    Some(sql)
}

fn render_where_clause(
    clause: &WhereClause,
    is_top_level: bool,
    cfg: &SqlConfig,
    binds: &mut Vec<Value>,
) -> String {
    match clause {
        WhereClause::Condition { col, op, val } => {
            binds.push(val.clone());
            let placeholder = (cfg.ph)(binds.len());
            format!("{} {} {}", (cfg.qi)(col), op.as_str(), placeholder)
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
