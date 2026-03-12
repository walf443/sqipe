use crate::{
    AggregateExpr, AggregateFunc, OrderByClause, SortDir, Value, WhereClause, WhereEntry,
};

/// FROM clause with optional dialect-specific modifiers appended after the table name.
#[derive(Debug, Clone)]
pub struct FromClause {
    pub table: String,
    /// Raw SQL fragments appended after the table name (e.g., "FORCE INDEX (idx)").
    /// Dialect crates populate this via tree transformation.
    pub table_suffix: Vec<String>,
}

/// What the SELECT clause looks like.
#[derive(Debug, Clone)]
pub enum SelectClause {
    /// SELECT * or SELECT col1, col2, ...
    Columns(Vec<String>),
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
    pub(crate) wheres: Vec<WhereEntry>,
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

/// Configuration for rendering SQL from trees.
pub struct RenderConfig<'a> {
    pub ph: &'a dyn Fn(usize) -> String,
    pub qi: &'a dyn Fn(&str) -> String,
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
                table_suffix: Vec::new(),
            },
            wheres: query.wheres.clone(),
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

// ── Shared rendering helpers ──

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

fn render_wheres(wheres: &[WhereEntry], cfg: &RenderConfig, binds: &mut Vec<Value>) -> Option<String> {
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

fn render_aggregate_expr(expr: &AggregateExpr, cfg: &RenderConfig) -> String {
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

fn render_from(from: &FromClause, cfg: &RenderConfig) -> String {
    let mut s = format!("FROM {}", (cfg.qi)(&from.table));
    for suffix in &from.table_suffix {
        s.push(' ');
        s.push_str(suffix);
    }
    s
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

fn render_limit_offset(limit: Option<u64>, offset: Option<u64>) -> (Option<String>, Option<String>) {
    (
        limit.map(|n| format!("LIMIT {}", n)),
        offset.map(|n| format!("OFFSET {}", n)),
    )
}

fn render_select_columns(cols: &[String], cfg: &RenderConfig) -> String {
    if cols.is_empty() {
        "SELECT *".to_string()
    } else {
        let quoted: Vec<String> = cols.iter().map(|c| (cfg.qi)(c)).collect();
        format!("SELECT {}", quoted.join(", "))
    }
}

// ── Standard SQL renderer ──

impl SelectTree {
    /// Render the core body (no ORDER BY / LIMIT) as standard SQL.
    fn render_standard_core(&self, cfg: &RenderConfig, binds: &mut Vec<Value>) -> String {
        let mut parts = Vec::new();

        // SELECT clause
        match &self.select {
            SelectClause::Columns(cols) => {
                parts.push(render_select_columns(cols, cfg));
            }
            SelectClause::Aggregate { group_bys, exprs } => {
                let mut items = Vec::new();
                for col in group_bys {
                    items.push((cfg.qi)(col));
                }
                for expr in exprs {
                    items.push(render_aggregate_expr(expr, cfg));
                }
                parts.push(format!("SELECT {}", items.join(", ")));
            }
        }

        // FROM clause
        parts.push(render_from(&self.from, cfg));

        // WHERE clause
        if let Some(where_sql) = render_wheres(&self.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        // GROUP BY clause
        if let SelectClause::Aggregate { group_bys, .. } = &self.select {
            if !group_bys.is_empty() {
                let cols: Vec<String> = group_bys.iter().map(|c| (cfg.qi)(c)).collect();
                parts.push(format!("GROUP BY {}", cols.join(", ")));
            }
        }

        parts.join(" ")
    }

    /// Render as a full standalone standard SQL query.
    pub fn render_standard(&self, cfg: &RenderConfig) -> (String, Vec<Value>) {
        let mut binds = Vec::new();
        let mut sql = self.render_standard_core(cfg, &mut binds);

        if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
            sql.push_str(&format!(" {}", order_by));
        }

        let (limit, offset) = render_limit_offset(self.limit, self.offset);
        if let Some(l) = limit {
            sql.push_str(&format!(" {}", l));
        }
        if let Some(o) = offset {
            sql.push_str(&format!(" {}", o));
        }

        (sql, binds)
    }

    /// Render as a standard SQL union part (wrapped in parens if has ORDER BY/LIMIT/OFFSET).
    fn render_standard_union_part(&self, cfg: &RenderConfig, binds: &mut Vec<Value>) -> String {
        let mut sql = self.render_standard_core(cfg, binds);
        let has_extra = !self.order_bys.is_empty()
            || self.limit.is_some()
            || self.offset.is_some();

        if has_extra {
            if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
                sql.push_str(&format!(" {}", order_by));
            }
            let (limit, offset) = render_limit_offset(self.limit, self.offset);
            if let Some(l) = limit {
                sql.push_str(&format!(" {}", l));
            }
            if let Some(o) = offset {
                sql.push_str(&format!(" {}", o));
            }
            sql = format!("({})", sql);
        }

        sql
    }

    /// Render the core body (no ORDER BY / LIMIT) as pipe SQL.
    fn render_pipe_core(&self, cfg: &RenderConfig, binds: &mut Vec<Value>) -> String {
        let mut parts = Vec::new();

        // FROM clause
        parts.push(render_from(&self.from, cfg));

        // WHERE clause
        if let Some(where_sql) = render_wheres(&self.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        // SELECT / AGGREGATE clause
        match &self.select {
            SelectClause::Columns(cols) => {
                parts.push(render_select_columns(cols, cfg));
            }
            SelectClause::Aggregate { group_bys, exprs } => {
                let agg_exprs: Vec<String> = exprs
                    .iter()
                    .map(|e| render_aggregate_expr(e, cfg))
                    .collect();
                let mut clause = format!("AGGREGATE {}", agg_exprs.join(", "));
                if !group_bys.is_empty() {
                    let cols: Vec<String> = group_bys.iter().map(|c| (cfg.qi)(c)).collect();
                    clause.push_str(&format!(" GROUP BY {}", cols.join(", ")));
                }
                parts.push(clause);
            }
        }

        parts.join(" |> ")
    }

    /// Render as a full standalone pipe SQL query.
    pub fn render_pipe(&self, cfg: &RenderConfig) -> (String, Vec<Value>) {
        let mut binds = Vec::new();
        let mut sql = self.render_pipe_core(cfg, &mut binds);

        if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
            sql.push_str(&format!(" |> {}", order_by));
        }

        let (limit, offset) = render_limit_offset(self.limit, self.offset);
        let mut lo_parts = Vec::new();
        if let Some(l) = limit {
            lo_parts.push(l);
        }
        if let Some(o) = offset {
            lo_parts.push(o);
        }
        if !lo_parts.is_empty() {
            sql.push_str(&format!(" |> {}", lo_parts.join(" ")));
        }

        (sql, binds)
    }

    /// Render as a pipe SQL union part (wrapped in parens if has ORDER BY/LIMIT/OFFSET).
    fn render_pipe_union_part(&self, cfg: &RenderConfig, binds: &mut Vec<Value>) -> String {
        let mut sql = self.render_pipe_core(cfg, binds);
        let has_extra = !self.order_bys.is_empty()
            || self.limit.is_some()
            || self.offset.is_some();

        if has_extra {
            if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
                sql.push_str(&format!(" |> {}", order_by));
            }
            let (limit, offset) = render_limit_offset(self.limit, self.offset);
            let mut lo_parts = Vec::new();
            if let Some(l) = limit {
                lo_parts.push(l);
            }
            if let Some(o) = offset {
                lo_parts.push(o);
            }
            if !lo_parts.is_empty() {
                sql.push_str(&format!(" |> {}", lo_parts.join(" ")));
            }
            sql = format!("({})", sql);
        }

        sql
    }
}

// ── Union tree renderers ──

impl UnionTree {
    pub fn render_standard(&self, cfg: &RenderConfig) -> (String, Vec<Value>) {
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, tree)) in self.parts.iter().enumerate() {
            if i > 0 {
                let keyword = match op {
                    crate::SetOp::Union => "UNION",
                    crate::SetOp::UnionAll => "UNION ALL",
                };
                sql.push_str(&format!(" {} ", keyword));
            }
            sql.push_str(&tree.render_standard_union_part(cfg, &mut binds));
        }

        if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
            sql.push_str(&format!(" {}", order_by));
        }

        let (limit, offset) = render_limit_offset(self.limit, self.offset);
        if let Some(l) = limit {
            sql.push_str(&format!(" {}", l));
        }
        if let Some(o) = offset {
            sql.push_str(&format!(" {}", o));
        }

        (sql, binds)
    }

    pub fn render_pipe(&self, cfg: &RenderConfig) -> (String, Vec<Value>) {
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, tree)) in self.parts.iter().enumerate() {
            if i > 0 {
                let keyword = match op {
                    crate::SetOp::Union => "UNION",
                    crate::SetOp::UnionAll => "UNION ALL",
                };
                sql.push_str(&format!(" |> {} ", keyword));
            }
            sql.push_str(&tree.render_pipe_union_part(cfg, &mut binds));
        }

        if let Some(order_by) = render_order_by(&self.order_bys, cfg) {
            sql.push_str(&format!(" |> {}", order_by));
        }

        let (limit, offset) = render_limit_offset(self.limit, self.offset);
        let mut lo_parts = Vec::new();
        if let Some(l) = limit {
            lo_parts.push(l);
        }
        if let Some(o) = offset {
            lo_parts.push(o);
        }
        if !lo_parts.is_empty() {
            sql.push_str(&format!(" |> {}", lo_parts.join(" ")));
        }

        (sql, binds)
    }
}
