use super::{
    RenderConfig, Renderer, append_limit_offset_pipe, append_order_by, render_aggregate_expr,
    render_from, render_joins, render_select_columns, render_wheres, set_op_keyword,
};
use crate::tree::{SelectClause, SelectTree, UnionTree};

pub struct PipeSqlRenderer;

impl PipeSqlRenderer {
    fn render_core<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        let mut parts = Vec::new();

        parts.push(render_from(&tree.from, cfg, binds));

        for join_sql in render_joins(&tree.joins, cfg) {
            parts.push(join_sql);
        }

        if let Some(where_sql) = render_wheres(&tree.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        match &tree.select {
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

        if let Some(having_sql) = render_wheres(&tree.havings, cfg, binds) {
            parts.push(format!("WHERE {}", having_sql));
        }

        parts.join(" |> ")
    }

    fn render_union_part<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        let mut sql = self.render_core(tree, cfg, binds);
        let has_extra = !tree.order_bys.is_empty() || tree.limit.is_some() || tree.offset.is_some();

        if has_extra {
            append_order_by(&mut sql, &tree.order_bys, cfg, " |> ");
            append_limit_offset_pipe(&mut sql, tree.limit, tree.offset);
            sql = format!("({})", sql);
        }

        sql
    }
}

impl Renderer for PipeSqlRenderer {
    fn render_select<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
    ) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let mut sql = self.render_core(tree, cfg, &mut binds);
        append_order_by(&mut sql, &tree.order_bys, cfg, " |> ");
        append_limit_offset_pipe(&mut sql, tree.limit, tree.offset);
        (sql, binds)
    }

    fn render_union<V: Clone>(&self, tree: &UnionTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, part)) in tree.parts.iter().enumerate() {
            if i > 0 {
                sql.push_str(&format!(" |> {} ", set_op_keyword(op)));
            }
            sql.push_str(&self.render_union_part(part, cfg, &mut binds));
        }

        append_order_by(&mut sql, &tree.order_bys, cfg, " |> ");
        append_limit_offset_pipe(&mut sql, tree.limit, tree.offset);
        (sql, binds)
    }
}
