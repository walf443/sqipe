use super::{
    RenderConfig, Renderer, append_limit_offset_pipe, append_lock_clause, append_order_by,
    render_aggregate_expr, render_from, render_joins, render_select_columns, render_wheres,
    set_op_keyword,
};
use crate::tree::{SelectClause, SelectTree, StageRef, UnionTree};

pub struct PipeSqlRenderer;

/// Render WHERE clauses for the given indices and append to parts as a single WHERE stage.
fn flush_pending_wheres<V: Clone>(
    wheres: &[crate::WhereEntry<V>],
    indices: &mut Vec<usize>,
    parts: &mut Vec<String>,
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) {
    if indices.is_empty() {
        return;
    }
    let entries: Vec<_> = indices.iter().map(|&i| wheres[i].clone()).collect();
    if let Some(where_sql) = render_wheres(&entries, cfg, binds) {
        parts.push(format!("WHERE {}", where_sql));
    }
    indices.clear();
}

impl PipeSqlRenderer {
    fn render_core<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        let mut parts = Vec::new();

        parts.push(render_from(&tree.from, cfg, binds));

        if !tree.stage_order.is_empty() {
            // Render in stage_order sequence, grouping consecutive WHEREs
            let mut pending_where_indices: Vec<usize> = Vec::new();

            for stage in &tree.stage_order {
                match stage {
                    StageRef::Where(idx) => {
                        pending_where_indices.push(*idx);
                    }
                    StageRef::Join(idx) => {
                        flush_pending_wheres(
                            &tree.wheres,
                            &mut pending_where_indices,
                            &mut parts,
                            cfg,
                            binds,
                        );
                        let join = &tree.joins[*idx];
                        let sub_slice = tree.join_subqueries.get(*idx..*idx + 1).unwrap_or(&[]);
                        for js in render_joins(std::slice::from_ref(join), sub_slice, cfg, binds) {
                            parts.push(js);
                        }
                    }
                }
            }
            flush_pending_wheres(
                &tree.wheres,
                &mut pending_where_indices,
                &mut parts,
                cfg,
                binds,
            );
        } else {
            // Fallback: original behavior (JOINs then WHEREs)
            for join_sql in render_joins(&tree.joins, &tree.join_subqueries, cfg, binds) {
                parts.push(join_sql);
            }

            if let Some(where_sql) = render_wheres(&tree.wheres, cfg, binds) {
                parts.push(format!("WHERE {}", where_sql));
            }
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
        append_lock_clause(&mut sql, tree.lock_for.as_deref());
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
