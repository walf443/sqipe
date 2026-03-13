use super::{
    RenderConfig, Renderer, append_limit_offset_flat, append_order_by, render_from, render_joins,
    render_select_columns, render_select_core, set_op_keyword,
};
use crate::tree::{FromSource, SelectClause, SelectTree, StageRef, UnionTree};

pub struct StandardSqlRenderer;

/// Render WHERE clauses for specific indices only.
fn render_wheres_for_indices<V: Clone>(
    wheres: &[crate::WhereEntry<V>],
    indices: &[usize],
    cfg: &RenderConfig,
    binds: &mut Vec<V>,
) -> Option<String> {
    let entries: Vec<crate::WhereEntry<V>> = indices.iter().map(|&i| wheres[i].clone()).collect();
    super::render_wheres(&entries, cfg, binds)
}

/// Check if stage_order contains a WHERE→JOIN transition (CTE needed).
fn has_where_before_join(stage_order: &[StageRef]) -> bool {
    let mut seen_where = false;
    for stage in stage_order {
        match stage {
            StageRef::Where(_) => seen_where = true,
            StageRef::Join(_) => {
                if seen_where {
                    return true;
                }
            }
        }
    }
    false
}

impl StandardSqlRenderer {
    fn render_union_part<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        let mut sql = render_select_core(tree, cfg, binds);
        let has_extra =
            !tree.order_bys.is_empty() || tree.limit.is_some() || tree.offset.is_some();

        if has_extra {
            append_order_by(&mut sql, &tree.order_bys, cfg, " ");
            append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
            sql = format!("({})", sql);
        }

        sql
    }

    /// Render a SELECT with CTE generation for WHERE-before-JOIN patterns.
    fn render_select_with_cte<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        if !has_where_before_join(&tree.stage_order) {
            let mut sql = render_select_core(tree, cfg, binds);
            append_order_by(&mut sql, &tree.order_bys, cfg, " ");
            append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
            return sql;
        }

        // Determine the base table name and the alias used in JOIN conditions
        let base_table = match &tree.from.source {
            FromSource::Table(t) => t.clone(),
            FromSource::Subquery(_) => String::new(),
        };
        // The name used in JOIN conditions (alias if set, otherwise table name)
        let effective_name = tree
            .from
            .alias
            .as_deref()
            .unwrap_or(&base_table)
            .to_string();

        let mut cte_parts: Vec<String> = Vec::new();
        let mut cte_counter: usize = 0;
        let mut pending_wheres: Vec<usize> = Vec::new();
        let mut pending_joins: Vec<usize> = Vec::new();
        // Track the previous CTE name so we can reference it
        let mut prev_cte_name: Option<String> = None;

        for stage in &tree.stage_order {
            match stage {
                StageRef::Where(idx) => {
                    pending_wheres.push(*idx);
                }
                StageRef::Join(idx) => {
                    if !pending_wheres.is_empty() {
                        // WHERE→JOIN transition: emit CTE for pending_wheres
                        let cte_name = format!("_cte_{}", cte_counter);
                        cte_counter += 1;

                        let mut cte_sql = String::from("SELECT *");

                        if prev_cte_name.is_none() && pending_joins.is_empty() {
                            // First CTE, no prior joins: FROM the original table
                            let from_str = render_from(&tree.from, cfg, binds);
                            cte_sql.push(' ');
                            cte_sql.push_str(&from_str);
                        } else {
                            // FROM the previous CTE, aliased to the effective name
                            let from_name = prev_cte_name.as_deref().unwrap_or(&base_table);
                            cte_sql.push_str(&format!(
                                " FROM {} AS {}",
                                (cfg.qi)(from_name),
                                (cfg.qi)(&effective_name)
                            ));
                            for &ji in &pending_joins {
                                if let Some(join) = tree.joins.get(ji) {
                                    for js in render_joins(std::slice::from_ref(join), cfg) {
                                        cte_sql.push(' ');
                                        cte_sql.push_str(&js);
                                    }
                                }
                            }
                        }

                        // WHERE clauses
                        if let Some(where_sql) = render_wheres_for_indices(
                            &tree.wheres,
                            &pending_wheres,
                            cfg,
                            binds,
                        ) {
                            cte_sql.push_str(&format!(" WHERE {}", where_sql));
                        }

                        cte_parts.push(format!("{} AS ({})", (cfg.qi)(&cte_name), cte_sql));
                        prev_cte_name = Some(cte_name);
                        pending_wheres.clear();
                        pending_joins.clear();
                    }
                    pending_joins.push(*idx);
                }
            }
        }

        // Build the main query
        let select_str = match &tree.select {
            SelectClause::Columns(cols) => render_select_columns(cols, cfg),
            SelectClause::Aggregate { group_bys, exprs } => {
                let mut items = Vec::new();
                for c in group_bys {
                    items.push((cfg.qi)(c));
                }
                for expr in exprs {
                    items.push(super::render_aggregate_expr(expr, cfg));
                }
                format!("SELECT {}", items.join(", "))
            }
        };

        let mut main_sql = select_str;

        // FROM: use previous CTE aliased to effective name
        if let Some(ref cte_name) = prev_cte_name {
            main_sql.push_str(&format!(
                " FROM {} AS {}",
                (cfg.qi)(cte_name),
                (cfg.qi)(&effective_name)
            ));
        }

        // Pending joins go to the main query
        for &ji in &pending_joins {
            if let Some(join) = tree.joins.get(ji) {
                for js in render_joins(std::slice::from_ref(join), cfg) {
                    main_sql.push(' ');
                    main_sql.push_str(&js);
                }
            }
        }

        // Remaining pending wheres go to the main WHERE clause
        if !pending_wheres.is_empty() {
            if let Some(where_sql) =
                render_wheres_for_indices(&tree.wheres, &pending_wheres, cfg, binds)
            {
                main_sql.push_str(&format!(" WHERE {}", where_sql));
            }
        }

        // GROUP BY / HAVING
        if let SelectClause::Aggregate { group_bys, .. } = &tree.select {
            if !group_bys.is_empty() {
                let cols: Vec<String> = group_bys.iter().map(|c| (cfg.qi)(c)).collect();
                main_sql.push_str(&format!(" GROUP BY {}", cols.join(", ")));
            }
        }

        if let Some(having_sql) = super::render_wheres(&tree.havings, cfg, binds) {
            main_sql.push_str(&format!(" HAVING {}", having_sql));
        }

        let mut sql = format!("WITH {} {}", cte_parts.join(", "), main_sql);
        append_order_by(&mut sql, &tree.order_bys, cfg, " ");
        append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
        sql
    }
}

impl Renderer for StandardSqlRenderer {
    fn render_select<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
    ) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let sql = self.render_select_with_cte(tree, cfg, &mut binds);
        (sql, binds)
    }

    fn render_union<V: Clone>(&self, tree: &UnionTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let mut sql = String::new();

        for (i, (op, part)) in tree.parts.iter().enumerate() {
            if i > 0 {
                sql.push_str(&format!(" {} ", set_op_keyword(op)));
            }
            sql.push_str(&self.render_union_part(part, cfg, &mut binds));
        }

        append_order_by(&mut sql, &tree.order_bys, cfg, " ");
        append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
        (sql, binds)
    }
}
