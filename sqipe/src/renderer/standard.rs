use crate::Value;

use super::{
    RenderConfig, Renderer, append_limit_offset_flat, append_order_by, render_aggregate_expr,
    render_from, render_select_columns, render_wheres, set_op_keyword,
};
use crate::tree::{SelectClause, SelectTree, UnionTree};

pub struct StandardSqlRenderer;

impl StandardSqlRenderer {
    fn render_core(&self, tree: &SelectTree, cfg: &RenderConfig, binds: &mut Vec<Value>) -> String {
        let mut parts = Vec::new();

        match &tree.select {
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

        parts.push(render_from(&tree.from, cfg));

        if let Some(where_sql) = render_wheres(&tree.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        if let SelectClause::Aggregate { group_bys, .. } = &tree.select
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

    fn render_union_part(
        &self,
        tree: &SelectTree,
        cfg: &RenderConfig,
        binds: &mut Vec<Value>,
    ) -> String {
        let mut sql = self.render_core(tree, cfg, binds);
        let has_extra = !tree.order_bys.is_empty() || tree.limit.is_some() || tree.offset.is_some();

        if has_extra {
            append_order_by(&mut sql, &tree.order_bys, cfg, " ");
            append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
            sql = format!("({})", sql);
        }

        sql
    }
}

impl Renderer for StandardSqlRenderer {
    fn render_select(&self, tree: &SelectTree, cfg: &RenderConfig) -> (String, Vec<Value>) {
        let mut binds = Vec::new();
        let mut sql = self.render_core(tree, cfg, &mut binds);
        append_order_by(&mut sql, &tree.order_bys, cfg, " ");
        append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
        (sql, binds)
    }

    fn render_union(&self, tree: &UnionTree, cfg: &RenderConfig) -> (String, Vec<Value>) {
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
