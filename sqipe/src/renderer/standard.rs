use super::{
    RenderConfig, Renderer, append_limit_offset_flat, append_order_by, render_select_core,
    set_op_keyword,
};
use crate::tree::{SelectTree, UnionTree};

pub struct StandardSqlRenderer;

impl StandardSqlRenderer {
    fn render_union_part<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
        binds: &mut Vec<V>,
    ) -> String {
        let mut sql = render_select_core(tree, cfg, binds);
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
    fn render_select<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
    ) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let mut sql = render_select_core(tree, cfg, &mut binds);
        append_order_by(&mut sql, &tree.order_bys, cfg, " ");
        append_limit_offset_flat(&mut sql, tree.limit, tree.offset);
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
