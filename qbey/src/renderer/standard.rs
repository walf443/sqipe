use super::{RenderConfig, Renderer, render_select_tokens};
use crate::tree::SelectTree;

pub struct StandardSqlRenderer;

impl Renderer for StandardSqlRenderer {
    fn render_select<V: Clone>(&self, tree: &SelectTree<V>, cfg: &RenderConfig) -> String {
        let mut bind_count: usize = 0;
        let mut parts = Vec::new();
        render_select_tokens(&tree.tokens, cfg, &mut bind_count, &mut parts);
        parts.join(" ")
    }
}
