use super::{RenderConfig, Renderer, render_select_tokens};
use crate::tree::SelectTree;

pub struct StandardSqlRenderer;

impl Renderer for StandardSqlRenderer {
    fn render_select<V: Clone>(
        &self,
        tree: &SelectTree<V>,
        cfg: &RenderConfig,
    ) -> (String, Vec<V>) {
        let mut binds = Vec::new();
        let mut parts = Vec::new();
        render_select_tokens(&tree.tokens, cfg, &mut binds, &mut parts);
        (parts.join(" "), binds)
    }
}
