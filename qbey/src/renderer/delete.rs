use super::{RenderConfig, append_order_by, render_wheres};
use crate::tree::DeleteTree;

/// Render a DELETE statement from a `DeleteTree`.
pub fn render_delete<V: Clone>(tree: &DeleteTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
    let mut binds: Vec<V> = Vec::new();
    let mut parts = Vec::new();

    // DELETE FROM "table" or DELETE FROM "table" "alias"
    let table = match &tree.table_alias {
        Some(alias) => format!("DELETE FROM {} {}", (cfg.qi)(&tree.table), (cfg.qi)(alias)),
        None => format!("DELETE FROM {}", (cfg.qi)(&tree.table)),
    };
    parts.push(table);

    // WHERE ...
    if let Some(where_sql) = render_wheres(&tree.wheres, cfg, &mut binds) {
        parts.push(format!("WHERE {}", where_sql));
    }

    let mut sql = parts.join(" ");

    // ORDER BY ...
    append_order_by(&mut sql, &tree.order_bys, cfg, " ");

    // LIMIT ...
    if let Some(limit) = tree.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }

    (sql, binds)
}
