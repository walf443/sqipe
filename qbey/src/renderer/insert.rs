use super::RenderConfig;
use crate::tree::{InsertTree, InsertTreeSource};

/// Render an INSERT statement from an `InsertTree`.
pub fn render_insert<V: Clone>(tree: &InsertTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
    let mut binds: Vec<V> = Vec::new();

    match &tree.source {
        InsertTreeSource::Values(rows) => {
            // INSERT INTO "table" ("col1", "col2") VALUES (?, ?), (?, ?)
            let quoted_cols: Vec<String> = tree.columns.iter().map(|c| (cfg.qi)(c)).collect();
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES ",
                (cfg.qi)(&tree.table),
                quoted_cols.join(", ")
            );

            for (i, row) in rows.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (j, val) in row.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    binds.push(val.clone());
                    sql.push_str(&(cfg.ph)(binds.len()));
                }
                sql.push(')');
            }

            (sql, binds)
        }
        InsertTreeSource::Select(sub) => {
            // INSERT INTO "table" SELECT ...
            let sub_sql = super::render_select_core(sub, cfg, &mut binds);
            let mut sql = format!("INSERT INTO {} {}", (cfg.qi)(&tree.table), sub_sql);
            super::append_order_by(&mut sql, &sub.order_bys, cfg, " ");
            super::append_limit_offset_flat(&mut sql, sub.limit, sub.offset);
            (sql, binds)
        }
    }
}
