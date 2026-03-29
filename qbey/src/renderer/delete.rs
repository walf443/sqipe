use super::{RenderConfig, render_cte_clause, render_wheres};
use crate::tree::{DeleteToken, DeleteTree};

/// Render a DELETE statement from a `DeleteTree`.
pub fn render_delete<'a, V: Clone>(
    tree: &'a DeleteTree<V>,
    cfg: &RenderConfig,
) -> (String, Vec<&'a V>) {
    let mut binds: Vec<&V> = Vec::new();
    let mut parts = Vec::new();

    for token in &tree.tokens {
        match token {
            DeleteToken::With(ctes) => {
                if let Some(with_sql) = render_cte_clause(ctes, cfg, &mut binds) {
                    parts.push(with_sql);
                }
            }
            DeleteToken::DeleteFrom { table, alias } => {
                let s = match alias {
                    Some(a) => format!("DELETE FROM {} {}", (cfg.qi)(table), (cfg.qi)(a)),
                    None => format!("DELETE FROM {}", (cfg.qi)(table)),
                };
                parts.push(s);
            }
            DeleteToken::Where(wheres) => {
                if let Some(where_sql) = render_wheres(wheres, cfg, &mut binds) {
                    parts.push(format!("WHERE {}", where_sql));
                }
            }
            DeleteToken::Raw(s) => {
                parts.push(s.clone());
            }
            #[cfg(feature = "returning")]
            DeleteToken::Returning(cols) => {
                if let Some(returning_sql) = super::render_returning(cols, cfg) {
                    parts.push(returning_sql);
                }
            }
        }
    }

    (parts.join(" "), binds)
}
