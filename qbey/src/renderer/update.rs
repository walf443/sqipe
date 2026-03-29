use super::{RenderConfig, render_cte_clause, render_wheres};
use crate::SetClause;
use crate::tree::{UpdateToken, UpdateTree};

/// Render an UPDATE statement from an `UpdateTree`.
///
/// # Panics
///
/// Panics if no `Set` token is found, as an UPDATE with no SET clause is invalid SQL.
pub fn render_update<V: Clone>(tree: &UpdateTree<V>, cfg: &RenderConfig) -> String {
    let mut bind_count: usize = 0;
    let mut parts = Vec::new();

    for token in &tree.tokens {
        match token {
            UpdateToken::With(ctes) => {
                if let Some(with_sql) = render_cte_clause(ctes, cfg, &mut bind_count) {
                    parts.push(with_sql);
                }
            }
            UpdateToken::Update { table, alias } => {
                let s = match alias {
                    Some(a) => format!("UPDATE {} {}", (cfg.qi)(table), (cfg.qi)(a)),
                    None => format!("UPDATE {}", (cfg.qi)(table)),
                };
                parts.push(s);
            }
            UpdateToken::Set(sets) => {
                assert!(!sets.is_empty(), "UPDATE requires at least one SET clause");
                let set_items: Vec<String> = sets
                    .iter()
                    .map(|clause| match clause {
                        SetClause::Value(col, _val) => {
                            bind_count += 1;
                            let placeholder = (cfg.ph)(bind_count);
                            format!("{} = {}", (cfg.qi)(col), placeholder)
                        }
                        SetClause::Expr(expr) => expr.render(cfg, &mut bind_count),
                    })
                    .collect();
                parts.push(format!("SET {}", set_items.join(", ")));
            }
            UpdateToken::Where(wheres) => {
                if let Some(where_sql) = render_wheres(wheres, cfg, &mut bind_count) {
                    parts.push(format!("WHERE {}", where_sql));
                }
            }
            UpdateToken::Raw(s) => {
                parts.push(s.clone());
            }
            #[cfg(feature = "returning")]
            UpdateToken::Returning(cols) => {
                if let Some(returning_sql) = super::render_returning(cols, cfg) {
                    parts.push(returning_sql);
                }
            }
        }
    }

    parts.join(" ")
}
