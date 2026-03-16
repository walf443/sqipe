use super::{RenderConfig, render_wheres};
use crate::SetClause;
use crate::tree::{UpdateToken, UpdateTree};

/// Render an UPDATE statement from an `UpdateTree`.
///
/// # Panics
///
/// Panics if no `Set` token is found, as an UPDATE with no SET clause is invalid SQL.
pub fn render_update<V: Clone>(tree: &UpdateTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
    let mut binds: Vec<V> = Vec::new();
    let mut parts = Vec::new();

    for token in &tree.tokens {
        match token {
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
                        SetClause::Value(col, val) => {
                            binds.push(val.clone());
                            let placeholder = (cfg.ph)(binds.len());
                            format!("{} = {}", (cfg.qi)(col), placeholder)
                        }
                        SetClause::Expr(expr) => expr.to_string(),
                    })
                    .collect();
                parts.push(format!("SET {}", set_items.join(", ")));
            }
            UpdateToken::Where(wheres) => {
                if let Some(where_sql) = render_wheres(wheres, cfg, &mut binds) {
                    parts.push(format!("WHERE {}", where_sql));
                }
            }
            UpdateToken::Raw(s) => {
                parts.push(s.clone());
            }
        }
    }

    (parts.join(" "), binds)
}
