use super::RenderConfig;
use crate::tree::{InsertToken, InsertTree};

/// Render an INSERT statement from an `InsertTree`.
pub fn render_insert<V: Clone>(tree: &InsertTree<V>, cfg: &RenderConfig) -> String {
    let mut bind_count: usize = 0;
    let mut parts = Vec::new();

    // Extract InsertInto metadata first (required by Values/SelectSource).
    let (table, columns, col_exprs) = extract_insert_into(&tree.tokens);

    for token in &tree.tokens {
        match token {
            InsertToken::InsertInto { .. } => {
                // Already extracted above; nothing to emit here.
            }
            InsertToken::Values(rows) => {
                let mut quoted_cols: Vec<String> = columns.iter().map(|c| (cfg.qi)(c)).collect();
                for (col, _) in col_exprs {
                    quoted_cols.push((cfg.qi)(col));
                }
                let header = format!(
                    "INSERT INTO {} ({}) VALUES ",
                    (cfg.qi)(table),
                    quoted_cols.join(", ")
                );
                let col_count = columns.len() + col_exprs.len();
                // Estimate: header + per-row "(?, ?, ...)" ~= (col_count * 3 + 2) per row
                let estimated_len =
                    header.len() + rows.len() * (col_count * 3 + 2) + rows.len() * 2;
                let mut sql = String::with_capacity(estimated_len);
                sql.push_str(&header);

                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push('(');
                    for (j, _val) in row.iter().enumerate() {
                        if j > 0 {
                            sql.push_str(", ");
                        }
                        bind_count += 1;
                        sql.push_str(&(cfg.ph)(bind_count));
                    }
                    for (k, (_, expr)) in col_exprs.iter().enumerate() {
                        if !row.is_empty() || k > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(expr);
                    }
                    sql.push(')');
                }

                parts.push(sql);
            }
            InsertToken::SelectSource(sub) => {
                let sub_sql = super::render_subquery_sql(sub, cfg, &mut bind_count);
                parts.push(format!("INSERT INTO {} {}", (cfg.qi)(table), sub_sql));
            }
            InsertToken::Raw(s) => {
                parts.push(s.clone());
            }
            InsertToken::KeywordAssignments { keyword, sets } => {
                let mut items = Vec::new();
                for clause in sets {
                    match clause {
                        crate::SetClause::Value(col, _val) => {
                            bind_count += 1;
                            items.push(format!("{} = {}", (cfg.qi)(col), (cfg.ph)(bind_count)));
                        }
                        crate::SetClause::Expr(expr) => {
                            items.push(expr.render(cfg, &mut bind_count));
                        }
                    }
                }
                parts.push(format!("{} {}", keyword, items.join(", ")));
            }
            #[cfg(feature = "returning")]
            InsertToken::Returning(cols) => {
                if let Some(returning_sql) = super::render_returning(cols, cfg) {
                    parts.push(returning_sql);
                }
            }
        }
    }

    parts.join(" ")
}

/// Extract table, columns, and col_exprs from the first `InsertInto` token.
///
/// # Panics
///
/// Panics if no `InsertInto` token is found.
fn extract_insert_into<V: Clone>(
    tokens: &[InsertToken<V>],
) -> (&str, &[String], &[(String, String)]) {
    for token in tokens {
        if let InsertToken::InsertInto {
            table,
            columns,
            col_exprs,
        } = token
        {
            return (table, columns, col_exprs);
        }
    }
    unreachable!("InsertTree must contain an InsertInto token")
}
