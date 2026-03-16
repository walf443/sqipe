use super::RenderConfig;
use crate::tree::{InsertToken, InsertTree};

/// Render an INSERT statement from an `InsertTree`.
pub fn render_insert<V: Clone>(tree: &InsertTree<V>, cfg: &RenderConfig) -> (String, Vec<V>) {
    let mut binds: Vec<V> = Vec::new();
    let mut parts = Vec::new();

    // Track table/columns from InsertInto token for SelectSource rendering
    let mut table_name = String::new();

    for token in &tree.tokens {
        match token {
            InsertToken::InsertInto {
                table,
                columns,
                col_exprs,
            } => {
                table_name = table.clone();
                // We don't emit the full INSERT INTO here yet;
                // the Values or SelectSource token decides the format.
                // Store info for use by subsequent tokens.
                // For Values, we emit the header when we see Values.
                // For SelectSource, we emit just INSERT INTO table.
                // We handle this below in Values/SelectSource.
                let _ = (columns, col_exprs); // used by Values token
            }
            InsertToken::Values(rows) => {
                // Find the InsertInto token to get columns/col_exprs
                let (columns, col_exprs) = find_insert_into(&tree.tokens);
                let mut quoted_cols: Vec<String> = columns.iter().map(|c| (cfg.qi)(c)).collect();
                for (col, _) in col_exprs {
                    quoted_cols.push((cfg.qi)(col));
                }
                let mut sql = format!(
                    "INSERT INTO {} ({}) VALUES ",
                    (cfg.qi)(&table_name),
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
                let sub_sql = super::render_subquery_sql(sub, cfg, &mut binds);
                parts.push(format!("INSERT INTO {} {}", (cfg.qi)(&table_name), sub_sql));
            }
            InsertToken::Raw(s) => {
                parts.push(s.clone());
            }
            InsertToken::KeywordAssignments { keyword, sets } => {
                let mut items = Vec::new();
                for clause in sets {
                    match clause {
                        crate::SetClause::Value(col, val) => {
                            binds.push(val.clone());
                            items.push(format!("{} = {}", (cfg.qi)(col), (cfg.ph)(binds.len())));
                        }
                        crate::SetClause::Expr(expr) => {
                            items.push(expr.as_str().to_string());
                        }
                    }
                }
                parts.push(format!("{} {}", keyword, items.join(", ")));
            }
        }
    }

    (parts.join(" "), binds)
}

/// Extract columns and col_exprs from the InsertInto token.
fn find_insert_into<V: Clone>(
    tokens: &[InsertToken<V>],
) -> (&Vec<String>, &Vec<(String, String)>) {
    for token in tokens {
        if let InsertToken::InsertInto {
            columns, col_exprs, ..
        } = token
        {
            return (columns, col_exprs);
        }
    }
    unreachable!("InsertTree must contain InsertInto token")
}
