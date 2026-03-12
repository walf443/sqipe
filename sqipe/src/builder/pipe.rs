use crate::builder::{
    build_aggregate_pipe, build_limit_offset, build_order_by_clause, build_select_clause,
    build_where, SqlBuilder, SqlConfig,
};
use crate::{Query, Value};

pub(crate) struct PipeSqlBuilder;

impl SqlBuilder for PipeSqlBuilder {
    fn build_core(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String {
        let mut parts = Vec::new();

        parts.push(format!("FROM {}", (cfg.qi)(&query.table)));

        if let Some(where_sql) = build_where(&query.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        if !query.aggregates.is_empty() {
            parts.push(build_aggregate_pipe(
                &query.aggregates,
                &query.group_bys,
                cfg,
            ));
        } else {
            parts.push(build_select_clause(&query.selects, cfg));
        }

        parts.join(" |> ")
    }

    fn build_union_part(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String {
        let mut sql = Self::build_core(query, cfg, binds);
        let has_extra = !query.order_bys.is_empty()
            || query.limit_val.is_some()
            || query.offset_val.is_some();

        if has_extra {
            if let Some(order_by) = build_order_by_clause(&query.order_bys, cfg) {
                sql.push_str(&format!(" |> {}", order_by));
            }
            let (limit, offset) = build_limit_offset(query.limit_val, query.offset_val);
            let mut lo_parts = Vec::new();
            if let Some(l) = limit {
                lo_parts.push(l);
            }
            if let Some(o) = offset {
                lo_parts.push(o);
            }
            if !lo_parts.is_empty() {
                sql.push_str(&format!(" |> {}", lo_parts.join(" ")));
            }
            sql = format!("({})", sql);
        }

        sql
    }

}
