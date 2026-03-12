use crate::builder::{
    build_aggregate_select, build_group_by_clause, build_limit_offset, build_order_by_clause,
    build_select_clause, build_where, SqlBuilder, SqlConfig,
};
use crate::{Query, Value};

pub(crate) struct StandardSqlBuilder;

impl SqlBuilder for StandardSqlBuilder {
    fn build_core(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String {
        let mut parts = Vec::new();

        if !query.aggregates.is_empty() {
            parts.push(build_aggregate_select(
                &query.group_bys,
                &query.aggregates,
                cfg,
            ));
        } else {
            parts.push(build_select_clause(&query.selects, cfg));
        }

        parts.push(format!("FROM {}", (cfg.qi)(&query.table)));

        if let Some(where_sql) = build_where(&query.wheres, cfg, binds) {
            parts.push(format!("WHERE {}", where_sql));
        }

        if let Some(group_by) = build_group_by_clause(&query.group_bys, cfg) {
            parts.push(group_by);
        }

        parts.join(" ")
    }

    fn build_union_part(query: &Query, cfg: &SqlConfig, binds: &mut Vec<Value>) -> String {
        let mut sql = Self::build_core(query, cfg, binds);
        let has_extra = !query.order_bys.is_empty()
            || query.limit_val.is_some()
            || query.offset_val.is_some();

        if has_extra {
            if let Some(order_by) = build_order_by_clause(&query.order_bys, cfg) {
                sql.push_str(&format!(" {}", order_by));
            }
            let (limit, offset) = build_limit_offset(query.limit_val, query.offset_val);
            if let Some(l) = limit {
                sql.push_str(&format!(" {}", l));
            }
            if let Some(o) = offset {
                sql.push_str(&format!(" {}", o));
            }
            sql = format!("({})", sql);
        }

        sql
    }

}
