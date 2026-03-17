use criterion::{Criterion, criterion_group, criterion_main};
use qbey::{
    ConditionExpr, InsertQueryBuilder, SelectQueryBuilder, col, count_all, qbey,
    renderer::{RenderConfig, Renderer, standard::StandardSqlRenderer},
    table,
};

// ── Query builders (return SelectQuery / InsertQuery) ──

fn make_complex_select_query() -> qbey::SelectQuery<qbey::Value> {
    let mut q = qbey("users");
    q.as_("u");
    q.select(&["id", "name", "email", "status", "created_at"]);
    q.add_select(count_all().as_("order_count"));

    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.left_join(
        table("profiles").as_("p"),
        table("u").col("id").eq_col("user_id"),
    );
    q.join(
        table("order_items").as_("oi"),
        table("o").col("id").eq_col("order_id"),
    );
    q.left_join(
        table("products").as_("pr"),
        table("oi").col("product_id").eq_col("id"),
    );
    q.left_join(
        table("categories").as_("c"),
        table("pr").col("category_id").eq_col("id"),
    );

    let mut sub = qbey("payments");
    sub.select(&["order_id"]);
    sub.add_select_expr(qbey::RawSql::new("SUM(amount)"), Some("total_paid"));
    sub.group_by(&["order_id"]);
    q.left_join_subquery(sub, "pay", table("o").col("id").eq_col("order_id"));

    q.and_where(col("status").eq("active"));
    q.and_where(col("created_at").gt("2024-01-01"));
    q.and_where(col("email").not_like(qbey::LikeExpression::contains("test")));
    q.and_where(col("role").included(&[
        qbey::Value::String("admin".into()),
        qbey::Value::String("editor".into()),
        qbey::Value::String("viewer".into()),
    ]));

    q.group_by(&["id", "name", "email", "status", "created_at"]);
    q.order_by(col("created_at").desc());
    q.order_by(col("name").asc());
    q.limit(50);
    q.offset(100);
    q
}

fn make_union_query() -> qbey::SelectQuery<qbey::Value> {
    let tables = [
        "employees",
        "contractors",
        "interns",
        "consultants",
        "temps",
    ];
    let mut parts: Vec<qbey::SelectQuery<qbey::Value>> = tables
        .iter()
        .map(|t| {
            let mut q = qbey(*t);
            q.select(&["id", "name", "dept", "start_date"]);
            q.and_where(col("active").eq(true));
            q.and_where(col("dept").eq("engineering"));
            q
        })
        .collect();
    let first = parts.remove(0);
    let mut compound = first;
    for part in &parts {
        compound.add_union_all(part);
    }
    compound.order_by(col("name").asc());
    compound.limit(100);
    compound
}

fn make_bulk_insert_query() -> qbey::InsertQuery<qbey::Value> {
    let mut ins = qbey("events").into_insert();
    for i in 0..100 {
        ins.add_value(&[
            ("user_id", qbey::Value::Int(i)),
            (
                "event_type",
                qbey::Value::String(format!("type_{}", i % 10)),
            ),
            ("payload", qbey::Value::String(format!("{{\"seq\":{}}}", i))),
            (
                "created_at",
                qbey::Value::String("2024-06-15T12:00:00Z".into()),
            ),
        ]);
    }
    ins
}

fn make_nested_subquery() -> qbey::SelectQuery<qbey::Value> {
    let mut inner = qbey("order_items");
    inner.select(&["product_id"]);
    inner.and_where(col("qty").gt(5));

    let mut middle = qbey("orders");
    middle.select(&["user_id"]);
    middle.and_where(col("product_id").included(inner));

    let mut outer = qbey("users");
    outer.and_where(col("id").included(middle));
    outer.order_by(col("name").asc());
    outer.limit(20);
    outer
}

// ── Benchmarks ──

/// Full to_sql (build query + tree + render).
fn bench_to_sql(c: &mut Criterion) {
    c.bench_function("complex_select_to_sql", |b| {
        b.iter(|| make_complex_select_query().to_sql())
    });
    c.bench_function("union_5parts_to_sql", |b| {
        b.iter(|| make_union_query().to_sql())
    });
    c.bench_function("bulk_insert_100rows_to_sql", |b| {
        b.iter(|| make_bulk_insert_query().to_sql())
    });
    c.bench_function("nested_subquery_3level_to_sql", |b| {
        b.iter(|| make_nested_subquery().to_sql())
    });
}

/// Tree construction only (query already built, measure to_tree).
fn bench_build_tree(c: &mut Criterion) {
    let complex_query = make_complex_select_query();
    c.bench_function("complex_select_build_tree", |b| {
        b.iter(|| complex_query.to_tree())
    });

    let union_query = make_union_query();
    c.bench_function("union_5parts_build_tree", |b| {
        b.iter(|| union_query.to_tree())
    });

    let insert_query = make_bulk_insert_query();
    c.bench_function("bulk_insert_100rows_build_tree", |b| {
        b.iter(|| insert_query.to_tree())
    });

    let nested_query = make_nested_subquery();
    c.bench_function("nested_subquery_3level_build_tree", |b| {
        b.iter(|| nested_query.to_tree())
    });
}

/// Rendering only (tree already built).
fn bench_render(c: &mut Criterion) {
    let ph = |_: usize| "?".to_string();
    let qi = |name: &str| format!("\"{}\"", name);
    let dialect = qbey::DefaultDialect;
    let cfg = RenderConfig::from_dialect(&ph, &qi, &dialect);
    let renderer = StandardSqlRenderer;

    let complex_tree = make_complex_select_query().to_tree();
    c.bench_function("complex_select_render", |b| {
        b.iter(|| renderer.render_select(&complex_tree, &cfg))
    });

    let union_tree = make_union_query().to_tree();
    c.bench_function("union_5parts_render", |b| {
        b.iter(|| renderer.render_select(&union_tree, &cfg))
    });

    let insert_tree = make_bulk_insert_query().to_tree();
    c.bench_function("bulk_insert_100rows_render", |b| {
        b.iter(|| qbey::renderer::insert::render_insert(&insert_tree, &cfg))
    });

    let nested_tree = make_nested_subquery().to_tree();
    c.bench_function("nested_subquery_3level_render", |b| {
        b.iter(|| renderer.render_select(&nested_tree, &cfg))
    });
}

criterion_group!(benches, bench_to_sql, bench_build_tree, bench_render);
criterion_main!(benches);
