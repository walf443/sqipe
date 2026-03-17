use super::common::{MysqlValue, bind_params, setup_pool};
use qbey::{ConditionExpr, LikeExpression, SelectQueryBuilder, col, row_number, table, window};
use qbey_mysql::qbey_with;
use sqlx::Row;

#[tokio::test]
async fn test_basic_select() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_where_condition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[0].get::<i64, _>("age"), 30);
}

#[tokio::test]
async fn test_order_by_and_limit() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.order_by(col("age").desc());
    q.limit(2);
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
    assert_eq!(rows[1].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_join() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.and_where(table("orders").col("status").eq("shipped"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_join_with_alias() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.as_("u");
    q.join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.and_where(table("o").col("status").eq("shipped"));
    let mut cols = table("u").cols(&["id", "name"]);
    cols.extend(table("o").cols(&["total"]));
    q.select(&cols);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_left_join() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.as_("u");
    q.left_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select(&table("u").cols(&["id", "name"]));
    q.add_select(table("o").col("total").as_("order_total"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn test_straight_join() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.straight_join("orders", table("users").col("id").eq_col("user_id"));
    q.select(&table("users").cols(&["id", "name"]));
    q.add_select(table("orders").col("total"));
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_force_index() {
    let pool = setup_pool().await;

    // Create an index to reference
    sqlx::query("CREATE INDEX idx_name ON users (name)")
        .execute(&pool)
        .await
        .unwrap();

    let mut q = qbey_with::<MysqlValue>("users");
    q.force_index(&["idx_name"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_force_index_for_join() {
    let pool = setup_pool().await;

    // Create an index on the join column
    sqlx::query("CREATE INDEX idx_user_id ON orders (user_id)")
        .execute(&pool)
        .await
        .unwrap();

    let mut q = qbey_with::<MysqlValue>("users");
    q.force_index_for(qbey_mysql::IndexHintScope::Join, &["PRIMARY"]);
    q.join("orders", table("users").col("id").eq_col("user_id"));
    q.select(&table("users").cols(&["id", "name"]));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_use_index_for_order_by() {
    let pool = setup_pool().await;

    sqlx::query("CREATE INDEX idx_name_ob ON users (name)")
        .execute(&pool)
        .await
        .unwrap();

    let mut q = qbey_with::<MysqlValue>("users");
    q.use_index_for(qbey_mysql::IndexHintScope::OrderBy, &["idx_name_ob"]);
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert!(!rows.is_empty());
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_multiple_index_hints() {
    let pool = setup_pool().await;

    sqlx::query("CREATE INDEX idx_name_mh ON users (name)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX idx_age_mh ON users (age)")
        .execute(&pool)
        .await
        .unwrap();

    // MySQL allows combining USE INDEX FOR JOIN + USE INDEX FOR ORDER BY on the same table
    let mut q = qbey_with::<MysqlValue>("users");
    q.use_index_for(qbey_mysql::IndexHintScope::Join, &["idx_name_mh"]);
    q.use_index_for(qbey_mysql::IndexHintScope::OrderBy, &["idx_age_mh"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name", "age"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_between() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").between(25, 30));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_union() {
    let pool = setup_pool().await;

    let mut q1 = qbey_with::<MysqlValue>("users");
    q1.and_where(col("age").gt(30));
    q1.select(&["id", "name"]);

    let mut q2 = qbey_with::<MysqlValue>("users");
    q2.and_where(col("age").lt(26));
    q2.select(&["id", "name"]);

    let uq = q1.union(&q2);
    let (sql, binds) = uq.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_in_subquery() {
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_in_subquery_with_outer_binds() {
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("age").gt(26));
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice (age=30 > 26, has shipped order) — Bob (age=25) filtered out by age > 26
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_not_in_subquery() {
    let pool = setup_pool().await;

    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("id").not_included(sub));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Charlie (id=3) is not in shipped orders (user_id 1,2)
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_from_subquery() {
    let pool = setup_pool().await;

    // Use MysqlQuery as subquery source via IntoSelectTree
    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_mysql::qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.order_by(col("total").desc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1); // Alice, total=100
    assert_eq!(rows[1].get::<i64, _>("user_id"), 2); // Bob, total=50
}

#[tokio::test]
async fn test_from_subquery_with_outer_where() {
    let pool = setup_pool().await;

    // Use MysqlQuery as subquery source via IntoSelectTree
    let mut sub = qbey_with::<MysqlValue>("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey_mysql::qbey_from_subquery_with(sub, "t");
    q.select(&["user_id", "total"]);
    q.and_where(col("total").gt(60.0));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Only Alice's order (total=100) passes total > 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1);
}

#[tokio::test]
async fn test_like_contains() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_like_starts_with() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Al")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_like_ends_with() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ob")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_not_like() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").not_like(LikeExpression::contains("li")));
    q.select(&["id", "name"]);
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Bob");
}

#[tokio::test]
async fn test_for_update() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update();
    let (sql, binds) = q.to_sql();

    assert!(sql.ends_with("FOR UPDATE"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_for_update_with_nowait() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_update_with("NOWAIT");
    let (sql, binds) = q.to_sql();

    assert!(sql.ends_with("FOR UPDATE NOWAIT"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_for_update_skip_locked() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.for_update_with("SKIP LOCKED");
    let (sql, _) = q.to_sql();

    assert!(sql.ends_with("FOR UPDATE SKIP LOCKED"));

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn test_for_with_share() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name"]);
    q.and_where(col("id").eq(1));
    q.for_with("SHARE");
    let (sql, binds) = q.to_sql();

    assert!(sql.ends_with("FOR SHARE"));

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
}

#[tokio::test]
async fn test_like_custom_escape_char() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "li")));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice");
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie");
}

// --- DISTINCT ---

#[tokio::test]
async fn test_distinct() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.distinct();
    q.select(&["status"]);
    q.order_by(col("status").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // orders has: shipped, pending, shipped → distinct gives: pending, shipped
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("status"), "pending");
    assert_eq!(rows[1].get::<String, _>("status"), "shipped");
}

#[tokio::test]
async fn test_having() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["user_id"]);
    q.add_select(qbey::count_all().as_("cnt"));
    q.group_by(&["user_id"]);
    q.having(col("cnt").gt(1));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Only Alice (user_id=1) has 2 orders
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("user_id"), 1);
}

#[tokio::test]
async fn test_having_with_where() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["user_id"]);
    q.add_select(qbey::count_all().as_("cnt"));
    q.and_where(col("status").eq("shipped"));
    q.group_by(&["user_id"]);
    q.and_having(col("cnt").gt(0));
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();
    // Alice (1 shipped) and Bob (1 shipped)
    assert_eq!(rows.len(), 2);
}

// ── Window functions ──

#[tokio::test]
async fn test_row_number_over() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("users");
    q.select(&["id", "name", "age"]);
    q.add_select(
        row_number()
            .over(window().order_by(col("age").desc()))
            .as_("rn"),
    );
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // Ordered by age DESC: Charlie(35)=1, Alice(30)=2, Bob(25)=3
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<u64, _>("rn"), 1);
    assert_eq!(rows[0].get::<String, _>("name"), "Charlie");
}

#[tokio::test]
async fn test_sum_over_partition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["id", "user_id", "total"]);
    q.add_select(
        col("total")
            .sum_over(window().partition_by(&[col("user_id")]))
            .as_("user_total"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has orders 100+200=300, user_id=2 has 50
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<f64, _>("user_total"), 300.0);
    assert_eq!(rows[2].get::<f64, _>("user_total"), 50.0);
}

#[tokio::test]
async fn test_count_over_partition() {
    let pool = setup_pool().await;

    let mut q = qbey_with::<MysqlValue>("orders");
    q.select(&["id", "user_id"]);
    q.add_select(
        col("id")
            .count_over(window().partition_by(&[col("user_id")]))
            .as_("user_order_count"),
    );
    q.order_by(col("id").asc());
    let (sql, _) = q.to_sql();

    let rows = sqlx::query(&sql).fetch_all(&pool).await.unwrap();

    // user_id=1 has 2 orders, user_id=2 has 1
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i64, _>("user_order_count"), 2);
    assert_eq!(rows[2].get::<i64, _>("user_order_count"), 1);
}

#[tokio::test]
async fn test_cte() {
    let pool = setup_pool().await;

    let mut cte_q = qbey_with::<MysqlValue>("users");
    cte_q.select(&["id", "name", "age"]);
    cte_q.and_where(col("age").gt(28));

    let mut q = qbey_with::<MysqlValue>("older_users");
    q.with_cte("older_users", &[], cte_q);
    q.select(&["id", "name"]);
    q.order_by(col("age").asc());
    let (sql, binds) = q.to_sql();

    let rows = bind_params(sqlx::query(&sql), &binds)
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("name"), "Alice"); // age 30
    assert_eq!(rows[1].get::<String, _>("name"), "Charlie"); // age 35
}
