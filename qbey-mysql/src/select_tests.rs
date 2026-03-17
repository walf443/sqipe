use crate::IndexHintScope;
use crate::qbey;
use qbey::{ConditionExpr, SelectQueryBuilder, col, table};

#[test]
fn test_basic_to_sql() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT `id`, `name` FROM `employee` WHERE `name` = ?");
}

#[test]
fn test_force_index() {
    let mut q = qbey("employee");
    q.force_index(&["idx_name"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name) WHERE `name` = ?"
    );
}

#[test]
fn test_force_index_multiple() {
    let mut q = qbey("employee");
    q.force_index(&["idx_name", "idx_age"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_name, idx_age) WHERE `name` = ?"
    );
}

#[test]
fn test_use_index() {
    let mut q = qbey("employee");
    q.use_index(&["idx_name"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` USE INDEX (idx_name) WHERE `name` = ?"
    );
}

#[test]
fn test_ignore_index() {
    let mut q = qbey("employee");
    q.ignore_index(&["idx_old"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` IGNORE INDEX (idx_old) WHERE `name` = ?"
    );
}

#[test]
fn test_force_index_for_join() {
    let mut q = qbey("employee");
    q.force_index_for(IndexHintScope::Join, &["idx_name"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` FORCE INDEX FOR JOIN (idx_name) WHERE `name` = ?"
    );
}

#[test]
fn test_use_index_for_order_by() {
    let mut q = qbey("employee");
    q.use_index_for(IndexHintScope::OrderBy, &["idx_name"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` USE INDEX FOR ORDER BY (idx_name) WHERE `name` = ?"
    );
}

#[test]
fn test_ignore_index_for_group_by() {
    let mut q = qbey("employee");
    q.ignore_index_for(IndexHintScope::GroupBy, &["idx_dept"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` IGNORE INDEX FOR GROUP BY (idx_dept) WHERE `name` = ?"
    );
}

#[test]
fn test_multiple_index_hints_combined() {
    let mut q = qbey("employee");
    q.use_index_for(IndexHintScope::Join, &["idx_a"]);
    q.use_index_for(IndexHintScope::OrderBy, &["idx_b"]);
    q.and_where(("name", "Alice"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` USE INDEX FOR JOIN (idx_a) USE INDEX FOR ORDER BY (idx_b) WHERE `name` = ?"
    );
}

#[test]
fn test_delegates_core_methods() {
    let mut q = qbey("employee");
    q.and_where(("name", "Alice"));
    q.and_where(col("age").gt(20));
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    q.limit(10);
    q.offset(5);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` WHERE `name` = ? AND `age` > ? ORDER BY `name` ASC LIMIT 10 OFFSET 5"
    );
}

#[test]
fn test_union_all_with_force_index() {
    let mut q1 = qbey("employee");
    q1.force_index(&["idx_dept"]);
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.force_index(&["idx_dept"]);
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let uq = q1.union_all(&q2);

    let (sql, binds) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_dept) WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` FORCE INDEX (idx_dept) WHERE `dept` = ?"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("eng".to_string()),
            qbey::Value::String("sales".to_string()),
        ]
    );
}

#[test]
fn test_union_with_order_by_and_limit() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let mut uq = q1.union_all(&q2);
    uq.order_by(col("name").asc());
    uq.limit(10);

    let (sql, _) = uq.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? ORDER BY `name` ASC LIMIT 10"
    );
}

#[test]
fn test_union_with_add_union() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let mut q3 = qbey("contractor");
    q3.and_where(("dept", "eng"));
    q3.select(&["id", "name"]);

    let mut q4 = qbey("contractor");
    q4.and_where(("dept", "sales"));
    q4.select(&["id", "name"]);

    let uq2 = q3.union_all(&q4);
    let mut uq1 = q1.union_all(&q2);
    uq1.add_union_all(&uq2);

    let (sql, binds) = uq1.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ?"
    );
    assert_eq!(binds.len(), 4);
}

#[test]
fn test_query_union_with_compound_query() {
    let mut q1 = qbey("employee");
    q1.and_where(("dept", "eng"));
    q1.select(&["id", "name"]);

    let mut q2 = qbey("employee");
    q2.and_where(("dept", "sales"));
    q2.select(&["id", "name"]);

    let mut q3 = qbey("contractor");
    q3.and_where(("dept", "eng"));
    q3.select(&["id", "name"]);

    let uq = q2.union_all(&q3);
    let result = q1.union_all(&uq);

    let (sql, binds) = result.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `employee` WHERE `dept` = ? UNION ALL SELECT `id`, `name` FROM `contractor` WHERE `dept` = ?"
    );
    assert_eq!(binds.len(), 3);
}

#[test]
fn test_straight_join() {
    let mut q = qbey("users");
    q.straight_join("orders", table("users").col("id").eq_col("user_id"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN `orders` ON `users`.`id` = `orders`.`user_id`"
    );
}

#[test]
fn test_in_subquery() {
    let mut sub = qbey("orders");
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` WHERE `status` = ?)"
    );
    assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
}

#[test]
fn test_in_subquery_with_force_index() {
    let mut sub = qbey("orders");
    sub.force_index(&["idx_status"]);
    sub.select(&["user_id"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.and_where(col("id").included(sub));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` WHERE `id` IN (SELECT `user_id` FROM `orders` FORCE INDEX (idx_status) WHERE `status` = ?)"
    );
    assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
}

#[test]
fn test_straight_join_with_alias() {
    let mut q = qbey("users");
    q.as_("u");
    q.straight_join(
        table("orders").as_("o"),
        table("u").col("id").eq_col("user_id"),
    );
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` AS `u` STRAIGHT_JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`"
    );
}

#[test]
fn test_like_escape_backslash() {
    use qbey::LikeExpression;

    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::contains("test")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT `id`, `name` FROM `users` WHERE `name` LIKE ? ESCAPE '\\'"#
    );
}

#[test]
fn test_like_custom_escape_char() {
    use qbey::LikeExpression;

    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "test")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT `id`, `name` FROM `users` WHERE `name` LIKE ? ESCAPE '!'"#
    );
}

#[test]
fn test_not_like_escape_backslash() {
    use qbey::LikeExpression;

    let mut q = qbey("users");
    q.and_where(col("name").not_like(LikeExpression::contains("test")));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT `id`, `name` FROM `users` WHERE `name` NOT LIKE ? ESCAPE '\\'"#
    );
}

#[test]
fn test_join_subquery() {
    let mut sub = qbey::qbey("orders");
    sub.select(&["user_id", "total"]);
    sub.and_where(col("status").eq("shipped"));

    let mut q = qbey("users");
    q.join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` INNER JOIN (SELECT `user_id`, `total` FROM `orders` WHERE `status` = ?) AS `o` ON `users`.`id` = `o`.`user_id`"
    );
    assert_eq!(binds, vec![qbey::Value::String("shipped".to_string())]);
}

#[test]
fn test_order_by_expr() {
    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.order_by_expr(qbey::RawSql::new("RAND()"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT `id`, `name` FROM `users` ORDER BY RAND()");
}

#[test]
fn test_order_by_expr_mixed_with_col() {
    let mut q = qbey("users");
    q.select(&["id", "name"]);
    q.order_by(col("name").asc());
    q.order_by_expr(qbey::RawSql::new("RAND()"));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` ORDER BY `name` ASC, RAND()"
    );
}

#[test]
fn test_straight_join_subquery() {
    let mut sub = qbey::qbey("orders");
    sub.select(&["user_id", "total"]);

    let mut q = qbey("users");
    q.straight_join_subquery(sub, "o", table("users").col("id").eq_col("user_id"));
    q.select(&["id", "name"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `id`, `name` FROM `users` STRAIGHT_JOIN (SELECT `user_id`, `total` FROM `orders`) AS `o` ON `users`.`id` = `o`.`user_id`"
    );
}

#[test]
fn test_intersect() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let q = q1.intersect(&q2);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` INTERSECT SELECT `dept` FROM `contractor`"
    );
}

#[test]
fn test_intersect_all() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let q = q1.intersect_all(&q2);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` INTERSECT ALL SELECT `dept` FROM `contractor`"
    );
}

#[test]
fn test_except() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let q = q1.except(&q2);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` EXCEPT SELECT `dept` FROM `contractor`"
    );
}

#[test]
fn test_except_all() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let q = q1.except_all(&q2);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` EXCEPT ALL SELECT `dept` FROM `contractor`"
    );
}

#[test]
fn test_intersect_with_order_by_and_limit() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let mut q = q1.intersect(&q2);
    q.order_by(col("dept").asc());
    q.limit(5);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` INTERSECT SELECT `dept` FROM `contractor` ORDER BY `dept` ASC LIMIT 5"
    );
}

#[test]
fn test_except_with_order_by_and_limit() {
    let mut q1 = qbey("employee");
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.select(&["dept"]);

    let mut q = q1.except(&q2);
    q.order_by(col("dept").desc());
    q.limit(3);
    q.offset(1);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` EXCEPT SELECT `dept` FROM `contractor` ORDER BY `dept` DESC LIMIT 3 OFFSET 1"
    );
}

#[test]
fn test_col_count() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept`, COUNT(`id`) AS `cnt` FROM `employee` GROUP BY `dept`"
    );
}

#[test]
fn test_col_count_with_table_qualified() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(table("employee").col("id").count().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept`, COUNT(`employee`.`id`) AS `cnt` FROM `employee` GROUP BY `dept`"
    );
}

#[test]
fn test_count_all() {
    let mut q = qbey("employee");
    q.select(&["dept"]);
    q.add_select(qbey::count_all().as_("cnt"));
    q.group_by(&["dept"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept`, COUNT(*) AS `cnt` FROM `employee` GROUP BY `dept`"
    );
}

#[test]
fn test_count_one() {
    let mut q = qbey("employee");
    q.add_select(qbey::count_one().as_("cnt"));

    let (sql, _) = q.to_sql();
    assert_eq!(sql, "SELECT COUNT(1) AS `cnt` FROM `employee`");
}

#[test]
fn test_col_sum() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").sum().as_("total"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, SUM(`price`) AS `total` FROM `orders` GROUP BY `product`"
    );
}

#[test]
fn test_col_avg() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").avg().as_("avg_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, AVG(`price`) AS `avg_price` FROM `orders` GROUP BY `product`"
    );
}

#[test]
fn test_col_min() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").min().as_("min_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, MIN(`price`) AS `min_price` FROM `orders` GROUP BY `product`"
    );
}

#[test]
fn test_col_max() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("price").max().as_("max_price"));
    q.group_by(&["product"]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, MAX(`price`) AS `max_price` FROM `orders` GROUP BY `product`"
    );
}

#[test]
fn test_intersect_with_force_index() {
    let mut q1 = qbey("employee");
    q1.force_index(&["idx_dept"]);
    q1.select(&["dept"]);

    let mut q2 = qbey("contractor");
    q2.force_index(&["idx_dept"]);
    q2.select(&["dept"]);

    let q = q1.intersect(&q2);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `dept` FROM `employee` FORCE INDEX (idx_dept) INTERSECT SELECT `dept` FROM `contractor` FORCE INDEX (idx_dept)"
    );
}

#[test]
fn test_having() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, COUNT(`id`) AS `cnt` FROM `orders` GROUP BY `product` HAVING `cnt` > ?"
    );
    assert_eq!(binds, vec![qbey::Value::Int(5)]);
}

#[test]
fn test_having_with_where() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.and_where(col("status").eq("completed"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(5));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, COUNT(`id`) AS `cnt` FROM `orders` WHERE `status` = ? GROUP BY `product` HAVING `cnt` > ?"
    );
    assert_eq!(
        binds,
        vec![
            qbey::Value::String("completed".to_string()),
            qbey::Value::Int(5),
        ]
    );
}

#[test]
fn test_having_or() {
    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("cnt").gt(10));
    q.or_having(col("cnt").lt(2));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, COUNT(`id`) AS `cnt` FROM `orders` GROUP BY `product` HAVING `cnt` > ? OR `cnt` < ?"
    );
    assert_eq!(binds, vec![qbey::Value::Int(10), qbey::Value::Int(2)]);
}

#[test]
fn test_having_in_subquery() {
    let mut sub = qbey("popular_products");
    sub.select(&["product"]);

    let mut q = qbey("orders");
    q.select(&["product"]);
    q.add_select(col("id").count().as_("cnt"));
    q.group_by(&["product"]);
    q.and_having(col("product").included(sub));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        "SELECT `product`, COUNT(`id`) AS `cnt` FROM `orders` GROUP BY `product` HAVING `product` IN (SELECT `product` FROM `popular_products`)"
    );
    assert!(binds.is_empty());
}
