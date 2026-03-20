use qbey::*;

qbey_schema!(Users, "users", [id, name, email]);
qbey_schema!(Orders, "orders", [id, user_id, total, status]);

#[test]
fn test_schema_table_name() {
    let u = Users::new();
    assert_eq!(u.table_name(), "users");
}

#[test]
fn test_schema_select_all_columns() {
    let u = Users::new();
    let mut q = qbey("users");
    q.select(&u.all_columns());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "users"."id", "users"."name", "users"."email" FROM "users""#
    );
}

#[test]
fn test_schema_where_with_column() {
    let u = Users::new();
    let mut q = qbey("users");
    q.select(&u.all_columns());
    q.and_where(u.name().eq("Alice"));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "users"."id", "users"."name", "users"."email" FROM "users" WHERE "users"."name" = ?"#
    );
    assert_eq!(binds, vec![Value::String("Alice".to_string())]);
}

#[test]
fn test_schema_single_column_select() {
    let u = Users::new();
    let mut q = qbey("users");
    q.add_select(u.id());
    q.add_select(u.name());

    let (sql, _) = q.to_sql();
    assert_eq!(sql, r#"SELECT "users"."id", "users"."name" FROM "users""#);
}

#[test]
fn test_schema_alias() {
    let m = Users::new().as_("managers");
    assert_eq!(m.table_name(), "users");

    let mut q = qbey("managers");
    q.add_select(m.id());
    q.add_select(m.name());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "managers"."id", "managers"."name" FROM "managers""#
    );
}

#[test]
fn test_schema_alias_table_ref_for_join() {
    let u = Users::new();
    let o = Orders::new();
    let mut q = qbey("users");
    q.select(&[u.name()]);
    q.add_select(o.total());
    q.join(o.table(), u.id().eq(o.user_id()));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "users"."name", "orders"."total" FROM "users" INNER JOIN "orders" ON "users"."id" = "orders"."user_id""#
    );
}

#[test]
fn test_schema_self_join_with_alias() {
    qbey_schema!(Employees, "employees", [id, name, manager_id]);

    let e = Employees::new();
    let m = Employees::new().as_("mgr");
    let mut q = qbey("employees");
    q.select(&[e.name(), m.name().as_("manager_name")]);
    q.left_join(m.table(), e.manager_id().eq(m.id()));

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "employees"."name", "mgr"."name" AS "manager_name" FROM "employees" LEFT JOIN "employees" AS "mgr" ON "employees"."manager_id" = "mgr"."id""#
    );
}

#[test]
fn test_schema_with_order_by() {
    let u = Users::new();
    let mut q = qbey("users");
    q.select(&u.all_columns());
    q.order_by(u.name().asc());
    q.order_by(u.id().desc());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "users"."id", "users"."name", "users"."email" FROM "users" ORDER BY "users"."name" ASC, "users"."id" DESC"#
    );
}

#[test]
fn test_schema_with_aggregate() {
    let o = Orders::new();
    let mut q = qbey("orders");
    q.add_select(o.user_id());
    q.add_select(o.total().sum().as_("total_amount"));
    q.group_by(&[o.user_id()]);

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "orders"."user_id", SUM("orders"."total") AS "total_amount" FROM "orders" GROUP BY "orders"."user_id""#
    );
}

#[test]
fn test_schema_update() {
    let u = Users::new();
    let mut q = qbey("users").into_update();
    q.set(u.name(), "Bob");
    let q = q.and_where(u.id().eq(1));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"UPDATE "users" SET "name" = ? WHERE "users"."id" = ?"#
    );
    assert_eq!(binds, vec![Value::String("Bob".to_string()), Value::Int(1)]);
}

#[test]
fn test_schema_delete() {
    let u = Users::new();
    let q = qbey("users").into_delete().and_where(u.id().eq(1));

    let (sql, binds) = q.to_sql();
    assert_eq!(sql, r#"DELETE FROM "users" WHERE "users"."id" = ?"#);
    assert_eq!(binds, vec![Value::Int(1)]);
}

#[test]
fn test_schema_const_initialization() {
    const USERS: Users = Users::new();
    let mut q = qbey("users");
    q.select(&USERS.all_columns());

    let (sql, _) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "users"."id", "users"."name", "users"."email" FROM "users""#
    );
}

#[test]
fn test_schema_trailing_comma_in_columns() {
    qbey_schema!(Items, "items", [id, name, price,]);

    let i = Items::new();
    let (sql, _) = qbey("items").select(&i.all_columns()).to_sql();
    assert_eq!(
        sql,
        r#"SELECT "items"."id", "items"."name", "items"."price" FROM "items""#
    );
}
