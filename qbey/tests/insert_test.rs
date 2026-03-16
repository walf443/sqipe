use qbey::*;

#[test]
fn test_insert_single_row() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(30)]
    );
}

#[test]
fn test_insert_multiple_rows() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("name", "Bob".into()), ("age", 25.into())]);
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::String("Bob".to_string()),
            Value::Int(25),
        ]
    );
}

#[test]
fn test_insert_reorders_columns() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("age", 25.into()), ("name", "Bob".into())]);
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::String("Bob".to_string()),
            Value::Int(25),
        ]
    );
}

#[test]
fn test_insert_from_select() {
    let mut sub = qbey("old_employee");
    sub.select(&["name", "age"]);
    sub.and_where(col("active").eq(true));

    let mut ins = qbey("employee").into_insert();
    ins.from_select(sub);
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" SELECT "name", "age" FROM "old_employee" WHERE "active" = ?"#
    );
    assert_eq!(binds, vec![Value::Bool(true)]);
}

#[test]
fn test_insert_with_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    let (sql, binds) = ins.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES ($1, $2)"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(30)]
    );
}

#[test]
fn test_insert_multiple_rows_with_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("name", "Bob".into()), ("age", 25.into())]);
    let (sql, binds) = ins.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES ($1, $2), ($3, $4)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::String("Bob".to_string()),
            Value::Int(25),
        ]
    );
}

#[test]
#[should_panic(expected = "INSERT requires at least one row")]
fn test_insert_no_values_panics() {
    let ins = qbey("employee").into_insert();
    let _ = ins.to_sql();
}

#[test]
#[should_panic(expected = "add_value requires at least one column-value pair")]
fn test_insert_empty_pairs_panics() {
    let mut ins = qbey("employee").into_insert();
    let empty: &[(&str, Value)] = &[];
    ins.add_value(empty);
}

#[test]
#[should_panic(expected = "column count mismatch")]
fn test_insert_column_count_mismatch_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("name", "Bob".into())]);
}

#[test]
#[should_panic(expected = "missing column")]
fn test_insert_missing_column_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("name", "Bob".into()), ("email", "bob@example.com".into())]);
}

#[test]
#[should_panic(expected = "Cannot mix from_select() with add_value()")]
fn test_insert_mix_select_after_values_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into())]);
    ins.from_select(qbey("old_employee"));
}

#[test]
#[should_panic(expected = "Cannot mix add_value() with from_select()")]
fn test_insert_mix_values_after_select_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.from_select(qbey("old_employee"));
    ins.add_value(&[("name", "Alice".into())]);
}

#[test]
#[should_panic(expected = "WHERE which is not supported in INSERT")]
fn test_insert_from_select_query_with_where_panics() {
    let mut q = qbey("employee");
    q.and_where(col("id").eq(1));
    let _ = q.into_insert();
}

#[test]
#[should_panic(expected = "duplicate column")]
fn test_insert_duplicate_column_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("name", "Bob".into())]);
}

#[test]
fn test_insert_with_col_expr() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age", "created_at") VALUES (?, ?, NOW())"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(30)]
    );
}

#[test]
fn test_insert_multiple_rows_with_col_expr() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_value(&[("name", "Bob".into()), ("age", 25.into())]);
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    ins.add_col_value_expr("updated_at", RawSql::new("NOW()"));
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age", "created_at", "updated_at") VALUES (?, ?, NOW(), NOW()), (?, ?, NOW(), NOW())"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::String("Bob".to_string()),
            Value::Int(25),
        ]
    );
}

#[test]
fn test_insert_col_expr_with_dialect() {
    struct PgDialect;
    impl Dialect for PgDialect {
        fn placeholder(&self, index: usize) -> String {
            format!("${}", index)
        }
    }

    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into())]);
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    let (sql, binds) = ins.to_sql_with(&PgDialect);
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "created_at") VALUES ($1, NOW())"#
    );
    assert_eq!(binds, vec![Value::String("Alice".to_string())]);
}

#[test]
fn test_insert_col_expr_with_col() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    ins.add_col_value_expr(col("created_at"), RawSql::new("NOW()"));
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age", "created_at") VALUES (?, ?, NOW())"#
    );
    assert_eq!(
        binds,
        vec![Value::String("Alice".to_string()), Value::Int(30)]
    );
}

#[test]
#[should_panic(expected = "already exists in value columns")]
fn test_insert_col_expr_duplicate_value_column_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into())]);
    ins.add_col_value_expr("name", RawSql::new("'default'"));
}

#[test]
#[should_panic(expected = "duplicate column")]
fn test_insert_col_expr_duplicate_expr_column_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
}

#[test]
#[should_panic(expected = "Cannot mix add_col_value_expr() with from_select()")]
fn test_insert_col_expr_after_from_select_panics() {
    let mut ins = qbey("employee").into_insert();
    ins.from_select(qbey("old_employee"));
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
}

#[test]
fn test_insert_col_expr_only() {
    let mut ins = qbey("employee").into_insert();
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("created_at") VALUES (NOW())"#
    );
    assert!(binds.is_empty());
}

#[test]
fn test_insert_col_expr_only_multiple() {
    let mut ins = qbey("employee").into_insert();
    ins.add_col_value_expr("created_at", RawSql::new("NOW()"));
    ins.add_col_value_expr("uuid", RawSql::new("UUID()"));
    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("created_at", "uuid") VALUES (NOW(), UUID())"#
    );
    assert!(binds.is_empty());
}

#[test]
fn test_insert_col_expr_with_qualified_col_ignores_table() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into())]);
    ins.add_col_value_expr(table("employee").col("created_at"), RawSql::new("NOW()"));
    let (sql, _) = ins.to_sql();
    // table qualifier is ignored in INSERT column lists
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "created_at") VALUES (?, NOW())"#
    );
}

#[test]
fn test_insert_tree_map_values() {
    let mut ins = qbey("employee").into_insert();
    ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    let tree = ins.to_tree();

    let mapped = tree.map_values(&|v: Value| match v {
        Value::String(s) => format!("str:{}", s),
        Value::Int(n) => format!("int:{}", n),
        _ => format!("{:?}", v),
    });

    assert_eq!(mapped.table, "employee");
    assert_eq!(mapped.columns, vec!["name", "age"]);
    match mapped.source {
        qbey::tree::InsertTreeSource::Values(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], vec!["str:Alice", "int:30"]);
        }
        _ => panic!("expected Values source"),
    }
}
