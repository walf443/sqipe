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
