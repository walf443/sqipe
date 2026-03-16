use qbey::*;

struct Employee {
    name: String,
    age: i32,
}

impl ToInsertRow<Value> for Employee {
    fn to_insert_row(&self) -> Vec<(&'static str, Value)> {
        vec![
            ("name", self.name.as_str().into()),
            ("age", self.age.into()),
        ]
    }
}

#[test]
fn test_insert_from_vec_of_structs() {
    let employees = vec![
        Employee {
            name: "Alice".to_string(),
            age: 30,
        },
        Employee {
            name: "Bob".to_string(),
            age: 25,
        },
        Employee {
            name: "Charlie".to_string(),
            age: 35,
        },
    ];

    let mut ins = qbey("employee").into_insert();
    for e in &employees {
        ins.add_value(e);
    }

    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?), (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            Value::String("Alice".to_string()),
            Value::Int(30),
            Value::String("Bob".to_string()),
            Value::Int(25),
            Value::String("Charlie".to_string()),
            Value::Int(35),
        ]
    );
}

#[test]
fn test_insert_from_vec_of_structs_with_custom_value() {
    #[derive(Debug, Clone, PartialEq)]
    enum MyValue {
        Text(String),
        Int(i32),
    }

    impl From<&str> for MyValue {
        fn from(s: &str) -> Self {
            MyValue::Text(s.to_string())
        }
    }

    impl From<i32> for MyValue {
        fn from(n: i32) -> Self {
            MyValue::Int(n)
        }
    }

    struct Employee2 {
        name: String,
        age: i32,
    }

    impl ToInsertRow<MyValue> for Employee2 {
        fn to_insert_row(&self) -> Vec<(&'static str, MyValue)> {
            vec![
                ("name", self.name.as_str().into()),
                ("age", self.age.into()),
            ]
        }
    }

    let employees = vec![
        Employee2 {
            name: "Alice".to_string(),
            age: 30,
        },
        Employee2 {
            name: "Bob".to_string(),
            age: 25,
        },
    ];

    let mut ins = qbey_with::<MyValue>("employee").into_insert();
    for e in &employees {
        ins.add_value(e);
    }

    let (sql, binds) = ins.to_sql();
    assert_eq!(
        sql,
        r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?)"#
    );
    assert_eq!(
        binds,
        vec![
            MyValue::Text("Alice".to_string()),
            MyValue::Int(30),
            MyValue::Text("Bob".to_string()),
            MyValue::Int(25),
        ]
    );
}

/// Slice-based API still works alongside ToInsertRow.
#[test]
fn test_insert_slice_api_still_works() {
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
