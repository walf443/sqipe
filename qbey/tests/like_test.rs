use qbey::*;

#[test]
fn test_like_contains() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::contains("Ali")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
}

#[test]
fn test_like_starts_with() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::starts_with("Ali")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("Ali%".to_string())]);
}

#[test]
fn test_like_ends_with() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::ends_with("ice")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("%ice".to_string())]);
}

#[test]
fn test_not_like_contains() {
    let mut q = qbey("users");
    q.and_where(col("name").not_like(LikeExpression::contains("test")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "users" WHERE "name" NOT LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("%test%".to_string())]);
}

#[test]
fn test_like_escape_special_chars() {
    let mut q = qbey("products");
    q.and_where(col("name").like(LikeExpression::starts_with("a_b%")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("a\\_b\\%%".to_string())]);
}

#[test]
fn test_like_contains_escape_char_itself() {
    let mut q = qbey("products");
    q.and_where(col("name").like(LikeExpression::starts_with("a\\b")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("a\\\\b%".to_string())]);
}

#[test]
fn test_like_all_special_chars_combined() {
    let mut q = qbey("products");
    q.and_where(col("name").like(LikeExpression::starts_with("a_b%")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "products" WHERE "name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("a\\_b\\%%".to_string())]);
}

#[test]
fn test_like_qualified_col() {
    let mut q = qbey("users");
    q.as_("u");
    q.and_where(table("u").col("name").like(LikeExpression::contains("Ali")));
    q.select(&["id", "name"]);

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT "id", "name" FROM "users" AS "u" WHERE "u"."name" LIKE ? ESCAPE '\'"#
    );
    assert_eq!(binds, vec![Value::String("%Ali%".to_string())]);
}

#[test]
fn test_like_custom_escape_char() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::contains_escaped_by('!', "100%")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
    );
    assert_eq!(binds, vec![Value::String("%100!%%".to_string())]);
}

#[test]
fn test_like_custom_escape_starts_with() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::starts_with_escaped_by('!', "a_b")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
    );
    assert_eq!(binds, vec![Value::String("a!_b%".to_string())]);
}

#[test]
fn test_like_custom_escape_ends_with() {
    let mut q = qbey("users");
    q.and_where(col("name").like(LikeExpression::ends_with_escaped_by('!', "x%y")));

    let (sql, binds) = q.to_sql();
    assert_eq!(
        sql,
        r#"SELECT * FROM "users" WHERE "name" LIKE ? ESCAPE '!'"#
    );
    assert_eq!(binds, vec![Value::String("%x!%y".to_string())]);
}

#[test]
#[should_panic(expected = "escape character must not be")]
fn test_like_rejects_percent_as_escape() {
    LikeExpression::contains_escaped_by('%', "foo");
}

#[test]
#[should_panic(expected = "escape character must not be")]
fn test_like_rejects_underscore_as_escape() {
    LikeExpression::starts_with_escaped_by('_', "foo");
}

#[test]
#[should_panic(expected = "escape character must not be")]
fn test_like_rejects_single_quote_as_escape() {
    LikeExpression::ends_with_escaped_by('\'', "foo");
}
