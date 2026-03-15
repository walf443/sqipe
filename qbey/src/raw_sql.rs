/// A raw SQL expression that is embedded verbatim into generated SQL.
///
/// This type exists to make it explicit that the caller is injecting raw SQL.
/// The expression is inserted **without escaping or parameterization**.
///
/// # Security
///
/// Never construct a `RawSql` from user-supplied input — doing so opens the
/// door to SQL injection. Only use hard-coded or application-controlled
/// expressions.
///
/// ```
/// use qbey::RawSql;
///
/// let expr = RawSql::new("RAND()");
/// ```
#[derive(Debug, Clone)]
pub struct RawSql(pub(crate) String);

impl RawSql {
    /// Create a new raw SQL expression.
    pub fn new(sql: &str) -> Self {
        Self(sql.to_string())
    }

    /// Return the raw SQL string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RawSql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
