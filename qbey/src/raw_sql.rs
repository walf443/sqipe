use crate::renderer::RenderConfig;
use crate::value::Value;

/// A raw SQL expression that is embedded verbatim into generated SQL,
/// optionally with bind parameters.
///
/// This type exists to make it explicit that the caller is injecting raw SQL.
/// When `.binds()` is called, `{}` in the SQL template is treated as a
/// placeholder and replaced with the dialect's bind parameter marker
/// (`?` for SQLite/MySQL, `$1`/`$2` for PostgreSQL). Without `.binds()`,
/// the SQL string is used as-is — any `{}` in the template is preserved
/// literally.
///
/// # Security
///
/// Never construct a `RawSql` from user-supplied input — doing so opens the
/// door to SQL injection. Only use hard-coded or application-controlled
/// expressions. The `binds()` method provides safe parameterization for values
/// within raw SQL expressions.
///
/// ```
/// use qbey::RawSql;
///
/// let expr: RawSql = RawSql::new("RAND()");
/// let expr_with_bind: RawSql = RawSql::new("CONCAT(foo, {})").binds(&["bar"]);
/// ```
#[derive(Debug, Clone)]
pub struct RawSql<V: Clone = Value> {
    pub(crate) sql: String,
    pub(crate) binds: Vec<V>,
}

impl<V: Clone> RawSql<V> {
    /// Create a new raw SQL expression.
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            binds: Vec::new(),
        }
    }

    /// Attach bind values to the `{}` placeholders in the SQL template.
    ///
    /// The number of values must match the number of `{}` placeholders.
    ///
    /// ```
    /// use qbey::RawSql;
    ///
    /// // Single bind
    /// let expr: RawSql = RawSql::new("CONCAT(foo, {})").binds(&["bar"]);
    ///
    /// // Multiple binds
    /// let expr: RawSql = RawSql::new("COALESCE({}, {})").binds(&["a", "b"]);
    /// ```
    pub fn binds(mut self, vals: &[impl Into<V> + Clone]) -> Self {
        self.binds = vals.iter().map(|v| v.clone().into()).collect();
        debug_assert_eq!(
            self.sql.matches("{}").count(),
            self.binds.len(),
            "number of {{}} placeholders ({}) must match number of bind values ({})",
            self.sql.matches("{}").count(),
            self.binds.len(),
        );
        self
    }

    /// Convert a `RawSql<Value>` into `RawSql<V>` for any `V`.
    ///
    /// Panics if the source has bind values, since `Value` cannot be
    /// automatically converted to an arbitrary `V`.
    pub fn from_default(raw: RawSql) -> Self {
        assert!(
            raw.binds.is_empty(),
            "Cannot convert RawSql with binds to a different value type"
        );
        RawSql::new(raw.as_str())
    }

    /// Return the raw SQL string (template, with `{}` placeholders if any).
    pub fn as_str(&self) -> &str {
        &self.sql
    }

    /// Render the SQL expression, replacing `{}` placeholders with
    /// dialect-specific bind markers and incrementing the bind counter.
    ///
    /// If there are no bind values, the SQL template is returned as-is.
    pub(crate) fn render(&self, cfg: &RenderConfig, bind_count: &mut usize) -> String {
        if self.binds.is_empty() {
            return self.sql.clone();
        }

        let mut result = String::new();
        let mut remaining_binds = self.binds.len();
        let parts: Vec<&str> = self.sql.split("{}").collect();
        for (i, part) in parts.iter().enumerate() {
            result.push_str(part);
            if i < parts.len() - 1 && remaining_binds > 0 {
                *bind_count += 1;
                result.push_str(&(cfg.ph)(*bind_count));
                remaining_binds -= 1;
            }
        }
        result
    }

    /// Transform all bind values in this expression.
    pub fn map_values<U: Clone>(self, f: &dyn Fn(V) -> U) -> RawSql<U> {
        RawSql {
            sql: self.sql,
            binds: self.binds.into_iter().map(f).collect(),
        }
    }
}

impl<V: Clone> std::fmt::Display for RawSql<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sql)
    }
}
