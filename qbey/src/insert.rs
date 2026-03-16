use std::collections::HashMap;

use crate::Dialect;
use crate::tree::SelectTree;
use crate::value::Value;

use crate::renderer::RenderConfig;
use crate::tree::default_quote_identifier;

/// The source of values for an INSERT statement.
#[derive(Debug, Clone)]
pub(crate) enum InsertSource<V: Clone> {
    /// Explicit value rows provided via `add_value()`.
    Values(Vec<Vec<V>>),
    /// A subquery (INSERT ... SELECT ...).
    Select(Box<SelectTree<V>>),
}

/// An INSERT query builder, generic over the bind value type `V`.
///
/// Created via [`SelectQuery::into_insert()`] to convert a SELECT query builder
/// into an INSERT statement.
///
/// At least one row must be provided via [`add_value()`](InsertQuery::add_value)
/// or a subquery via [`from_select()`](InsertQuery::from_select) before calling
/// `to_sql()`. If neither is called, `to_sql()` will panic. When building rows
/// from a dynamic collection, the caller is responsible for ensuring the
/// collection is non-empty before calling `to_sql()`.
///
/// ```
/// use qbey::{qbey, Value};
///
/// let mut ins = qbey("employee").into_insert();
/// ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
/// let (sql, binds) = ins.to_sql();
/// assert_eq!(sql, r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?)"#);
/// assert_eq!(binds, vec![Value::String("Alice".to_string()), Value::Int(30)]);
/// ```
#[derive(Debug, Clone)]
pub struct InsertQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
    pub(crate) source: InsertSource<V>,
}

impl<V: Clone + std::fmt::Debug> InsertQuery<V> {
    pub(crate) fn new(table: String) -> Self {
        InsertQuery {
            table,
            columns: Vec::new(),
            source: InsertSource::Values(Vec::new()),
        }
    }

    /// Add a row of column-value pairs.
    ///
    /// The first call establishes the column list. Subsequent calls must provide
    /// the same set of column names (order may differ — values are reordered to
    /// match the column order established by the first call).
    ///
    /// # Panics
    ///
    /// - Panics if called after [`from_select()`](InsertQuery::from_select).
    /// - Panics if `pairs` is empty.
    /// - Panics if the column set does not match the first call's column set.
    ///
    /// ```
    /// use qbey::{qbey, Value};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("name", "Alice".into()), ("age", 30.into())]);
    /// ins.add_value(&[("age", 25.into()), ("name", "Bob".into())]);
    /// let (sql, binds) = ins.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?)"#
    /// );
    /// assert_eq!(
    ///     binds,
    ///     vec![
    ///         Value::String("Alice".to_string()), Value::Int(30),
    ///         Value::String("Bob".to_string()), Value::Int(25),
    ///     ]
    /// );
    /// ```
    pub fn add_value(&mut self, pairs: &[(&str, V)]) -> &mut Self {
        assert!(
            !pairs.is_empty(),
            "add_value requires at least one column-value pair"
        );
        assert!(
            matches!(self.source, InsertSource::Values(_)),
            "Cannot mix add_value() with from_select()"
        );

        if self.columns.is_empty() {
            // First call: establish column order.
            self.columns = pairs.iter().map(|(c, _)| c.to_string()).collect();
            let row: Vec<V> = pairs.iter().map(|(_, v)| v.clone()).collect();
            if let InsertSource::Values(ref mut rows) = self.source {
                rows.push(row);
            }
        } else {
            // Subsequent calls: validate and reorder.
            assert_eq!(
                pairs.len(),
                self.columns.len(),
                "add_value: column count mismatch (expected {}, got {})",
                self.columns.len(),
                pairs.len()
            );

            let pair_map: HashMap<&str, &V> = pairs.iter().map(|(c, v)| (*c, v)).collect();

            let mut row = Vec::with_capacity(self.columns.len());
            for col_name in &self.columns {
                let val = pair_map.get(col_name.as_str()).unwrap_or_else(|| {
                    panic!(
                        "add_value: missing column {:?} (expected columns: {:?})",
                        col_name, self.columns
                    )
                });
                row.push((*val).clone());
            }

            if let InsertSource::Values(ref mut rows) = self.source {
                rows.push(row);
            }
        }

        self
    }

    /// Use a SELECT query as the source of rows (INSERT ... SELECT ...).
    ///
    /// # Panics
    ///
    /// Panics if `add_value()` has already been called.
    ///
    /// ```
    /// use qbey::{qbey, col};
    ///
    /// let mut sub = qbey("old_employee");
    /// sub.select(&["name", "age"]);
    /// sub.and_where(col("active").eq(true));
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.from_select(sub);
    /// let (sql, binds) = ins.to_sql();
    /// assert_eq!(
    ///     sql,
    ///     r#"INSERT INTO "employee" SELECT "name", "age" FROM "old_employee" WHERE "active" = ?"#
    /// );
    /// ```
    pub fn from_select(&mut self, sub: impl crate::query::IntoSelectTree<V>) -> &mut Self {
        if let InsertSource::Values(ref rows) = self.source {
            assert!(rows.is_empty(), "Cannot mix from_select() with add_value()");
        }
        self.source = InsertSource::Select(Box::new(sub.into_select_tree()));
        self
    }

    /// Build an InsertTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn to_tree(&self) -> crate::tree::InsertTree<V> {
        match &self.source {
            InsertSource::Values(rows) => {
                assert!(
                    !rows.is_empty(),
                    "INSERT requires at least one row of values or a SELECT subquery"
                );
                crate::tree::InsertTree {
                    table: self.table.clone(),
                    columns: self.columns.clone(),
                    source: crate::tree::InsertTreeSource::Values(rows.clone()),
                }
            }
            InsertSource::Select(sub) => crate::tree::InsertTree {
                table: self.table.clone(),
                columns: self.columns.clone(),
                source: crate::tree::InsertTreeSource::Select(sub.clone()),
            },
        }
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        crate::renderer::insert::render_insert(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        crate::renderer::insert::render_insert(
            &tree,
            &RenderConfig::from_dialect(&ph, &qi, dialect),
        )
    }
}
