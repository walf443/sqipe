use std::collections::{HashMap, HashSet};

use crate::Dialect;
use crate::column::Col;
use crate::raw_sql::RawSql;
use crate::tree::SelectTree;
use crate::value::Value;

use crate::renderer::RenderConfig;

/// Trait for types that can be converted into a row of column-value pairs
/// for use with [`InsertQuery::add_value()`].
///
/// Implement this trait on your domain structs to enable direct insertion:
///
/// ```
/// use qbey::{qbey, Value, ToInsertRow, InsertQueryBuilder};
///
/// struct Employee {
///     name: String,
///     age: i32,
/// }
///
/// impl ToInsertRow<Value> for Employee {
///     fn to_insert_row(&self) -> Vec<(&'static str, Value)> {
///         vec![
///             ("name", self.name.as_str().into()),
///             ("age", self.age.into()),
///         ]
///     }
/// }
///
/// let employees = vec![
///     Employee { name: "Alice".to_string(), age: 30 },
///     Employee { name: "Bob".to_string(), age: 25 },
/// ];
///
/// let mut ins = qbey("employee").into_insert();
/// for e in &employees {
///     ins.add_value(e);
/// }
///
/// let (sql, binds) = ins.to_sql();
/// assert_eq!(sql, r#"INSERT INTO "employee" ("name", "age") VALUES (?, ?), (?, ?)"#);
/// ```
pub trait ToInsertRow<V: Clone> {
    fn to_insert_row(&self) -> Vec<(&'static str, V)>;
}

impl<V: Clone> ToInsertRow<V> for [(&'static str, V)] {
    fn to_insert_row(&self) -> Vec<(&'static str, V)> {
        self.to_vec()
    }
}

impl<V: Clone, const N: usize> ToInsertRow<V> for [(&'static str, V); N] {
    fn to_insert_row(&self) -> Vec<(&'static str, V)> {
        self.to_vec()
    }
}

/// Trait for INSERT query builder methods.
///
/// Implement this trait on dialect-specific INSERT wrappers to ensure they
/// expose the same builder API as the core [`InsertQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait InsertQueryBuilder<V: Clone> {
    /// Add a row of column-value pairs.
    ///
    /// Accepts any type that implements [`ToInsertRow<V>`], including:
    /// - A slice of `(&str, V)` tuples: `&[("name", "Alice".into())]`
    /// - A custom struct that implements `ToInsertRow<V>`
    ///
    /// The first call establishes the column list. Subsequent calls must provide
    /// the same set of column names (order may differ — values are reordered to
    /// match the column order established by the first call).
    ///
    /// # Panics
    ///
    /// - Panics if called after [`from_select()`](InsertQueryBuilder::from_select).
    /// - Panics if the row is empty.
    /// - Panics if the column set does not match the first call's column set.
    fn add_value(&mut self, row: &(impl ToInsertRow<V> + ?Sized)) -> &mut Self;

    /// Add an extra column whose value is a raw SQL expression applied to every row.
    ///
    /// This is useful for columns like `created_at` that should use a database
    /// function such as `NOW()` rather than a bind parameter.
    ///
    /// # Panics
    ///
    /// Panics if the column name duplicates a column already added via
    /// `add_value()` or a previous `add_col_value_expr()` call.
    fn add_col_value_expr(&mut self, column: impl Into<Col>, expr: RawSql) -> &mut Self;

    /// Use a SELECT query as the source of rows (INSERT ... SELECT ...).
    ///
    /// # Panics
    ///
    /// Panics if `add_value()` has already been called.
    #[allow(clippy::wrong_self_convention)]
    fn from_select(&mut self, sub: impl crate::query::IntoSelectTree<V>) -> &mut Self;
}

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
/// use qbey::{qbey, Value, InsertQueryBuilder};
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
    /// Extra columns whose values are raw SQL expressions (e.g., `NOW()`).
    /// These are appended after the normal bind-value columns in every row.
    pub(crate) col_exprs: Vec<(String, RawSql)>,
}

impl<V: Clone + std::fmt::Debug> InsertQueryBuilder<V> for InsertQuery<V> {
    fn add_value(&mut self, row: &(impl ToInsertRow<V> + ?Sized)) -> &mut Self {
        let pairs = row.to_insert_row();
        assert!(
            !pairs.is_empty(),
            "add_value requires at least one column-value pair"
        );
        assert!(
            matches!(self.source, InsertSource::Values(_)),
            "Cannot mix add_value() with from_select()"
        );

        if self.columns.is_empty() {
            self.columns = pairs.iter().map(|(c, _)| c.to_string()).collect();
            {
                let mut seen = HashSet::with_capacity(self.columns.len());
                for col in &self.columns {
                    assert!(
                        seen.insert(col.as_str()),
                        "add_value: duplicate column {:?}",
                        col
                    );
                }
            }
            let row: Vec<V> = pairs.into_iter().map(|(_, v)| v).collect();
            if let InsertSource::Values(ref mut rows) = self.source {
                rows.push(row);
            }
        } else {
            assert_eq!(
                pairs.len(),
                self.columns.len(),
                "add_value: column count mismatch (expected {}, got {})",
                self.columns.len(),
                pairs.len()
            );

            let pair_map: HashMap<&str, V> = pairs.into_iter().collect();

            let mut row = Vec::with_capacity(self.columns.len());
            for col_name in &self.columns {
                let val = pair_map.get(col_name.as_str()).unwrap_or_else(|| {
                    panic!(
                        "add_value: missing column {:?} (expected columns: {:?})",
                        col_name, self.columns
                    )
                });
                row.push(val.clone());
            }

            if let InsertSource::Values(ref mut rows) = self.source {
                rows.push(row);
            }
        }

        self
    }

    fn add_col_value_expr(&mut self, column: impl Into<Col>, expr: RawSql) -> &mut Self {
        let column = column.into().column;
        assert!(
            matches!(self.source, InsertSource::Values(_)),
            "Cannot mix add_col_value_expr() with from_select()"
        );
        assert!(
            !self.columns.iter().any(|c| c == &column),
            "add_col_value_expr: column {:?} already exists in value columns",
            column
        );
        assert!(
            !self.col_exprs.iter().any(|(c, _)| c == &column),
            "add_col_value_expr: duplicate column {:?}",
            column
        );
        self.col_exprs.push((column, expr));
        self
    }

    fn from_select(&mut self, sub: impl crate::query::IntoSelectTree<V>) -> &mut Self {
        if let InsertSource::Values(ref rows) = self.source {
            assert!(rows.is_empty(), "Cannot mix from_select() with add_value()");
        }
        self.source = InsertSource::Select(Box::new(sub.into_select_tree()));
        self
    }
}

impl<V: Clone + std::fmt::Debug> InsertQuery<V> {
    pub(crate) fn new(table: String) -> Self {
        InsertQuery {
            table,
            columns: Vec::new(),
            source: InsertSource::Values(Vec::new()),
            col_exprs: Vec::new(),
        }
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
                    !rows.is_empty() || !self.col_exprs.is_empty(),
                    "INSERT requires at least one row of values, a col_expr, or a SELECT subquery"
                );
                let col_exprs = self
                    .col_exprs
                    .iter()
                    .map(|(c, e)| (c.clone(), e.as_str().to_string()))
                    .collect();
                // When only col_exprs are provided (no add_value rows),
                // produce a single empty row so the renderer emits one VALUES tuple.
                let rows = if rows.is_empty() {
                    vec![vec![]]
                } else {
                    rows.clone()
                };
                crate::tree::InsertTree {
                    table: self.table.clone(),
                    columns: self.columns.clone(),
                    source: crate::tree::InsertTreeSource::Values(rows),
                    col_exprs,
                }
            }
            InsertSource::Select(sub) => crate::tree::InsertTree {
                table: self.table.clone(),
                columns: self.columns.clone(),
                source: crate::tree::InsertTreeSource::Select(sub.clone()),
                col_exprs: Vec::new(),
            },
        }
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
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
