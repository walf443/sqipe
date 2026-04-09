use std::collections::{HashMap, HashSet};

use crate::Dialect;
use crate::column::Col;
use crate::raw_sql::RawSql;
use crate::tree::SelectTree;
use crate::value::Value;

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

    /// Add multiple rows at once from a slice of [`ToInsertRow<V>`] implementors.
    ///
    /// This is equivalent to calling [`add_value()`](InsertQueryBuilder::add_value)
    /// in a loop, but more convenient when you already have a collection of rows.
    ///
    /// # Panics
    ///
    /// Same as [`add_value()`](InsertQueryBuilder::add_value).
    fn add_values(&mut self, rows: &[impl ToInsertRow<V>]) -> &mut Self {
        for row in rows {
            self.add_value(row);
        }
        self
    }

    /// Add an extra column whose value is a raw SQL expression applied to every row.
    ///
    /// This is useful for columns like `created_at` that should use a database
    /// function such as `NOW()` rather than a bind parameter.
    ///
    /// # Panics
    ///
    /// Panics if the column name duplicates a column already added via
    /// `add_value()` or a previous `add_col_value_expr()` call.
    fn add_col_value_expr(&mut self, column: impl Into<Col>, expr: RawSql<V>) -> &mut Self;

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

/// A clause in the ON CONFLICT DO UPDATE SET list.
#[cfg(feature = "conflict")]
#[derive(Debug, Clone)]
pub(crate) enum OnConflictUpdateClause<V: Clone> {
    /// A column set to a bind value: `"col" = ?`.
    Value(String, V),
    /// A raw SQL expression: `"col" = "col" + 1`.
    Expr(RawSql<V>),
    /// A column set to EXCLUDED."col".
    Excluded(String),
}

/// The action to take on conflict.
#[cfg(feature = "conflict")]
#[derive(Debug, Clone)]
pub(crate) enum OnConflict<V: Clone> {
    DoNothing {
        columns: Vec<String>,
    },
    DoUpdate {
        columns: Vec<String>,
        sets: Vec<OnConflictUpdateClause<V>>,
    },
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
    pub(crate) col_exprs: Vec<(String, RawSql<V>)>,
    /// ON CONFLICT action (PostgreSQL, SQLite).
    #[cfg(feature = "conflict")]
    pub(crate) on_conflict: Option<OnConflict<V>>,
    /// Columns to return via RETURNING clause (non-standard SQL).
    #[cfg(feature = "returning")]
    pub(crate) returning_columns: Vec<crate::Col>,
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

    fn add_col_value_expr(&mut self, column: impl Into<Col>, expr: RawSql<V>) -> &mut Self {
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
            #[cfg(feature = "conflict")]
            on_conflict: None,
            #[cfg(feature = "returning")]
            returning_columns: Vec::new(),
        }
    }

    /// Add columns to the RETURNING clause (non-standard SQL; PostgreSQL, SQLite, MariaDB).
    ///
    /// Columns are accumulated — calling this method multiple times appends
    /// to the existing list rather than replacing it.
    ///
    /// ```
    /// use qbey::{qbey, col, Value, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("name", "Alice".into())]);
    /// ins.returning(&[col("id"), col("created_at")]);
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("name") VALUES (?) RETURNING "id", "created_at""#);
    /// ```
    #[cfg(feature = "returning")]
    pub fn returning(&mut self, cols: &[crate::Col]) -> &mut Self {
        for col in cols {
            self.returning_columns.push(col.clone());
        }
        self
    }

    /// Add an ON CONFLICT (...) DO NOTHING clause (PostgreSQL, SQLite).
    ///
    /// Accepts `&str` or `Col` (from `qbey_schema!`). When `Col` is passed,
    /// the table prefix is ignored — only the column name is used.
    ///
    /// # Panics
    ///
    /// - Panics if `columns` is empty.
    /// - Panics if an ON CONFLICT clause has already been set.
    ///
    /// ```
    /// use qbey::{qbey, Value, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
    /// ins.on_conflict_do_nothing(&["id"]);
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("id", "name") VALUES (?, ?) ON CONFLICT ("id") DO NOTHING"#);
    /// ```
    #[cfg(feature = "conflict")]
    pub fn on_conflict_do_nothing(&mut self, columns: &[impl Into<Col> + Clone]) -> &mut Self {
        assert!(
            !columns.is_empty(),
            "on_conflict_do_nothing: columns must not be empty"
        );
        assert!(
            self.on_conflict.is_none(),
            "on_conflict_do_nothing: ON CONFLICT clause already set"
        );
        self.on_conflict = Some(OnConflict::DoNothing {
            columns: columns.iter().map(|c| c.clone().into().column).collect(),
        });
        self
    }

    /// Add an ON CONFLICT (...) DO UPDATE SET col = ? clause with a bind value.
    ///
    /// Accepts `&str` or `Col` for `columns` and `col`. When `Col` is passed,
    /// the table prefix is ignored — only the column name is used.
    ///
    /// # Panics
    ///
    /// - Panics if `columns` is empty.
    /// - Panics if an ON CONFLICT clause has already been set.
    ///
    /// ```
    /// use qbey::{qbey, Value, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("name", "Alice".into())]);
    /// ins.on_conflict_do_update(&["id"], "name", "Bob");
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("id", "name") VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = ?"#);
    /// ```
    #[cfg(feature = "conflict")]
    pub fn on_conflict_do_update(
        &mut self,
        columns: &[impl Into<Col> + Clone],
        col: impl Into<Col>,
        val: impl Into<V>,
    ) -> &mut Self {
        assert!(
            !columns.is_empty(),
            "on_conflict_do_update: columns must not be empty"
        );
        assert!(
            self.on_conflict.is_none(),
            "on_conflict_do_update: ON CONFLICT clause already set"
        );
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|c| c.clone().into().column).collect(),
            sets: vec![OnConflictUpdateClause::Value(col.into().column, val.into())],
        });
        self
    }

    /// Add an ON CONFLICT (...) DO UPDATE SET with a raw SQL expression.
    ///
    /// Accepts `&str` or `Col` for `columns`. When `Col` is passed,
    /// the table prefix is ignored — only the column name is used.
    ///
    /// # Panics
    ///
    /// - Panics if `columns` is empty.
    /// - Panics if an ON CONFLICT clause has already been set.
    ///
    /// ```
    /// use qbey::{qbey, Value, RawSql, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("age", 30.into())]);
    /// ins.on_conflict_do_update_expr(&["id"], RawSql::new(r#""age" = "age" + 1"#));
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("id", "age") VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "age" = "age" + 1"#);
    /// ```
    #[cfg(feature = "conflict")]
    pub fn on_conflict_do_update_expr(
        &mut self,
        columns: &[impl Into<Col> + Clone],
        expr: RawSql<V>,
    ) -> &mut Self {
        assert!(
            !columns.is_empty(),
            "on_conflict_do_update_expr: columns must not be empty"
        );
        assert!(
            self.on_conflict.is_none(),
            "on_conflict_do_update_expr: ON CONFLICT clause already set"
        );
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|c| c.clone().into().column).collect(),
            sets: vec![OnConflictUpdateClause::Expr(expr)],
        });
        self
    }

    /// Add an ON CONFLICT (...) DO UPDATE SET col = EXCLUDED.col for each update column.
    ///
    /// Accepts `&str` or `Col` (from `qbey_schema!`). When `Col` is passed,
    /// the table prefix is ignored — only the column name is used.
    ///
    /// # Panics
    ///
    /// - Panics if `columns` is empty.
    /// - Panics if an ON CONFLICT clause has already been set.
    ///
    /// ```
    /// use qbey::{qbey, Value, InsertQueryBuilder};
    ///
    /// let mut ins = qbey("employee").into_insert();
    /// ins.add_value(&[("id", 1.into()), ("name", "Alice".into()), ("age", 30.into())]);
    /// ins.on_conflict_do_update_with_excluded(&["id"], &["name", "age"]);
    /// let (sql, _) = ins.to_sql();
    /// assert_eq!(sql, r#"INSERT INTO "employee" ("id", "name", "age") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "age" = EXCLUDED."age""#);
    /// ```
    #[cfg(feature = "conflict")]
    pub fn on_conflict_do_update_with_excluded(
        &mut self,
        columns: &[impl Into<Col> + Clone],
        update_columns: &[impl Into<Col> + Clone],
    ) -> &mut Self {
        assert!(
            !columns.is_empty(),
            "on_conflict_do_update_with_excluded: columns must not be empty"
        );
        assert!(
            self.on_conflict.is_none(),
            "on_conflict_do_update_with_excluded: ON CONFLICT clause already set"
        );
        let sets: Vec<OnConflictUpdateClause<V>> = update_columns
            .iter()
            .map(|c| {
                let col: Col = c.clone().into();
                OnConflictUpdateClause::Excluded(col.column)
            })
            .collect();
        self.on_conflict = Some(OnConflict::DoUpdate {
            columns: columns.iter().map(|c| c.clone().into().column).collect(),
            sets,
        });
        self
    }

    /// Build an InsertTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn to_tree(&self) -> crate::tree::InsertTree<V> {
        self.clone().into_tree()
    }

    /// Consume this query and build an InsertTree AST by moving values
    /// instead of cloning. More efficient than `to_tree()` for large inserts.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn into_tree(self) -> crate::tree::InsertTree<V> {
        let mut tokens = Vec::new();
        match self.source {
            InsertSource::Values(rows) => {
                assert!(
                    !rows.is_empty() || !self.col_exprs.is_empty(),
                    "INSERT requires at least one row of values, a col_expr, or a SELECT subquery"
                );
                let col_exprs: Vec<(String, String)> = self
                    .col_exprs
                    .into_iter()
                    .map(|(c, e)| (c, e.as_str().to_string()))
                    .collect();
                tokens.push(crate::tree::InsertToken::InsertInto {
                    table: self.table,
                    columns: self.columns,
                    col_exprs,
                });
                let rows = if rows.is_empty() { vec![vec![]] } else { rows };
                tokens.push(crate::tree::InsertToken::Values(rows));
            }
            InsertSource::Select(sub) => {
                tokens.push(crate::tree::InsertToken::InsertInto {
                    table: self.table,
                    columns: self.columns,
                    col_exprs: Vec::new(),
                });
                tokens.push(crate::tree::InsertToken::SelectSource(sub));
            }
        }
        #[cfg(feature = "conflict")]
        if let Some(on_conflict) = self.on_conflict {
            match on_conflict {
                OnConflict::DoNothing { columns } => {
                    tokens.push(crate::tree::InsertToken::OnConflictDoNothing { columns });
                }
                OnConflict::DoUpdate { columns, sets } => {
                    let set_clauses = sets
                        .into_iter()
                        .map(|clause| match clause {
                            OnConflictUpdateClause::Value(col, val) => {
                                crate::SetClause::Value(col, val)
                            }
                            OnConflictUpdateClause::Expr(expr) => crate::SetClause::Expr(expr),
                            OnConflictUpdateClause::Excluded(col) => {
                                crate::SetClause::Excluded(col)
                            }
                        })
                        .collect();
                    tokens.push(crate::tree::InsertToken::OnConflictDoUpdate {
                        columns,
                        sets: set_clauses,
                    });
                }
            }
        }
        #[cfg(feature = "returning")]
        if !self.returning_columns.is_empty() {
            tokens.push(crate::tree::InsertToken::Returning(self.returning_columns));
        }
        crate::tree::InsertTree { tokens }
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
        self.clone().into_sql_with(dialect)
    }

    /// Consume this query and build standard SQL with `?` placeholders.
    /// More efficient than `to_sql()` as it avoids cloning the query into a tree.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn into_sql(self) -> (String, Vec<V>) {
        self.into_sql_with(&crate::DefaultDialect)
    }

    /// Consume this query and build SQL with dialect-specific placeholders and quoting.
    /// More efficient than `to_sql_with()` as it avoids cloning the query into a tree.
    ///
    /// # Panics
    ///
    /// Panics if no values or subquery have been provided.
    pub fn into_sql_with(self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        self.into_tree().into_sql_with(dialect)
    }
}
