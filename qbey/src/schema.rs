/// Define a typed schema struct for a database table.
///
/// Generates a struct with methods for each column that return [`Col`](crate::Col)
/// references qualified with the table name, supporting aliasing for self-joins.
///
/// # Example
///
/// ```
/// use qbey::qbey_schema;
/// use qbey::prelude::*;
/// use qbey::qbey;
///
/// qbey_schema!(Users, "users", [id, name, email]);
///
/// let u = Users::new();
/// let mut q = qbey(&u);
/// q.select(&u.all_columns());
/// q.and_where(u.name().eq("Alice"));
/// let (sql, _binds) = q.to_sql();
/// assert_eq!(sql, r#"SELECT "users"."id", "users"."name", "users"."email" FROM "users" WHERE "users"."name" = ?"#);
/// ```
///
/// # Self-join with alias
///
/// ```
/// use qbey::qbey_schema;
/// use qbey::prelude::*;
/// use qbey::qbey;
///
/// qbey_schema!(Users, "users", [id, name, manager_id]);
///
/// let u = Users::new();
/// let m = Users::new().as_("managers");
/// let mut q = qbey(&u);
/// q.select(&[u.name(), m.name().as_("manager_name")]);
/// q.left_join(
///     &m,
///     u.manager_id().eq(m.id()),
/// );
/// let (sql, _binds) = q.to_sql();
/// assert_eq!(sql, r#"SELECT "users"."name", "managers"."name" AS "manager_name" FROM "users" LEFT JOIN "users" AS "managers" ON "users"."manager_id" = "managers"."id""#);
/// ```
///
/// # Reserved words as column names
///
/// Use Rust raw identifiers for columns whose names are Rust reserved words:
///
/// ```
/// use qbey::qbey_schema;
/// use qbey::prelude::*;
/// use qbey::qbey;
///
/// qbey_schema!(Events, "events", [id, r#type]);
///
/// let e = Events::new();
/// let mut q = qbey(&e);
/// q.add_select(e.r#type());
/// let (sql, _binds) = q.to_sql();
/// assert_eq!(sql, r#"SELECT "events"."type" FROM "events""#);
/// ```
///
/// # Renaming columns
///
/// Use `rust_name = "sql_name"` when you want the Rust method name to differ
/// from the SQL column name. This is useful for columns that conflict with
/// built-in method names (`table`, `table_name`, `as_`, `all_columns`, `new`):
///
/// ```
/// use qbey::qbey_schema;
/// use qbey::prelude::*;
/// use qbey::qbey;
///
/// qbey_schema!(Features, "features", [id, name, is_new = "new"]);
///
/// let f = Features::new();
/// let mut q = qbey(&f);
/// q.select(&f.all_columns());
/// q.and_where(f.is_new().eq(true));
/// let (sql, _binds) = q.to_sql();
/// assert_eq!(sql, r#"SELECT "features"."id", "features"."name", "features"."new" FROM "features" WHERE "features"."new" = ?"#);
/// ```
///
/// The SQL name must be a string literal:
///
/// ```compile_fail
/// use qbey::qbey_schema;
///
/// qbey_schema!(Bad, "bad", [id, col = 42]);
/// ```
///
/// # Adding custom methods
///
/// The generated struct is a regular Rust struct, so you can add your own
/// methods with a separate `impl` block:
///
/// ```
/// use qbey::qbey_schema;
/// use qbey::Col;
///
/// qbey_schema!(Users, "users", [id, name, email]);
///
/// impl Users {
///     /// Returns columns typically needed for a list view.
///     pub fn list_columns(&self) -> Vec<Col> {
///         vec![self.id(), self.name()]
///     }
/// }
///
/// let u = Users::new();
/// assert_eq!(u.list_columns().len(), 2);
/// ```
#[macro_export]
macro_rules! qbey_schema {
    // Entry point: parse column definitions and forward to internal macro
    ($struct_name:ident, $table_name:expr, [$($col_def:tt)*]) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [] $($col_def)*);
    };
}

/// Internal macro that parses column definitions one by one.
/// Accumulates parsed columns as `[rust_ident, sql_name]` pairs.
#[doc(hidden)]
#[macro_export]
macro_rules! __qbey_schema_parse {
    // Renamed column: `col = "sql_name"` followed by comma and more
    ($struct_name:ident, $table_name:expr, [$($parsed:tt)*] $col:ident = $sql_name:expr, $($rest:tt)*) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [$($parsed)* [$col, $sql_name]] $($rest)*);
    };
    // Renamed column: `col = "sql_name"` at end
    ($struct_name:ident, $table_name:expr, [$($parsed:tt)*] $col:ident = $sql_name:expr) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [$($parsed)* [$col, $sql_name]]);
    };
    // Plain column followed by comma and more
    ($struct_name:ident, $table_name:expr, [$($parsed:tt)*] $col:ident, $($rest:tt)*) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [$($parsed)* [$col]] $($rest)*);
    };
    // Plain column at end
    ($struct_name:ident, $table_name:expr, [$($parsed:tt)*] $col:ident) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [$($parsed)* [$col]]);
    };
    // Trailing comma only
    ($struct_name:ident, $table_name:expr, [$($parsed:tt)*] ,) => {
        $crate::__qbey_schema_parse!($struct_name, $table_name, [$($parsed)*]);
    };
    // Terminal: all columns parsed, generate the struct
    ($struct_name:ident, $table_name:expr, [$([$($col_spec:tt)*])*]) => {
        $crate::__qbey_schema_emit!($struct_name, $table_name, $([$($col_spec)*]),*);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __qbey_schema_emit {
    ($struct_name:ident, $table_name:expr, $([$($col_spec:tt)*]),*) => {
        const _: &str = $table_name;

        #[allow(dead_code)]
        pub struct $struct_name {
            alias: Option<&'static str>,
        }

        #[allow(dead_code)]
        impl $struct_name {
            pub const fn new() -> Self {
                $struct_name { alias: None }
            }

            pub fn table_name(&self) -> &'static str {
                $table_name
            }

            /// Returns a `TableRef` suitable for FROM/JOIN clauses.
            /// When aliased, returns `table("name").as_("alias")`.
            pub fn table(&self) -> $crate::TableRef {
                match self.alias {
                    Some(alias) => $crate::table($table_name).as_(alias),
                    None => $crate::table($table_name),
                }
            }

            /// Create an aliased copy of this schema, useful for self-joins.
            ///
            /// Column accessors on the returned instance are qualified with the
            /// alias, and `table()` returns `table("original").as_("alias")` so
            /// it can be passed directly to `join` / `left_join`.
            pub fn as_(&self, alias: &'static str) -> Self {
                $struct_name { alias: Some(alias) }
            }

            $($crate::__qbey_schema_col!($table_name, $($col_spec)*);)*

            pub fn all_columns(&self) -> Vec<$crate::Col> {
                vec![$($crate::__qbey_schema_col_call!(self, $($col_spec)*)),*]
            }
        }

        impl $crate::IntoFromTable for &$struct_name {
            fn into_from_table(self) -> (String, Option<String>) {
                self.table().into_from_table()
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __qbey_schema_col {
    ($table_name:expr, $col:ident, $sql_name:expr) => {
        pub fn $col(&self) -> $crate::Col {
            $crate::table(self.alias.unwrap_or($table_name)).col($sql_name)
        }
    };
    ($table_name:expr, $col:ident) => {
        pub fn $col(&self) -> $crate::Col {
            let col_name = stringify!($col).trim_start_matches("r#");
            $crate::table(self.alias.unwrap_or($table_name)).col(col_name)
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __qbey_schema_col_call {
    ($self:ident, $col:ident $(, $sql_name:expr)?) => {
        $self.$col()
    };
}
