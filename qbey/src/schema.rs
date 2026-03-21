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
    ($struct_name:ident, $table_name:expr, [$($col:ident),* $(,)?]) => {
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

            $(
                pub fn $col(&self) -> $crate::Col {
                    $crate::table(self.alias.unwrap_or($table_name)).col(stringify!($col))
                }
            )*

            pub fn all_columns(&self) -> Vec<$crate::Col> {
                vec![$(self.$col()),*]
            }
        }

        impl $crate::IntoFromTable for &$struct_name {
            fn into_from_table(self) -> (String, Option<String>) {
                self.table().into_from_table()
            }
        }
    };
}
