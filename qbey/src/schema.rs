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
/// let mut q = qbey("users");
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
/// let mut q = qbey("users");
/// q.select(&[u.name(), m.name().as_("manager_name")]);
/// q.left_join(
///     m.table(),
///     u.manager_id().eq_col(m.id()),
/// );
/// let (sql, _binds) = q.to_sql();
/// assert_eq!(sql, r#"SELECT "users"."name", "managers"."name" AS "manager_name" FROM "users" LEFT JOIN "users" AS "managers" ON "users"."manager_id" = "managers"."id""#);
/// ```
#[macro_export]
macro_rules! qbey_schema {
    ($struct_name:ident, $table_name:expr, [$($col:ident),* $(,)?]) => {
        pub struct $struct_name {
            /// Table reference using the effective name (alias or table name)
            /// for qualifying column references.
            col_table: $crate::TableRef,
            alias: Option<&'static str>,
        }

        impl $struct_name {
            pub fn new() -> Self {
                $struct_name {
                    col_table: $crate::table($table_name),
                    alias: None,
                }
            }

            pub fn table_name(&self) -> &'static str {
                $table_name
            }

            /// Returns a `TableRef` suitable for FROM/JOIN clauses.
            /// When aliased, returns `table("name").as_("alias")`.
            pub fn table(&self) -> $crate::TableRef {
                match self.alias {
                    Some(alias) => $crate::table($table_name).as_(alias),
                    None => self.col_table.clone(),
                }
            }

            /// Create an aliased copy of this schema, useful for self-joins.
            ///
            /// Column accessors on the returned instance are qualified with the
            /// alias, and `table()` returns `table("original").as_("alias")` so
            /// it can be passed directly to `join` / `left_join`.
            pub fn as_(self, alias: &'static str) -> Self {
                $struct_name {
                    col_table: $crate::table(alias),
                    alias: Some(alias),
                }
            }

            $(
                pub fn $col(&self) -> $crate::Col {
                    self.col_table.col(stringify!($col))
                }
            )*

            pub fn all_columns(&self) -> Vec<$crate::Col> {
                vec![$(self.$col()),*]
            }
        }
    };
}
