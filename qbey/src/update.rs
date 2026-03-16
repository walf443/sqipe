use crate::Dialect;
use crate::column::Col;
use crate::raw_sql::RawSql;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};

use crate::renderer::RenderConfig;
use crate::tree::default_quote_identifier;

/// A single SET clause entry in an UPDATE statement.
#[derive(Debug, Clone)]
pub enum SetClause<V: Clone> {
    /// `"col" = ?` — identifier-quoted column with a bind value.
    Value(String, V),
    /// Raw SQL expression via [`RawSql`].
    Expr(RawSql),
}

/// Trait for UPDATE query builder methods.
///
/// Implement this trait on dialect-specific UPDATE wrappers to ensure they
/// expose the same builder API as the core [`UpdateQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait UpdateQueryBuilder<V: Clone> {
    /// Add a SET clause: `SET "col" = ?`.
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self;
    /// Add a raw SQL expression to the SET clause.
    fn set_expr(&mut self, expr: RawSql) -> &mut Self;
    /// Add an AND WHERE condition.
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an OR WHERE condition.
    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Explicitly allow this UPDATE to have no WHERE clause.
    fn allow_without_where(&mut self) -> &mut Self;
}

/// An UPDATE query builder, generic over the bind value type `V`.
///
/// Created via [`SelectQuery::into_update()`] to convert a SELECT query builder into an UPDATE statement.
///
/// By default, WHERE clause is required. Calling `to_sql()` or `to_sql_with()` without
/// any WHERE conditions will panic to prevent accidental full-table updates.
/// Use [`allow_without_where()`](UpdateQuery::allow_without_where) to explicitly allow WHERE-less updates.
///
/// ```
/// use qbey::{qbey, col, UpdateQueryBuilder};
///
/// let mut u = qbey("employee").into_update();
/// u.set(col("name"), "Alice");
/// u.and_where(col("id").eq(1));
/// let (sql, _) = u.to_sql();
/// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
/// ```
#[derive(Debug, Clone)]
pub struct UpdateQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) sets: Vec<SetClause<V>>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) allow_without_where: bool,
}

impl<V: Clone + std::fmt::Debug> UpdateQueryBuilder<V> for UpdateQuery<V> {
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self {
        self.sets.push(SetClause::Value(col.column, val.into()));
        self
    }

    fn set_expr(&mut self, expr: RawSql) -> &mut Self {
        self.sets.push(SetClause::Expr(expr));
        self
    }

    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::And(cond.into_where_clause()));
        self
    }

    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self {
        self.wheres.push(WhereEntry::Or(cond.into_where_clause()));
        self
    }

    fn allow_without_where(&mut self) -> &mut Self {
        self.allow_without_where = true;
        self
    }
}

impl<V: Clone + std::fmt::Debug> UpdateQuery<V> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
    ) -> Self {
        UpdateQuery {
            table,
            table_alias,
            sets: Vec::new(),
            wheres,
            allow_without_where: false,
        }
    }

    /// Build an UpdateTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_tree(&self) -> crate::tree::UpdateTree<V> {
        self.assert_where_present();
        crate::tree::UpdateTree {
            table: self.table.clone(),
            table_alias: self.table_alias.clone(),
            sets: self.sets.clone(),
            wheres: self.wheres.clone(),
            order_bys: Vec::new(),
            limit: None,
        }
    }

    fn assert_where_present(&self) {
        assert!(
            self.allow_without_where || !self.wheres.is_empty(),
            "UPDATE without WHERE is dangerous and not allowed by default. \
             Use .allow_without_where() to explicitly allow full-table updates."
        );
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let cfg = RenderConfig {
            ph: &|_| "?".to_string(),
            qi: &default_quote_identifier,
            backslash_escape: false,
        };
        crate::renderer::update::render_update(&tree, &cfg)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// Bind values are returned in SQL clause order: SET values first, then WHERE values.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](UpdateQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        crate::renderer::update::render_update(
            &tree,
            &RenderConfig::from_dialect(&ph, &qi, dialect),
        )
    }
}
