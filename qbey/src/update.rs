use crate::Dialect;
use crate::column::Col;
use crate::query::CteDefinition;
use crate::raw_sql::RawSql;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};

use crate::renderer::RenderConfig;

/// A single SET clause entry in an UPDATE statement.
#[derive(Debug, Clone)]
pub enum SetClause<V: Clone> {
    /// `"col" = ?` — identifier-quoted column with a bind value.
    Value(String, V),
    /// Raw SQL expression via [`RawSql`].
    Expr(RawSql<V>),
}

/// Trait for UPDATE query builder methods.
///
/// Implement this trait on dialect-specific UPDATE wrappers to ensure they
/// expose the same builder API as the core [`UpdateQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait UpdateQueryBuilder<V: Clone> {
    /// Add a SET clause: `SET "col" = ?`.
    ///
    /// Use [`col()`](crate::col) to create a column reference for the first argument.
    /// Column names are quoted as identifiers but **not** parameterized,
    /// so never pass external (user-supplied) input as a column name.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.set(col("name"), "Alice");
    /// u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "name" = ? WHERE "id" = ?"#);
    /// ```
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self;

    /// Add a raw SQL expression to the SET clause.
    ///
    /// Use [`RawSql::new()`] to create the expression, making it explicit
    /// that raw SQL is being injected.
    ///
    /// # Security
    ///
    /// The expression is embedded directly into the generated SQL **without
    /// escaping or parameterization**. Never pass user-supplied input;
    /// doing so opens the door to SQL injection.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, RawSql, UpdateQueryBuilder};
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.set_expr(RawSql::new(r#""visit_count" = "visit_count" + 1"#));
    /// u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert_eq!(sql, r#"UPDATE "employee" SET "visit_count" = "visit_count" + 1 WHERE "id" = ?"#);
    /// ```
    fn set_expr(&mut self, expr: RawSql<V>) -> &mut Self;

    /// Add an AND WHERE condition.
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;

    /// Add an OR WHERE condition.
    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;

    /// Explicitly allow this UPDATE to have no WHERE clause.
    ///
    /// By default, `to_sql()` panics if no WHERE conditions are set,
    /// to prevent accidental full-table updates.
    fn allow_without_where(&mut self) -> &mut Self;

    /// Add a CTE to the `WITH` clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut cte_q = qbey("departments");
    /// cte_q.select(&["id"]);
    /// cte_q.and_where(col("active").eq(true));
    ///
    /// let mut u = qbey("employee").into_update();
    /// u.with_cte("active_depts", &[], cte_q);
    /// u.set(col("status"), "active");
    /// u.and_where(col("dept_id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert!(sql.starts_with(r#"WITH "active_depts" AS"#));
    /// ```
    fn with_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;

    /// Add a recursive CTE to the `WITH RECURSIVE` clause.
    ///
    /// Note: per the SQL standard, the `RECURSIVE` keyword applies to the
    /// entire `WITH` block. If any CTE added via this method is recursive,
    /// the rendered SQL will use `WITH RECURSIVE` for all CTEs in the clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut base = qbey("employees");
    /// base.select(&["id", "name", "manager_id"]);
    /// base.and_where(col("manager_id").eq(0));
    ///
    /// let mut recursive = qbey("employees");
    /// recursive.select(&["id", "name", "manager_id"]);
    ///
    /// let cte_query = base.union_all(&recursive);
    ///
    /// let mut u = qbey("employees").into_update();
    /// u.with_recursive_cte("org_tree", &["id", "name", "manager_id"], cte_query);
    /// u.set(col("active"), true);
    /// u.and_where(col("id").eq(1));
    /// let (sql, _) = u.to_sql();
    /// assert!(sql.starts_with(r#"WITH RECURSIVE "org_tree""#));
    /// ```
    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;
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
/// use qbey::{qbey, col, ConditionExpr, UpdateQueryBuilder};
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
    pub(crate) ctes: Vec<CteDefinition<V>>,
}

impl<V: Clone + std::fmt::Debug> UpdateQueryBuilder<V> for UpdateQuery<V> {
    fn set(&mut self, col: Col, val: impl Into<V>) -> &mut Self {
        self.sets.push(SetClause::Value(col.column, val.into()));
        self
    }

    fn set_expr(&mut self, expr: RawSql<V>) -> &mut Self {
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

    fn with_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self {
        debug_assert!(
            !self.ctes.iter().any(|c| c.name == name),
            "duplicate CTE name {:?}: each CTE must have a unique name",
            name,
        );
        self.ctes
            .push(CteDefinition::new(name, columns, query, false));
        self
    }

    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self {
        debug_assert!(
            !self.ctes.iter().any(|c| c.name == name),
            "duplicate CTE name {:?}: each CTE must have a unique name",
            name,
        );
        self.ctes
            .push(CteDefinition::new(name, columns, query, true));
        self
    }
}

impl<V: Clone + std::fmt::Debug> UpdateQuery<V> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
        ctes: Vec<CteDefinition<V>>,
    ) -> Self {
        UpdateQuery {
            table,
            table_alias,
            sets: Vec::new(),
            wheres,
            allow_without_where: false,
            ctes,
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
        let mut tokens = Vec::new();
        if !self.ctes.is_empty() {
            tokens.push(crate::tree::UpdateToken::With(
                self.ctes.iter().map(|cte| cte.to_entry()).collect(),
            ));
        }
        tokens.push(crate::tree::UpdateToken::Update {
            table: self.table.clone(),
            alias: self.table_alias.clone(),
        });
        tokens.push(crate::tree::UpdateToken::Set(self.sets.clone()));
        if !self.wheres.is_empty() {
            tokens.push(crate::tree::UpdateToken::Where(self.wheres.clone()));
        }
        crate::tree::UpdateTree { tokens }
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
        self.to_sql_with(&crate::DefaultDialect)
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
