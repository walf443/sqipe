use crate::Dialect;
use crate::query::CteDefinition;
use crate::value::Value;
use crate::where_clause::{IntoWhereClause, WhereEntry};

use crate::renderer::RenderConfig;

/// Trait for DELETE query builder methods.
///
/// Implement this trait on dialect-specific DELETE wrappers to ensure they
/// expose the same builder API as the core [`DeleteQuery`].
/// When a new builder method is added here, all implementations must follow.
pub trait DeleteQueryBuilder<V: Clone> {
    /// Add an AND WHERE condition.
    fn and_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Add an OR WHERE condition.
    fn or_where(&mut self, cond: impl IntoWhereClause<V>) -> &mut Self;
    /// Explicitly allow this DELETE to have no WHERE clause.
    ///
    /// By default, `to_sql()` panics if no WHERE conditions are set,
    /// to prevent accidental full-table deletes.
    fn allow_without_where(&mut self) -> &mut Self;

    /// Add a CTE to the `WITH` clause.
    ///
    /// ```
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut cte_q = qbey("users");
    /// cte_q.select(&["id"]);
    /// cte_q.and_where(col("age").gt(30));
    ///
    /// let mut d = qbey("users").into_delete();
    /// d.with_cte("old_users", &[], cte_q);
    /// d.and_where(col("id").eq(1));
    /// let (sql, _) = d.to_sql();
    /// assert!(sql.starts_with(r#"WITH "old_users" AS"#));
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
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder, SelectQueryBuilder};
    ///
    /// let mut base = qbey("categories");
    /// base.select(&["id"]);
    /// base.and_where(col("parent_id").eq(1));
    ///
    /// let mut recursive = qbey("categories");
    /// recursive.select(&["id"]);
    ///
    /// let cte_query = base.union_all(&recursive);
    ///
    /// let mut d = qbey("items").into_delete();
    /// d.with_recursive_cte("cat_tree", &["id"], cte_query);
    /// d.and_where(col("category_id").eq(1));
    /// let (sql, _) = d.to_sql();
    /// assert!(sql.starts_with(r#"WITH RECURSIVE "cat_tree""#));
    /// ```
    fn with_recursive_cte(
        &mut self,
        name: &str,
        columns: &[&str],
        query: impl crate::query::IntoSelectTree<V>,
    ) -> &mut Self;
}

#[derive(Debug, Clone)]
pub struct DeleteQuery<V: Clone + std::fmt::Debug = Value> {
    pub(crate) table: String,
    pub(crate) table_alias: Option<String>,
    pub(crate) wheres: Vec<WhereEntry<V>>,
    pub(crate) allow_without_where: bool,
    pub(crate) ctes: Vec<CteDefinition<V>>,
    /// Columns to return via RETURNING clause (non-standard SQL).
    #[cfg(feature = "returning")]
    pub(crate) returning_columns: Vec<crate::Col>,
}

impl<V: Clone + std::fmt::Debug> DeleteQueryBuilder<V> for DeleteQuery<V> {
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

impl<V: Clone + std::fmt::Debug> DeleteQuery<V> {
    pub(crate) fn new(
        table: String,
        table_alias: Option<String>,
        wheres: Vec<WhereEntry<V>>,
        ctes: Vec<CteDefinition<V>>,
    ) -> Self {
        DeleteQuery {
            table,
            table_alias,
            wheres,
            allow_without_where: false,
            ctes,
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
    /// use qbey::{qbey, col, ConditionExpr, DeleteQueryBuilder};
    ///
    /// let mut d = qbey("employee").into_delete();
    /// d.and_where(col("id").eq(1));
    /// d.returning(&[col("id"), col("name")]);
    /// let (sql, _) = d.to_sql();
    /// assert_eq!(sql, r#"DELETE FROM "employee" WHERE "id" = ? RETURNING "id", "name""#);
    /// ```
    #[cfg(feature = "returning")]
    pub fn returning(&mut self, cols: &[crate::Col]) -> &mut Self {
        for col in cols {
            self.returning_columns.push(col.clone());
        }
        self
    }

    /// Build a DeleteTree AST from this query.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_tree(&self) -> crate::tree::DeleteTree<V> {
        self.assert_where_present();
        let mut tokens = Vec::new();
        if !self.ctes.is_empty() {
            tokens.push(crate::tree::DeleteToken::With(
                self.ctes.iter().map(|cte| cte.to_entry()).collect(),
            ));
        }
        tokens.push(crate::tree::DeleteToken::DeleteFrom {
            table: self.table.clone(),
            alias: self.table_alias.clone(),
        });
        if !self.wheres.is_empty() {
            tokens.push(crate::tree::DeleteToken::Where(self.wheres.clone()));
        }
        #[cfg(feature = "returning")]
        if !self.returning_columns.is_empty() {
            tokens.push(crate::tree::DeleteToken::Returning(
                self.returning_columns.clone(),
            ));
        }
        crate::tree::DeleteTree { tokens }
    }

    fn assert_where_present(&self) {
        assert!(
            self.allow_without_where || !self.wheres.is_empty(),
            "DELETE without WHERE is dangerous and not allowed by default. \
             Use .allow_without_where() to explicitly allow full-table deletes."
        );
    }

    /// Build standard SQL with `?` placeholders and double-quote identifiers.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql(&self) -> (String, Vec<V>) {
        self.to_sql_with(&crate::DefaultDialect)
    }

    /// Build standard SQL with dialect-specific placeholders and quoting.
    ///
    /// # Panics
    ///
    /// Panics if no WHERE conditions are set and [`allow_without_where()`](DeleteQuery::allow_without_where)
    /// has not been called.
    pub fn to_sql_with(&self, dialect: &dyn Dialect) -> (String, Vec<V>) {
        let tree = self.to_tree();
        let ph = |n: usize| dialect.placeholder(n);
        let qi = |name: &str| dialect.quote_identifier(name);
        crate::renderer::delete::render_delete(
            &tree,
            &RenderConfig::from_dialect(&ph, &qi, dialect),
        )
    }
}
