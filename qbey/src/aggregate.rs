/// An aggregate expression that can be aliased with `.as_()`.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub(crate) expr: AggregateFunc,
    pub(crate) alias: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum AggregateFunc {
    CountAll,
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    Expr(crate::raw_sql::RawSql),
}

impl AggregateExpr {
    pub fn as_(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }
}

pub fn count_all() -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::CountAll,
        alias: None,
    }
}

pub fn count(col: &str) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Count(col.to_string()),
        alias: None,
    }
}

pub fn sum(col: &str) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Sum(col.to_string()),
        alias: None,
    }
}

pub fn avg(col: &str) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Avg(col.to_string()),
        alias: None,
    }
}

pub fn min(col: &str) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Min(col.to_string()),
        alias: None,
    }
}

pub fn max(col: &str) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Max(col.to_string()),
        alias: None,
    }
}

/// Raw SQL expression for dialect-specific aggregate functions.
pub fn expr(raw: crate::raw_sql::RawSql) -> AggregateExpr {
    AggregateExpr {
        expr: AggregateFunc::Expr(raw),
        alias: None,
    }
}
