use qbey::tree::SelectTree;

/// The type of index hint action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexHintType {
    /// `USE INDEX` – suggest indexes to the optimizer.
    Use,
    /// `FORCE INDEX` – force the optimizer to use specified indexes.
    Force,
    /// `IGNORE INDEX` – tell the optimizer to skip specified indexes.
    Ignore,
}

/// Optional `FOR` clause that restricts the scope of an index hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexHintScope {
    /// `FOR JOIN`
    Join,
    /// `FOR ORDER BY`
    OrderBy,
    /// `FOR GROUP BY`
    GroupBy,
}

/// A single MySQL index hint, e.g. `FORCE INDEX FOR JOIN (idx1, idx2)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHint {
    pub(crate) hint_type: IndexHintType,
    pub(crate) scope: Option<IndexHintScope>,
    pub(crate) indexes: Vec<String>,
}

impl IndexHint {
    pub(crate) fn to_sql_fragment(&self) -> String {
        let action = match self.hint_type {
            IndexHintType::Use => "USE INDEX",
            IndexHintType::Force => "FORCE INDEX",
            IndexHintType::Ignore => "IGNORE INDEX",
        };
        let scope = match &self.scope {
            None => "",
            Some(IndexHintScope::Join) => " FOR JOIN",
            Some(IndexHintScope::OrderBy) => " FOR ORDER BY",
            Some(IndexHintScope::GroupBy) => " FOR GROUP BY",
        };
        format!("{}{} ({})", action, scope, self.indexes.join(", "))
    }
}

pub(crate) fn apply_index_hints_to<V: Clone>(tree: &mut SelectTree<V>, index_hints: &[IndexHint]) {
    use qbey::tree::SelectToken;
    if index_hints.is_empty() {
        return;
    }
    // Insert hints right after the From token
    if let Some(pos) = tree
        .tokens
        .iter()
        .position(|t| matches!(t, SelectToken::From(_)))
    {
        for (i, hint) in index_hints.iter().enumerate() {
            tree.tokens
                .insert(pos + 1 + i, SelectToken::Raw(hint.to_sql_fragment()));
        }
    }
}
