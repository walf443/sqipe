pub struct MySQL;

impl sqipe::Dialect for MySQL {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqipe::sqipe;

    #[test]
    fn test_to_sql_with_mysql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql_with(&MySQL);
        assert_eq!(sql, "SELECT id, name FROM employee WHERE name = ?");
    }
}
