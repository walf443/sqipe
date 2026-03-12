pub struct PostgreSQL;

impl sqipe::Dialect for PostgreSQL {
    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqipe::{col, sqipe};

    #[test]
    fn test_to_sql_with_postgresql() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.and_where(col("age").gt(20));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_sql_with(&PostgreSQL);
        assert_eq!(
            sql,
            "SELECT id, name FROM employee WHERE name = $1 AND age > $2"
        );
    }
}
