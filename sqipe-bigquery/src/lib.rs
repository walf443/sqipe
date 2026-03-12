pub struct BigQuery;

impl sqipe::Dialect for BigQuery {
    fn placeholder(&self, index: usize) -> String {
        format!("@p{}", index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqipe::sqipe;

    #[test]
    fn test_to_pipe_sql_with_bigquery() {
        let mut q = sqipe("employee");
        q.and_where(("name", "Alice"));
        q.select(&["id", "name"]);

        let (sql, _) = q.to_pipe_sql_with(&BigQuery);
        assert_eq!(sql, "FROM employee |> WHERE name = @p1 |> SELECT id, name");
    }
}
