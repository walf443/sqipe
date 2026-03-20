// MariaDB does NOT support UPDATE ... RETURNING.
// RETURNING is only available for INSERT and DELETE in MariaDB.

use qbey::{ConditionExpr, UpdateQueryBuilder, col};
use qbey_mysql::qbey_with;

use super::common::MysqlValue;

#[test]
#[should_panic(expected = "RETURNING is not supported for UPDATE in MySQL/MariaDB")]
fn test_update_returning_panics() {
    let mut u = qbey_with::<MysqlValue>("users").into_update();
    u.set(col("name"), "Alice");
    let mut u = u.and_where(col("id").eq(1));
    u.returning(&[col("id"), col("name")]);
}
