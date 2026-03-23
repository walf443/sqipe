/// Value represents a bind parameter value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Int(n as i64)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl From<&[u8]> for Value {
    fn from(b: &[u8]) -> Self {
        Value::Bytes(b.to_vec())
    }
}

/// Marker trait for types that can be used as bind parameter values in conditions.
///
/// This is automatically implemented for common types (integers, strings, booleans, floats)
/// and for the built-in [`Value`] enum. If you use a custom value type with
/// [`ConditionExpr::eq`](crate::ConditionExpr::eq) and similar methods, implement this
/// trait for your type.
///
/// [`Col`](crate::Col) intentionally does **not** implement this trait so that the
/// compiler can distinguish column references from scalar values.
pub trait ConditionValue: Clone {}

impl ConditionValue for i8 {}
impl ConditionValue for i16 {}
impl ConditionValue for i32 {}
impl ConditionValue for i64 {}
impl ConditionValue for u8 {}
impl ConditionValue for u16 {}
impl ConditionValue for u32 {}
impl ConditionValue for u64 {}
impl ConditionValue for f32 {}
impl ConditionValue for f64 {}
impl ConditionValue for bool {}
impl ConditionValue for String {}
impl ConditionValue for &str {}
impl ConditionValue for Vec<u8> {}
impl ConditionValue for &[u8] {}
impl ConditionValue for Value {}

/// Comparison operator.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl Op {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Gt => ">",
            Op::Lt => "<",
            Op::Gte => ">=",
            Op::Lte => "<=",
        }
    }
}
