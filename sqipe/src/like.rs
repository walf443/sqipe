/// A safe LIKE pattern expression.
///
/// Wildcards (`%`, `_`) in user input are escaped automatically, so the
/// resulting pattern matches the literal text.  Use the constructor methods
/// to add wildcards in controlled positions.
///
/// ```
/// use sqipe::LikeExpression;
///
/// assert_eq!(LikeExpression::contains("foo").to_pattern(), "%foo%");
/// assert_eq!(LikeExpression::starts_with("foo").to_pattern(), "foo%");
/// assert_eq!(LikeExpression::ends_with("foo").to_pattern(), "%foo");
/// assert_eq!(LikeExpression::contains("100%").to_pattern(), "%100\\%%");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct LikeExpression {
    pattern: String,
    escape_char: char,
}

impl LikeExpression {
    const DEFAULT_ESCAPE: char = '\\';

    /// Panics if `esc` is a LIKE wildcard (`%`, `_`) or a single quote (`'`).
    fn validate_escape_char(esc: char) {
        assert!(
            esc != '%' && esc != '_' && esc != '\'',
            "escape character must not be '%', '_', or '\\'' (got '{}')",
            esc
        );
    }

    /// Escape LIKE wildcards in user input using the given escape character.
    fn escape_with(input: &str, esc: char) -> String {
        let esc_s = esc.to_string();
        input
            .replace(&esc_s, &format!("{}{}", esc, esc))
            .replace('%', &format!("{}%", esc))
            .replace('_', &format!("{}_", esc))
    }

    /// Match rows that contain the given text anywhere.
    ///
    /// `LikeExpression::contains("foo")` → pattern `%foo%`
    pub fn contains(input: &str) -> Self {
        Self::contains_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that contain the given text anywhere, using a custom escape character.
    ///
    /// `LikeExpression::contains_escaped_by('!', "foo")` → pattern `%foo%`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn contains_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("%{}%", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Match rows that start with the given text.
    ///
    /// `LikeExpression::starts_with("foo")` → pattern `foo%`
    pub fn starts_with(input: &str) -> Self {
        Self::starts_with_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that start with the given text, using a custom escape character.
    ///
    /// `LikeExpression::starts_with_escaped_by('!', "foo")` → pattern `foo%`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn starts_with_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("{}%", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Match rows that end with the given text.
    ///
    /// `LikeExpression::ends_with("foo")` → pattern `%foo`
    pub fn ends_with(input: &str) -> Self {
        Self::ends_with_escaped_by(Self::DEFAULT_ESCAPE, input)
    }

    /// Match rows that end with the given text, using a custom escape character.
    ///
    /// `LikeExpression::ends_with_escaped_by('!', "foo")` → pattern `%foo`, escape `!`
    ///
    /// # Panics
    ///
    /// Panics if `esc` is `%`, `_`, or `'`.
    pub fn ends_with_escaped_by(esc: char, input: &str) -> Self {
        Self::validate_escape_char(esc);
        Self {
            pattern: format!("%{}", Self::escape_with(input, esc)),
            escape_char: esc,
        }
    }

    /// Return the constructed LIKE pattern string.
    pub fn to_pattern(&self) -> String {
        self.pattern.clone()
    }

    /// Return the escape character used in this expression.
    pub fn escape_char(&self) -> char {
        self.escape_char
    }
}
