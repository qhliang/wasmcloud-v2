//! Identifier validation and parameterised SQL construction.
//!
//! This module is the whole security boundary of the plugin. A component never
//! supplies SQL text: it supplies the fields of a [`Statement`], and everything
//! below turns that into a statement string plus a bound-parameter list.
//!
//! Two things follow from that, and they are what make DDL unreachable:
//!
//! - Values are always bound as `?` parameters. SQLite parameterises values
//!   natively, so a value can never change the shape of a statement.
//! - Identifiers *cannot* be parameterised, so they are the one place a
//!   component could try to escape into syntax. Every identifier is matched
//!   against [`IDENTIFIER_PATTERN`] and then wrapped in double quotes. The
//!   pattern admits no quote, semicolon, space or non-ASCII character, so the
//!   wrapping has nothing left to contain.
//!
//! The module depends on nothing but `serde_json`, which keeps it testable
//! without a component, a linker, or a network.

use serde_json::Value;

/// Longest name this plugin will place in a statement.
///
/// SQLite itself has no such limit; the bound exists so that an identifier
/// cannot be used to smuggle a large payload into every statement's text.
pub const MAX_IDENTIFIER_LEN: usize = 63;

/// Largest `LIMIT` a `select` will accept.
///
/// A larger value is refused rather than clamped: silently returning fewer rows
/// than the caller asked for, with no error, is the failure mode most likely to
/// be missed.
pub const MAX_LIMIT: u32 = 1000;

/// The accepted shape of every identifier the host emits.
///
/// First character a letter or underscore; the rest letters, digits or
/// underscores, up to [`MAX_IDENTIFIER_LEN`] in total.
pub const IDENTIFIER_PATTERN: &str = "^[A-Za-z_][A-Za-z0-9_]{0,62}$";

/// Why a request could not be turned into a statement.
///
/// Split by cause because the two travel differently: an [`Self::Identifier`]
/// means the component named something wrongly, and is recoverable by the
/// caller, while [`Self::Request`] means the request was structurally unusable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    /// A table, column or alias name did not match [`IDENTIFIER_PATTERN`].
    Identifier(String),
    /// The request was well-formed but semantically unusable.
    Request(String),
}

impl BuildError {
    /// Render the error the way a component should read it.
    ///
    /// Identifier failures name the pattern so a caller can fix the name without
    /// having to guess what the host accepts.
    pub fn message(&self) -> String {
        match self {
            Self::Identifier(detail) => {
                format!("invalid identifier: {detail} (must match {IDENTIFIER_PATTERN})")
            }
            Self::Request(detail) => detail.clone(),
        }
    }
}

/// One component-supplied name, in the role it plays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ident {
    /// A table name: the target of an operation, or the far side of a join.
    Table(String),
    /// A column name, wherever one appears.
    Column(String),
    /// A projection or result alias.
    Alias(String),
}

/// Quote and validate one identifier.
///
/// `role` only shapes the error text; the rule is identical for every position.
fn quote(id: &Ident) -> Result<String, BuildError> {
    let (raw, role) = match id {
        Ident::Table(name) => (name, "table"),
        Ident::Column(name) => (name, "column"),
        Ident::Alias(name) => (name, "alias"),
    };

    if raw.is_empty() {
        return Err(BuildError::Identifier(format!("{role} name is empty")));
    }
    if raw.chars().count() > MAX_IDENTIFIER_LEN {
        return Err(BuildError::Identifier(format!(
            "{role} name is longer than {MAX_IDENTIFIER_LEN} characters: {}",
            raw.chars().count()
        )));
    }
    if !is_valid_identifier(raw) {
        return Err(BuildError::Identifier(format!(
            "{role} name {raw:?} is not a bare identifier"
        )));
    }
    Ok(format!("\"{raw}\""))
}

/// Whether `raw` is entirely made of accepted identifier characters.
///
/// Written out as an explicit character walk rather than a regex so that the
/// check cannot be reinterpreted by a dependency upgrade, and so that a
/// non-ASCII name is rejected by the same rule as a space.
fn is_valid_identifier(raw: &str) -> bool {
    let mut chars = raw.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A comparison, resolved to SQL text plus the values it binds.
#[derive(Debug)]
struct Condition {
    sql: String,
    params: Vec<Value>,
}

/// A projection item, resolved to SQL text.
struct Projection {
    sql: String,
}

/// The six pieces of a `select`.
pub struct SelectSpec {
    pub table: String,
    pub joins: Vec<Join>,
    pub projection: Vec<SelectItem>,
    pub conditions: Vec<ConditionOp>,
    pub group_by: Vec<String>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// A join, in the shape a component supplies it.
pub struct Join {
    pub kind: JoinKind,
    pub table: String,
    pub on: Vec<ConditionOp>,
}

/// A projection item, in the shape a component supplies it.
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

/// A projection expression, in the shape a component supplies it.
pub enum Expr {
    /// `None` leaves the column unqualified.
    Column { table: Option<String>, name: String },
    /// `COUNT(*)`.
    CountAll,
    /// An aggregate over one column.
    Aggregate {
        func: AggregateFn,
        table: Option<String>,
        name: String,
    },
}

/// An aggregate function.
#[derive(Clone, Copy)]
pub enum AggregateFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFn {
    fn sql(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

/// The kind of join.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

impl JoinKind {
    fn sql(self) -> &'static str {
        match self {
            Self::Inner => "INNER JOIN",
            Self::Left => "LEFT JOIN",
        }
    }
}

/// A comparison, in the shape a component supplies it.
pub struct ConditionOp {
    pub column: String,
    pub op: ComparisonOp,
    /// Ignored for [`ComparisonOp::IsNull`] and [`ComparisonOp::NotNull`], and
    /// required for every other operator.
    pub value: Option<Value>,
}

/// A comparison operator.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    /// Takes no value; renders as `IS NULL`.
    IsNull,
    /// Takes no value; renders as `IS NOT NULL`.
    NotNull,
}

impl ComparisonOp {
    /// The SQL for the operator when it binds a value.
    fn sql(self) -> Option<&'static str> {
        match self {
            Self::Eq => Some("="),
            Self::Ne => Some("!="),
            Self::Lt => Some("<"),
            Self::Le => Some("<="),
            Self::Gt => Some(">"),
            Self::Ge => Some(">="),
            Self::Like => Some("LIKE"),
            Self::IsNull | Self::NotNull => None,
        }
    }

    /// The operator's spelling in the WIT enum.
    ///
    /// Errors quote this rather than a `Debug` rendering so the component can
    /// match the case it actually wrote.
    fn name(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Lt => "lt",
            Self::Le => "le",
            Self::Gt => "gt",
            Self::Ge => "ge",
            Self::Like => "like",
            Self::IsNull => "is-null",
            Self::NotNull => "not-null",
        }
    }
}

/// A sort term, in the shape a component supplies it.
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

/// A column and the value to write.
pub struct Field {
    pub name: String,
    pub value: Value,
}

/// How an `upsert` should handle a conflict.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum UpsertMode {
    Update,
    Ignore,
}

/// An `upsert` request.
pub struct UpsertSpec {
    pub mode: UpsertMode,
    pub conflict_columns: Vec<String>,
    /// Empty means "every column in `fields`". Must be empty for
    /// [`UpsertMode::Ignore`].
    pub update_columns: Vec<String>,
}

/// A finished statement: SQL text plus the values to bind, in order.
#[derive(Debug)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    fn new(sql: String, params: Vec<Value>) -> Self {
        Self { sql, params }
    }
}

/// Quote an optional table qualifier in front of a column.
fn qualified(table: Option<&str>, column: &Ident) -> Result<String, BuildError> {
    match table {
        Some(t) => {
            let t = quote(&Ident::Table(t.to_string()))?;
            let c = quote(column)?;
            Ok(format!("{t}.{c}"))
        }
        None => quote(column),
    }
}

/// Resolve one projection item to SQL.
fn build_projection(item: &SelectItem) -> Result<Projection, BuildError> {
    let expr = match &item.expr {
        Expr::Column { table, name } => qualified(table.as_deref(), &Ident::Column(name.clone()))?,
        Expr::CountAll => "COUNT(*)".to_string(),
        Expr::Aggregate { func, table, name } => {
            let inner = qualified(table.as_deref(), &Ident::Column(name.clone()))?;
            format!("{}({inner})", func.sql())
        }
    };

    let sql = match &item.alias {
        Some(alias) => {
            let alias = quote(&Ident::Alias(alias.clone()))?;
            format!("{expr} AS {alias}")
        }
        None => expr,
    };
    Ok(Projection { sql })
}

/// Resolve one condition to SQL plus the values it binds.
fn build_condition(cond: &ConditionOp) -> Result<Condition, BuildError> {
    let column = quote(&Ident::Column(cond.column.clone()))?;

    let Some(operator) = cond.op.sql() else {
        // IS NULL / IS NOT NULL bind nothing, so a supplied value is a mistake
        // worth reporting rather than silently dropping.
        if cond.value.is_some() {
            return Err(BuildError::Request(format!(
                "operator '{}' takes no value, but one was supplied for column {}",
                cond.op.name(),
                cond.column
            )));
        }
        let keyword = if cond.op == ComparisonOp::IsNull {
            "IS NULL"
        } else {
            "IS NOT NULL"
        };
        return Ok(Condition {
            sql: format!("{column} {keyword}"),
            params: Vec::new(),
        });
    };

    let Some(value) = &cond.value else {
        return Err(BuildError::Request(format!(
            "operator '{}' needs a value for column {}, but none was supplied",
            cond.op.name(),
            cond.column
        )));
    };

    Ok(Condition {
        sql: format!("{column} {operator} ?"),
        params: vec![value.clone()],
    })
}

/// Render a list of conditions as a `WHERE` body, collecting their values.
fn build_where(conditions: &[ConditionOp]) -> Result<(Option<String>, Vec<Value>), BuildError> {
    let mut params = Vec::new();
    let mut parts = Vec::with_capacity(conditions.len());
    for cond in conditions {
        let resolved = build_condition(cond)?;
        parts.push(resolved.sql);
        params.extend(resolved.params);
    }
    if parts.is_empty() {
        return Ok((None, params));
    }
    Ok((Some(parts.join(" AND ")), params))
}

/// Render a `JOIN` clause and collect any values its predicate binds.
fn build_join(join: &Join) -> Result<(String, Vec<Value>), BuildError> {
    if join.on.is_empty() {
        return Err(BuildError::Request(format!(
            "join on table {:?} needs at least one condition",
            join.table
        )));
    }
    let table = quote(&Ident::Table(join.table.clone()))?;
    let (on, params) = build_where(&join.on)?;
    // `build_where` is only ever handed a non-empty slice here.
    let on = on.unwrap_or_default();
    Ok((format!("{} {table} ON {on}", join.kind.sql()), params))
}

/// Build a `SELECT`.
pub fn build_select(spec: &SelectSpec) -> Result<Statement, BuildError> {
    if spec.projection.is_empty() {
        return Err(BuildError::Request(
            "select needs at least one projection item".to_string(),
        ));
    }
    if let Some(limit) = spec.limit
        && limit > MAX_LIMIT
    {
        return Err(BuildError::Request(format!(
            "limit {limit} exceeds the maximum of {MAX_LIMIT}"
        )));
    }

    let mut params = Vec::new();

    let mut items = Vec::with_capacity(spec.projection.len());
    for item in &spec.projection {
        items.push(build_projection(item)?.sql);
    }

    let mut joins = Vec::with_capacity(spec.joins.len());
    for join in &spec.joins {
        let (sql, join_params) = build_join(join)?;
        joins.push(sql);
        params.extend(join_params);
    }

    let (where_clause, where_params) = build_where(&spec.conditions)?;
    params.extend(where_params);

    let mut sql = format!(
        "SELECT {} FROM {}{}{}",
        items.join(", "),
        quote(&Ident::Table(spec.table.clone()))?,
        if joins.is_empty() {
            String::new()
        } else {
            format!(" {}", joins.join(" "))
        },
        if let Some(clause) = &where_clause {
            format!(" WHERE {clause}")
        } else {
            String::new()
        },
    );

    if !spec.group_by.is_empty() {
        let mut columns = Vec::with_capacity(spec.group_by.len());
        for column in &spec.group_by {
            columns.push(quote(&Ident::Column(column.clone()))?);
        }
        sql.push_str(&format!(" GROUP BY {}", columns.join(", ")));
    }

    if !spec.order_by.is_empty() {
        let mut terms = Vec::with_capacity(spec.order_by.len());
        for term in &spec.order_by {
            let column = quote(&Ident::Column(term.column.clone()))?;
            let direction = if term.descending { "DESC" } else { "ASC" };
            terms.push(format!("{column} {direction}"));
        }
        sql.push_str(&format!(" ORDER BY {}", terms.join(", ")));
    }

    // SQLite rejects OFFSET without LIMIT, so a bare offset becomes LIMIT -1,
    // which means "no limit" to the engine.
    match (spec.limit, spec.offset) {
        (None, None) => {}
        (Some(limit), None) | (Some(limit), Some(_)) => {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        (None, Some(_)) => {
            sql.push_str(" LIMIT -1");
        }
    }
    if let Some(offset) = spec.offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }

    Ok(Statement::new(sql, params))
}

/// Build the statement behind `count`.
pub fn build_count(
    table: &str,
    joins: Vec<Join>,
    conditions: Vec<ConditionOp>,
) -> Result<Statement, BuildError> {
    build_select(&SelectSpec {
        table: table.to_string(),
        joins,
        projection: vec![SelectItem {
            expr: Expr::CountAll,
            alias: None,
        }],
        conditions,
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    })
}

/// Validate a field list, returning its quoted columns and its bare names.
///
/// The names come back too because `upsert` has to compare them against the
/// update and conflict targets, and those comparisons are on bare names.
fn build_column_list(fields: &[Field]) -> Result<(Vec<String>, Vec<String>), BuildError> {
    if fields.is_empty() {
        return Err(BuildError::Request(
            "insert needs at least one field".to_string(),
        ));
    }
    let mut columns = Vec::with_capacity(fields.len());
    let mut names: Vec<String> = Vec::with_capacity(fields.len());
    for field in fields {
        if names.contains(&field.name) {
            return Err(BuildError::Request(format!(
                "column {:?} is given more than once",
                field.name
            )));
        }
        columns.push(quote(&Ident::Column(field.name.clone()))?);
        names.push(field.name.clone());
    }
    Ok((columns, names))
}

/// Build an `INSERT`.
pub fn build_insert(table: &str, fields: &[Field]) -> Result<Statement, BuildError> {
    let (columns, _) = build_column_list(fields)?;
    let placeholders = vec!["?"; fields.len()].join(", ");
    let params = fields.iter().map(|f| f.value.clone()).collect();
    Ok(Statement::new(
        format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            quote(&Ident::Table(table.to_string()))?,
            columns.join(", ")
        ),
        params,
    ))
}

/// Build an `INSERT` with `ON CONFLICT` handling.
pub fn build_upsert(
    table: &str,
    fields: &[Field],
    spec: &UpsertSpec,
) -> Result<Statement, BuildError> {
    let (columns, names) = build_column_list(fields)?;

    if spec.conflict_columns.is_empty() {
        return Err(BuildError::Request(
            "upsert needs at least one conflict column naming the uniqueness constraint"
                .to_string(),
        ));
    }
    let mut conflict = Vec::with_capacity(spec.conflict_columns.len());
    for column in &spec.conflict_columns {
        conflict.push(quote(&Ident::Column(column.clone()))?);
    }

    let suffix = match spec.mode {
        UpsertMode::Ignore => {
            if !spec.update_columns.is_empty() {
                return Err(BuildError::Request(
                    "upsert mode 'ignore' cannot carry update columns".to_string(),
                ));
            }
            "DO NOTHING".to_string()
        }
        UpsertMode::Update => {
            // An empty update-columns means "overwrite everything we are
            // inserting", which is the common case and keeps the caller from
            // restating the field list.
            let targets: Vec<&str> = if spec.update_columns.is_empty() {
                names.iter().map(String::as_str).collect()
            } else {
                for column in &spec.update_columns {
                    if !names.iter().any(|name| name == column) {
                        return Err(BuildError::Request(format!(
                            "update column {column:?} is not among the inserted fields"
                        )));
                    }
                }
                spec.update_columns.iter().map(String::as_str).collect()
            };

            let mut assignments = Vec::with_capacity(targets.len());
            for target in targets {
                assignments.push(format!(
                    "{} = excluded.{}",
                    quote(&Ident::Column(target.to_string()))?,
                    quote(&Ident::Column(target.to_string()))?
                ));
            }
            format!("DO UPDATE SET {}", assignments.join(", "))
        }
    };

    let placeholders = vec!["?"; fields.len()].join(", ");
    let params = fields.iter().map(|f| f.value.clone()).collect();
    Ok(Statement::new(
        format!(
            "INSERT INTO {} ({}) VALUES ({placeholders}) ON CONFLICT ({}) {suffix}",
            quote(&Ident::Table(table.to_string()))?,
            columns.join(", "),
            conflict.join(", ")
        ),
        params,
    ))
}

/// Build an `UPDATE`, refusing to run without a predicate.
pub fn build_update(
    table: &str,
    fields: &[Field],
    conditions: &[ConditionOp],
) -> Result<Statement, BuildError> {
    let (columns, _) = build_column_list(fields)?;
    if conditions.is_empty() {
        return Err(BuildError::Request(
            "update needs at least one condition; an empty list would rewrite every row"
                .to_string(),
        ));
    }

    let assignments: Vec<String> = columns.iter().map(|c| format!("{c} = ?")).collect();

    // SET placeholders come first in the statement, so their values must bind
    // first too.
    let mut params: Vec<Value> = fields.iter().map(|f| f.value.clone()).collect();
    let (where_clause, where_params) = build_where(conditions)?;
    params.extend(where_params);

    Ok(Statement::new(
        format!(
            "UPDATE {} SET {} WHERE {}",
            quote(&Ident::Table(table.to_string()))?,
            assignments.join(", "),
            where_clause.unwrap_or_default()
        ),
        params,
    ))
}

/// Build a `DELETE`, refusing to run without a predicate.
pub fn build_delete(table: &str, conditions: &[ConditionOp]) -> Result<Statement, BuildError> {
    if conditions.is_empty() {
        return Err(BuildError::Request(
            "delete needs at least one condition; an empty list would remove every row".to_string(),
        ));
    }
    let (where_clause, params) = build_where(conditions)?;
    Ok(Statement::new(
        format!(
            "DELETE FROM {} WHERE {}",
            quote(&Ident::Table(table.to_string()))?,
            where_clause.unwrap_or_default()
        ),
        params,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---------------------------------------------------------------- helpers

    fn cond(column: &str, op: ComparisonOp, value: Option<Value>) -> ConditionOp {
        ConditionOp {
            column: column.to_string(),
            op,
            value,
        }
    }

    fn eq_cond(column: &str, value: Value) -> ConditionOp {
        cond(column, ComparisonOp::Eq, Some(value))
    }

    fn col(table: Option<&str>, name: &str) -> Expr {
        Expr::Column {
            table: table.map(str::to_string),
            name: name.to_string(),
        }
    }

    fn agg(func: AggregateFn, table: Option<&str>, name: &str) -> Expr {
        Expr::Aggregate {
            func,
            table: table.map(str::to_string),
            name: name.to_string(),
        }
    }

    fn item(expr: Expr) -> SelectItem {
        SelectItem { expr, alias: None }
    }

    /// A `select` with only a table and a projection; tests overwrite the rest
    /// through [`spec`], which takes every field by value.
    fn select_of(table: &str, projection: Vec<SelectItem>) -> SelectSpec {
        SelectSpec {
            table: table.to_string(),
            joins: Vec::new(),
            projection,
            conditions: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    fn fields(names: &[(&str, Value)]) -> Vec<Field> {
        names
            .iter()
            .map(|(name, value)| Field {
                name: (*name).to_string(),
                value: value.clone(),
            })
            .collect()
    }

    fn join(kind: JoinKind, table: &str, on: Vec<ConditionOp>) -> Join {
        Join {
            kind,
            table: table.to_string(),
            on,
        }
    }

    fn upsert_spec(mode: UpsertMode, conflict: &[&str], update: &[&str]) -> UpsertSpec {
        UpsertSpec {
            mode,
            conflict_columns: conflict.iter().map(|c| (*c).to_string()).collect(),
            update_columns: update.iter().map(|c| (*c).to_string()).collect(),
        }
    }

    /// Keywords and statement forms that must never appear in generated SQL.
    ///
    /// `into` is absent because `INSERT INTO` is legitimate; the per-statement
    /// tests narrow the set where that matters.
    const STRUCTURE_KEYWORDS: [&str; 13] = [
        "create", "drop", "alter", "pragma", "attach", "detach", "vacuum", "reindex", "analyze",
        "replace", "begin", "commit", "rollback",
    ];

    fn assert_no_structure_keywords(sql: &str) {
        let lowered = sql.to_lowercase();
        for keyword in STRUCTURE_KEYWORDS {
            assert!(
                !lowered.contains(keyword),
                "generated SQL {sql:?} must not contain {keyword:?}"
            );
        }
    }

    /// One `?` per bound value, or the statement binds values to wrong slots.
    fn assert_placeholders_match(sql: &str, params: &[Value]) {
        assert_eq!(
            sql.matches('?').count(),
            params.len(),
            "placeholder count disagrees with bound values for {sql:?}"
        );
    }

    // ------------------------------------------------------- identifier rules

    #[test]
    fn accepts_ordinary_identifiers() {
        for name in [
            "users",
            "_private",
            "a",
            "A1",
            "table_2",
            "_",
            "Mixed_Case_99",
        ] {
            assert_eq!(
                quote(&Ident::Table(name.to_string())),
                Ok(format!("\"{name}\""))
            );
        }
    }

    #[test]
    fn rejects_identifiers_that_could_break_out() {
        for name in [
            // Quote breakout.
            "a\"b",
            "a\"; DROP TABLE t; --",
            "\"",
            // Statement chaining.
            "a; DROP TABLE t",
            "a;DROP TABLE t",
            // Whitespace and comments.
            "a b",
            "a\tb",
            "users--",
            "users /* x */",
            // Leading digit.
            "1abc",
            // Empty.
            "",
            // Non-ASCII.
            "用户",
            "café",
            // Quote delimiters SQLite would otherwise honour.
            "a`b",
            "a[b]",
        ] {
            assert!(
                quote(&Ident::Table(name.to_string())).is_err(),
                "{name:?} must be refused"
            );
        }
    }

    /// A name that happens to spell a SQL keyword is still a name.
    ///
    /// Quoting is what makes that safe: `"select"` refers to a table literally
    /// called `select`, not to the keyword. Refusing keyword-shaped identifiers
    /// would buy nothing and would make some real schemas unusable.
    #[test]
    fn keyword_shaped_identifiers_are_accepted_because_they_are_quoted() {
        for name in ["select", "from", "where", "join", "table", "index"] {
            assert_eq!(
                quote(&Ident::Table(name.to_string())),
                Ok(format!("\"{name}\""))
            );
        }
    }

    #[test]
    fn rejects_over_long_identifier() {
        assert!(quote(&Ident::Table("a".repeat(MAX_IDENTIFIER_LEN))).is_ok());
        assert!(quote(&Ident::Table("a".repeat(MAX_IDENTIFIER_LEN + 1))).is_err());
    }

    #[test]
    fn identifier_error_names_the_pattern() {
        let err = quote(&Ident::Column("bad name".to_string())).unwrap_err();
        assert!(matches!(err, BuildError::Identifier(_)));
        assert!(
            err.message().contains(IDENTIFIER_PATTERN),
            "error should quote the accepted pattern: {}",
            err.message()
        );
    }

    // ------------------------------------------------------------- select SQL

    #[test]
    fn select_unqualified_projection() {
        let st = build_select(&select_of("users", vec![item(col(None, "id"))])).unwrap();
        assert_eq!(st.sql, "SELECT \"id\" FROM \"users\"");
        assert!(st.params.is_empty());
    }

    #[test]
    fn select_qualified_projection_with_alias() {
        let projection = vec![SelectItem {
            expr: col(Some("u"), "name"),
            alias: Some("user_name".to_string()),
        }];
        let st = build_select(&select_of("users", projection)).unwrap();
        assert_eq!(
            st.sql,
            "SELECT \"u\".\"name\" AS \"user_name\" FROM \"users\""
        );
    }

    #[test]
    fn select_count_all() {
        let st = build_select(&select_of("users", vec![item(Expr::CountAll)])).unwrap();
        assert_eq!(st.sql, "SELECT COUNT(*) FROM \"users\"");
    }

    #[test]
    fn select_aggregates() {
        let projection = vec![
            item(agg(AggregateFn::Sum, Some("o"), "amount")),
            item(agg(AggregateFn::Avg, None, "score")),
            item(agg(AggregateFn::Min, None, "ts")),
            item(agg(AggregateFn::Max, None, "ts")),
            item(agg(AggregateFn::Count, Some("o"), "id")),
        ];
        let st = build_select(&select_of("orders", projection)).unwrap();
        assert_eq!(
            st.sql,
            "SELECT SUM(\"o\".\"amount\"), AVG(\"score\"), MIN(\"ts\"), MAX(\"ts\"), COUNT(\"o\".\"id\") FROM \"orders\""
        );
    }

    #[test]
    fn select_with_joins() {
        let mut spec = select_of("users", vec![item(col(Some("u"), "id"))]);
        spec.joins = vec![
            join(
                JoinKind::Inner,
                "orders",
                vec![eq_cond("user_id", json!(7))],
            ),
            join(JoinKind::Left, "cities", vec![eq_cond("city_id", json!(9))]),
        ];
        let st = build_select(&spec).unwrap();

        assert_eq!(
            st.sql,
            "SELECT \"u\".\"id\" FROM \"users\" \
             INNER JOIN \"orders\" ON \"user_id\" = ? \
             LEFT JOIN \"cities\" ON \"city_id\" = ?"
        );
        assert_eq!(st.params, vec![json!(7), json!(9)]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    #[test]
    fn join_without_condition_is_refused() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.joins = vec![join(JoinKind::Inner, "orders", Vec::new())];
        assert!(matches!(build_select(&spec), Err(BuildError::Request(_))));
    }

    #[test]
    fn select_group_by_and_order_by() {
        let mut spec = select_of(
            "orders",
            vec![
                item(col(Some("u"), "id")),
                item(agg(AggregateFn::Count, None, "id")),
            ],
        );
        spec.group_by = vec!["u".to_string(), "id".to_string()];
        spec.order_by = vec![
            OrderBy {
                column: "id".to_string(),
                descending: true,
            },
            OrderBy {
                column: "name".to_string(),
                descending: false,
            },
        ];
        let st = build_select(&spec).unwrap();
        assert_eq!(
            st.sql,
            "SELECT \"u\".\"id\", COUNT(\"id\") FROM \"orders\" \
             GROUP BY \"u\", \"id\" ORDER BY \"id\" DESC, \"name\" ASC"
        );
    }

    #[test]
    fn select_limit_and_offset() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);

        spec.limit = Some(10);
        assert!(build_select(&spec).unwrap().sql.ends_with(" LIMIT 10"));

        spec.offset = Some(5);
        assert!(
            build_select(&spec)
                .unwrap()
                .sql
                .ends_with(" LIMIT 10 OFFSET 5")
        );

        spec.limit = None;
        // SQLite rejects OFFSET without LIMIT; -1 is the engine's "unlimited".
        assert!(
            build_select(&spec)
                .unwrap()
                .sql
                .ends_with(" LIMIT -1 OFFSET 5")
        );
    }

    #[test]
    fn limit_above_cap_is_refused() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.limit = Some(MAX_LIMIT);
        assert!(build_select(&spec).is_ok());

        spec.limit = Some(MAX_LIMIT + 1);
        let err = build_select(&spec).unwrap_err();
        assert!(matches!(err, BuildError::Request(_)));
        assert!(err.message().contains(&MAX_LIMIT.to_string()));
    }

    #[test]
    fn select_without_projection_is_refused() {
        assert!(matches!(
            build_select(&select_of("users", Vec::new())),
            Err(BuildError::Request(_))
        ));
    }

    // ------------------------------------------------------------- conditions

    #[test]
    fn comparison_operators() {
        let cases = [
            (ComparisonOp::Eq, "="),
            (ComparisonOp::Ne, "!="),
            (ComparisonOp::Lt, "<"),
            (ComparisonOp::Le, "<="),
            (ComparisonOp::Gt, ">"),
            (ComparisonOp::Ge, ">="),
            (ComparisonOp::Like, "LIKE"),
        ];
        for (op, sql) in cases {
            let mut spec = select_of("users", vec![item(col(None, "id"))]);
            spec.conditions = vec![cond("age", op, Some(json!(30)))];
            let st = build_select(&spec).unwrap();
            assert_eq!(
                st.sql,
                format!("SELECT \"id\" FROM \"users\" WHERE \"age\" {sql} ?")
            );
            assert_eq!(st.params, vec![json!(30)]);
        }
    }

    #[test]
    fn null_checks_bind_nothing() {
        for (op, keyword) in [
            (ComparisonOp::IsNull, "IS NULL"),
            (ComparisonOp::NotNull, "IS NOT NULL"),
        ] {
            let mut spec = select_of("users", vec![item(col(None, "id"))]);
            spec.conditions = vec![cond("deleted_at", op, None)];
            let st = build_select(&spec).unwrap();
            assert_eq!(
                st.sql,
                format!("SELECT \"id\" FROM \"users\" WHERE \"deleted_at\" {keyword}")
            );
            assert!(st.params.is_empty());
            assert_placeholders_match(&st.sql, &st.params);
        }
    }

    #[test]
    fn null_check_with_value_is_refused() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.conditions = vec![cond("deleted_at", ComparisonOp::IsNull, Some(json!(1)))];
        assert!(matches!(build_select(&spec), Err(BuildError::Request(_))));
    }

    #[test]
    fn value_less_operator_is_refused() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.conditions = vec![cond("age", ComparisonOp::Eq, None)];
        assert!(matches!(build_select(&spec), Err(BuildError::Request(_))));
    }

    #[test]
    fn conditions_join_with_and_in_order() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.conditions = vec![
            eq_cond("a", json!(1)),
            cond("b", ComparisonOp::IsNull, None),
            eq_cond("c", json!("x")),
        ];
        let st = build_select(&spec).unwrap();
        assert_eq!(
            st.sql,
            "SELECT \"id\" FROM \"users\" WHERE \"a\" = ? AND \"b\" IS NULL AND \"c\" = ?"
        );
        assert_eq!(st.params, vec![json!(1), json!("x")]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    #[test]
    fn condition_column_is_validated() {
        let mut spec = select_of("users", vec![item(col(None, "id"))]);
        spec.conditions = vec![eq_cond("a OR 1=1", json!(1))];
        assert!(matches!(
            build_select(&spec),
            Err(BuildError::Identifier(_))
        ));
    }

    #[test]
    fn operator_errors_use_the_wit_spelling() {
        let err = build_condition(&cond("a", ComparisonOp::IsNull, Some(json!(1)))).unwrap_err();
        assert!(
            err.message().contains("is-null"),
            "error should name the WIT case: {}",
            err.message()
        );
    }

    // ------------------------------------------------------------------ count

    #[test]
    fn count_with_join_and_filter() {
        let st = build_count(
            "users",
            vec![join(
                JoinKind::Inner,
                "orders",
                vec![eq_cond("id", json!(1))],
            )],
            vec![eq_cond("active", json!(true))],
        )
        .unwrap();
        assert_eq!(
            st.sql,
            "SELECT COUNT(*) FROM \"users\" INNER JOIN \"orders\" ON \"id\" = ? WHERE \"active\" = ?"
        );
        assert_eq!(st.params, vec![json!(1), json!(true)]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    // ----------------------------------------------------------------- insert

    #[test]
    fn insert_statement() {
        let st = build_insert("users", &fields(&[("id", json!(1)), ("name", json!("a"))])).unwrap();
        assert_eq!(
            st.sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (?, ?)"
        );
        assert_eq!(st.params, vec![json!(1), json!("a")]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    #[test]
    fn insert_without_fields_is_refused() {
        assert!(matches!(
            build_insert("users", &[]),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn insert_with_duplicate_column_is_refused() {
        assert!(matches!(
            build_insert("users", &fields(&[("id", json!(1)), ("id", json!(2))])),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn insert_rejects_injection_in_table_and_column() {
        assert!(matches!(
            build_insert("users; DROP TABLE t", &fields(&[("id", json!(1))])),
            Err(BuildError::Identifier(_))
        ));
        assert!(matches!(
            build_insert("users", &fields(&[("id) VALUES (1); --", json!(1))])),
            Err(BuildError::Identifier(_))
        ));
    }

    // ----------------------------------------------------------------- upsert

    #[test]
    fn upsert_update_uses_excluded_prefix() {
        let f = fields(&[("id", json!(1)), ("name", json!("a"))]);
        let st = build_upsert("users", &f, &upsert_spec(UpsertMode::Update, &["id"], &[])).unwrap();
        assert_eq!(
            st.sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (?, ?) \
             ON CONFLICT (\"id\") DO UPDATE SET \"id\" = excluded.\"id\", \"name\" = excluded.\"name\""
        );
        assert_eq!(st.params, vec![json!(1), json!("a")]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    #[test]
    fn upsert_update_with_explicit_subset() {
        let f = fields(&[("id", json!(1)), ("name", json!("a")), ("age", json!(2))]);
        let st = build_upsert(
            "users",
            &f,
            &upsert_spec(UpsertMode::Update, &["id"], &["name"]),
        )
        .unwrap();
        assert_eq!(
            st.sql,
            "INSERT INTO \"users\" (\"id\", \"name\", \"age\") VALUES (?, ?, ?) \
             ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\""
        );
        assert_eq!(st.params.len(), 3, "still binds every inserted value");
    }

    #[test]
    fn upsert_update_may_rewrite_a_conflict_column() {
        // Legal no-op in SQLite; permitted so a composite key can roll forward.
        let f = fields(&[("id", json!(1))]);
        let st = build_upsert(
            "users",
            &f,
            &upsert_spec(UpsertMode::Update, &["id"], &["id"]),
        )
        .unwrap();
        assert!(st.sql.contains("DO UPDATE SET \"id\" = excluded.\"id\""));
    }

    #[test]
    fn upsert_ignore() {
        let st = build_upsert(
            "users",
            &fields(&[("id", json!(1))]),
            &upsert_spec(UpsertMode::Ignore, &["id"], &[]),
        )
        .unwrap();
        assert_eq!(
            st.sql,
            "INSERT INTO \"users\" (\"id\") VALUES (?) ON CONFLICT (\"id\") DO NOTHING"
        );
    }

    #[test]
    fn upsert_ignore_with_update_columns_is_refused() {
        assert!(matches!(
            build_upsert(
                "users",
                &fields(&[("id", json!(1))]),
                &upsert_spec(UpsertMode::Ignore, &["id"], &["name"]),
            ),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn upsert_without_conflict_columns_is_refused() {
        assert!(matches!(
            build_upsert(
                "users",
                &fields(&[("id", json!(1))]),
                &upsert_spec(UpsertMode::Update, &[], &[]),
            ),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn upsert_update_column_outside_fields_is_refused() {
        assert!(matches!(
            build_upsert(
                "users",
                &fields(&[("id", json!(1))]),
                &upsert_spec(UpsertMode::Update, &["id"], &["missing"]),
            ),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn upsert_composite_conflict_target() {
        let st = build_upsert(
            "users",
            &fields(&[("a", json!(1)), ("b", json!(2))]),
            &upsert_spec(UpsertMode::Update, &["a", "b"], &[]),
        )
        .unwrap();
        assert!(st.sql.contains("ON CONFLICT (\"a\", \"b\")"));
    }

    #[test]
    fn upsert_validates_conflict_columns() {
        assert!(matches!(
            build_upsert(
                "users",
                &fields(&[("id", json!(1))]),
                &upsert_spec(UpsertMode::Ignore, &["id) VALUES (1); --"], &[]),
            ),
            Err(BuildError::Identifier(_))
        ));
    }

    // ----------------------------------------------------------------- update

    #[test]
    fn update_statement() {
        let st = build_update(
            "users",
            &fields(&[("name", json!("b"))]),
            &[eq_cond("id", json!(1))],
        )
        .unwrap();
        assert_eq!(st.sql, "UPDATE \"users\" SET \"name\" = ? WHERE \"id\" = ?");
        assert_eq!(st.params, vec![json!("b"), json!(1)]);
        assert_placeholders_match(&st.sql, &st.params);
    }

    #[test]
    fn update_without_conditions_is_refused() {
        assert!(matches!(
            build_update("users", &fields(&[("name", json!("b"))]), &[]),
            Err(BuildError::Request(_))
        ));
    }

    #[test]
    fn update_without_fields_is_refused() {
        assert!(matches!(
            build_update("users", &[], &[eq_cond("id", json!(1))]),
            Err(BuildError::Request(_))
        ));
    }

    // ----------------------------------------------------------------- delete

    #[test]
    fn delete_statement() {
        let st = build_delete("users", &[eq_cond("id", json!(1))]).unwrap();
        assert_eq!(st.sql, "DELETE FROM \"users\" WHERE \"id\" = ?");
        assert_eq!(st.params, vec![json!(1)]);
    }

    #[test]
    fn delete_without_conditions_is_refused() {
        assert!(matches!(
            build_delete("users", &[]),
            Err(BuildError::Request(_))
        ));
    }

    // ------------------------------------------------ the load-bearing test

    /// DDL is unreachable by construction, and these assertions are what hold
    /// that property in place. A refactor that starts splicing text in from
    /// elsewhere, or that loosens the identifier rule, fails here.
    #[test]
    fn no_operation_can_reach_a_schema_statement() {
        let mut spec = SelectSpec {
            table: "users".to_string(),
            joins: vec![join(
                JoinKind::Left,
                "orders",
                vec![eq_cond("user_id", json!(1))],
            )],
            projection: vec![item(col(None, "id")), item(Expr::CountAll)],
            conditions: vec![eq_cond("active", json!(true))],
            group_by: vec!["id".to_string()],
            order_by: vec![OrderBy {
                column: "id".to_string(),
                descending: true,
            }],
            limit: Some(10),
            offset: Some(0),
        };
        let select = build_select(&spec).unwrap();
        let count = build_count("users", Vec::new(), Vec::new()).unwrap();
        let delete = build_delete("users", &[eq_cond("id", json!(1))]).unwrap();

        // A read never carries INTO, which would create a table.
        for st in [&select, &count, &delete] {
            assert_no_structure_keywords(&st.sql);
            assert!(
                !st.sql.to_lowercase().contains("into"),
                "read statement must not contain INTO: {:?}",
                st.sql
            );
        }

        spec.joins = Vec::new();
        spec.group_by = Vec::new();
        let update = build_update(
            "users",
            &fields(&[("n", json!(1))]),
            &[eq_cond("id", json!(1))],
        )
        .unwrap();
        assert_no_structure_keywords(&update.sql);
        assert!(!update.sql.to_lowercase().contains("into"));

        let f = fields(&[("id", json!(1))]);
        let insert = build_insert("users", &f).unwrap();
        assert_no_structure_keywords(&insert.sql);
        // Only INSERT INTO is legitimate here.
        assert_eq!(insert.sql.matches("INTO").count(), 1);

        let upsert =
            build_upsert("users", &f, &upsert_spec(UpsertMode::Update, &["id"], &[])).unwrap();
        assert_no_structure_keywords(&upsert.sql);
        assert_eq!(upsert.sql.matches("INTO").count(), 1);
        assert!(upsert.sql.contains("ON CONFLICT"));
        // INSERT OR REPLACE deletes and reinserts, which cascades on foreign
        // keys and resets unlisted columns; the interface never emits it.
        assert!(!upsert.sql.to_lowercase().contains("or replace"));

        // Every statement starts with one of the four verbs this plugin offers.
        for st in [&select, &count, &delete, &update, &insert, &upsert] {
            let head = st.sql.split_whitespace().next().unwrap_or_default();
            assert!(
                ["SELECT", "INSERT", "UPDATE", "DELETE"].contains(&head),
                "unexpected leading verb {head:?} in {:?}",
                st.sql
            );
        }
    }

    #[test]
    fn every_statement_binds_one_value_per_placeholder() {
        let spec = SelectSpec {
            table: "users".to_string(),
            joins: vec![join(
                JoinKind::Inner,
                "orders",
                vec![eq_cond("a", json!(1)), eq_cond("b", json!(2))],
            )],
            projection: vec![item(col(None, "id"))],
            conditions: vec![
                eq_cond("c", json!(3)),
                cond("d", ComparisonOp::IsNull, None),
                cond("e", ComparisonOp::NotNull, None),
            ],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let st = build_select(&spec).unwrap();
        assert_placeholders_match(&st.sql, &st.params);
        // Two from the join, one from the equality; the two null checks bind
        // nothing.
        assert_eq!(st.params.len(), 3);
    }

    /// A value is data, never syntax, whatever it contains.
    #[test]
    fn hostile_values_stay_in_the_parameter_list() {
        let hostile = json!("'; DROP TABLE users; --");
        let st = build_update(
            "users",
            &[Field {
                name: "name".to_string(),
                value: hostile.clone(),
            }],
            &[eq_cond("name", hostile.clone())],
        )
        .unwrap();
        assert_eq!(
            st.sql,
            "UPDATE \"users\" SET \"name\" = ? WHERE \"name\" = ?"
        );
        assert_eq!(st.params, vec![hostile.clone(), hostile]);
        assert_no_structure_keywords(&st.sql);
    }
}
