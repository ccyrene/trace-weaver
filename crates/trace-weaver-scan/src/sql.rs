//! Best-effort column-level lineage extraction from SQL.
//!
//! Parses a SQL transform with `sqlparser` and maps each target column of an
//! `INSERT … SELECT` (or bare `SELECT`) back to the source columns referenced
//! in its projection expression, classifying each as identity / transformation
//! / aggregation. Everything produced here is provenance
//! [`trace_weaver_core::OriginSource::InferredSql`].

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, ObjectNamePart, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use trace_weaver_core::TransformType;

/// A column reference inside SQL: an optional table qualifier and a column name.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlColRef {
    pub table: Option<String>,
    pub column: String,
}

/// One inferred target-column mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlMapping {
    pub sources: Vec<SqlColRef>,
    pub target: String,
    /// The projection expression, normalised to a short label.
    pub function: String,
    pub transform_type: TransformType,
}

/// Result of analysing one SQL statement.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SqlColumnLineage {
    /// Target table of an `INSERT INTO t …`, if present.
    pub target_table: Option<String>,
    /// Tables read in `FROM`/`JOIN` clauses.
    pub source_tables: Vec<String>,
    pub mappings: Vec<SqlMapping>,
    /// Set when the projection contained `SELECT *` / `t.*` while an explicit
    /// INSERT column list was present. Positional mapping is impossible in this
    /// case, so `mappings` is left empty and the caller should warn.
    pub wildcard_in_projection: bool,
    /// Set to `(projection_len, insert_target_len)` when those counts differ for
    /// an `INSERT … (cols) SELECT …`. Positional mapping is ambiguous, so
    /// `mappings` is left empty and the caller should warn.
    pub arity_mismatch: Option<(usize, usize)>,
}

/// Analyse one SQL string and return its column lineage. Dialect-tolerant
/// (uses a generic dialect); returns an empty result for statements it cannot
/// map rather than erroring, so a scan never fails on exotic SQL.
pub fn column_lineage_from_sql(sql: &str) -> anyhow::Result<SqlColumnLineage> {
    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(SqlColumnLineage::default()),
    };

    // Use the first statement that we can map.
    for stmt in &statements {
        match stmt {
            Statement::Insert(insert) => {
                let target_table = object_name_last(&match &insert.table {
                    TableObject::TableName(n) => n.clone(),
                    _ => continue,
                });
                let target_cols: Vec<String> =
                    insert.columns.iter().map(object_name_last).collect();
                if let Some(query) = &insert.source {
                    let mut lineage = lineage_from_query(query, &target_cols);
                    lineage.target_table = Some(target_table);
                    return Ok(lineage);
                }
            }
            Statement::Query(query) => {
                let lineage = lineage_from_query(query, &[]);
                return Ok(lineage);
            }
            _ => continue,
        }
    }
    Ok(SqlColumnLineage::default())
}

/// Column lineage for a single SELECT-item fragment, e.g. `"amount * 1.08 AS x"`
/// or `"a + b"`. Reuses the full projection machinery by wrapping the fragment as
/// `SELECT <item>`. Used by the pandas/Spark dataflow analyzer to read SQL-string
/// expressions (`selectExpr(...)`, `expr("...")`). Returns the per-target mappings
/// (usually one); empty when the fragment can't be parsed.
pub fn select_expr_lineage(item: &str) -> Vec<SqlMapping> {
    column_lineage_from_sql(&format!("SELECT {item}"))
        .map(|l| l.mappings)
        .unwrap_or_default()
}

/// Build lineage from a top-level query.
///
/// * **INSERT mode** (`target_cols` non-empty): the INSERT column list is the
///   authoritative output arity. We map each projection expression *positionally*
///   to `target_cols[i]` — but only when the projection is a clean 1:1 list. A
///   wildcard (`SELECT *`) expands to an unknown number of columns, and a
///   projection/target count mismatch makes "which column shifted" ambiguous; in
///   either case we refuse to guess (emit no mappings, set a flag) so we never
///   fabricate a phantom column or silently drop one. See findings #1/#2.
/// * **Bare-SELECT mode** (`target_cols` empty): outputs are named by alias or
///   implicit column name; wildcards are simply skipped.
fn lineage_from_query(query: &Query, target_cols: &[String]) -> SqlColumnLineage {
    let mut out = SqlColumnLineage::default();
    let select = match unwrap_select(&query.body) {
        Some(s) => s,
        None => return out,
    };

    out.source_tables = collect_source_tables(&select.from);

    let build_mapping = |expr: &Expr, target: String| -> SqlMapping {
        let mut sources = Vec::new();
        collect_col_refs(expr, &mut sources);
        let transform_type = classify_expr(expr);
        SqlMapping {
            sources,
            target,
            function: render_expr(expr),
            transform_type,
        }
    };

    if !target_cols.is_empty() {
        // ── INSERT mode: positional, with strict guards ──
        let has_wildcard = select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        });
        if has_wildcard {
            out.wildcard_in_projection = true;
            return out; // table-level only; caller warns and asks for column_map
        }
        if select.projection.len() != target_cols.len() {
            out.arity_mismatch = Some((select.projection.len(), target_cols.len()));
            return out;
        }
        for (idx, item) in select.projection.iter().enumerate() {
            let expr = match item {
                SelectItem::UnnamedExpr(e) => e,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                // Unreachable after the wildcard guard, but stay safe rather
                // than desync the positional index.
                _ => {
                    out.mappings.clear();
                    out.arity_mismatch = Some((select.projection.len(), target_cols.len()));
                    return out;
                }
            };
            out.mappings
                .push(build_mapping(expr, target_cols[idx].clone()));
        }
    } else {
        // ── Bare-SELECT mode: name by alias / implicit column name ──
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(e) => (e, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => continue, // wildcards can't be named here
            };
            let target = if let Some(a) = alias {
                a
            } else if let Some(implicit) = implicit_target_name(expr) {
                implicit
            } else {
                continue;
            };
            out.mappings.push(build_mapping(expr, target));
        }
    }
    out
}

/// Drill through parenthesised / set-operation bodies to the leading SELECT.
fn unwrap_select(body: &SetExpr) -> Option<&Select> {
    match body {
        SetExpr::Select(s) => Some(s),
        SetExpr::Query(q) => unwrap_select(&q.body),
        SetExpr::SetOperation { left, .. } => unwrap_select(left),
        _ => None,
    }
}

/// For a bare-column projection with no alias, the output column name is the
/// column itself.
fn implicit_target_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
        Expr::Nested(inner) => implicit_target_name(inner),
        _ => None,
    }
}

/// Classify the projection expression.
fn classify_expr(expr: &Expr) -> TransformType {
    if top_function_is_aggregate(expr) {
        return TransformType::Aggregation;
    }
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => TransformType::Identity,
        Expr::Nested(inner) => classify_expr(inner),
        _ => TransformType::Transformation,
    }
}

/// True if the outermost expression is an aggregate function call.
fn top_function_is_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            let name = object_name_last(&f.name).to_ascii_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
        }
        Expr::Nested(inner) => top_function_is_aggregate(inner),
        _ => false,
    }
}

/// Recursively collect every column reference in an expression.
fn collect_col_refs(expr: &Expr, out: &mut Vec<SqlColRef>) {
    let push = |table: Option<String>, column: String, out: &mut Vec<SqlColRef>| {
        let r = SqlColRef { table, column };
        if !out.contains(&r) {
            out.push(r);
        }
    };

    match expr {
        Expr::Identifier(id) => push(None, id.value.clone(), out),
        Expr::CompoundIdentifier(parts) => {
            if let Some(col) = parts.last() {
                let table = if parts.len() >= 2 {
                    Some(parts[parts.len() - 2].value.clone())
                } else {
                    None
                };
                push(table, col.value.clone(), out);
            }
        }
        Expr::Nested(e)
        | Expr::IsFalse(e)
        | Expr::IsNotFalse(e)
        | Expr::IsTrue(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::Collate { expr: e, .. } => collect_col_refs(e, out),
        Expr::Cast { expr, .. } => collect_col_refs(expr, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_col_refs(left, out);
            collect_col_refs(right, out);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_col_refs(expr, out);
            collect_col_refs(low, out);
            collect_col_refs(high, out);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_col_refs(op, out);
            }
            for cw in conditions {
                collect_col_refs(&cw.condition, out);
                collect_col_refs(&cw.result, out);
            }
            if let Some(er) = else_result {
                collect_col_refs(er, out);
            }
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let Some(e) = function_arg_expr(arg) {
                        collect_col_refs(e, out);
                    }
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_col_refs(expr, out);
            for e in list {
                collect_col_refs(e, out);
            }
        }
        // Other expression shapes (literals, subqueries, …) contribute no
        // directly-mappable source columns for our purposes.
        _ => {}
    }
}

fn function_arg_expr(arg: &FunctionArg) -> Option<&Expr> {
    let fae = match arg {
        FunctionArg::Named { arg, .. } => arg,
        FunctionArg::ExprNamed { arg, .. } => arg,
        FunctionArg::Unnamed(arg) => arg,
    };
    match fae {
        FunctionArgExpr::Expr(e) => Some(e),
        _ => None,
    }
}

/// Collect base table names from a FROM list (including JOINs and the FROM of
/// derived subqueries).
fn collect_source_tables(from: &[TableWithJoins]) -> Vec<String> {
    let mut out = Vec::new();
    for twj in from {
        collect_factor_tables(&twj.relation, &mut out);
        for join in &twj.joins {
            collect_factor_tables(&join.relation, &mut out);
        }
    }
    out
}

fn collect_factor_tables(factor: &TableFactor, out: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            let t = object_name_last(name);
            if !out.contains(&t) {
                out.push(t);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            if let Some(select) = unwrap_select(&subquery.body) {
                for t in collect_source_tables(&select.from) {
                    if !out.contains(&t) {
                        out.push(t);
                    }
                }
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            for t in collect_source_tables(std::slice::from_ref(table_with_joins)) {
                if !out.contains(&t) {
                    out.push(t);
                }
            }
        }
        _ => {}
    }
}

/// Last identifier segment of an object name.
fn object_name_last(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|part| match part {
            ObjectNamePart::Identifier(id) => id.value.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default()
}

/// Render an expression to a compact label (single-line).
fn render_expr(expr: &Expr) -> String {
    let s = expr.to_string();
    // Collapse internal whitespace/newlines so the label stays compact.
    let collapsed: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_insert_select_with_subquery_and_casts() {
        let sql = r#"
INSERT INTO bronze_sales (event_id, customer_name, amount, currency, event_ts)
SELECT raw_event_id::bigint, customer, amount::numeric(12,2), currency, event_ts::timestamp
FROM (
  SELECT *, row_number() OVER (PARTITION BY raw_event_id ORDER BY ingested_at) AS rn
  FROM landing_sales
) deduped
WHERE rn = 1;
"#;
        let lin = column_lineage_from_sql(sql).unwrap();
        assert_eq!(lin.target_table.as_deref(), Some("bronze_sales"));
        assert!(lin.source_tables.contains(&"landing_sales".to_string()));
        assert_eq!(lin.mappings.len(), 5);

        let m: std::collections::HashMap<_, _> =
            lin.mappings.iter().map(|m| (m.target.clone(), m)).collect();

        // event_id <- raw_event_id (cast => Transformation)
        let ev = m.get("event_id").unwrap();
        assert_eq!(
            ev.sources,
            vec![SqlColRef {
                table: None,
                column: "raw_event_id".into()
            }]
        );
        assert_eq!(ev.transform_type, TransformType::Transformation);

        // currency <- currency (bare => Identity)
        let cur = m.get("currency").unwrap();
        assert_eq!(cur.transform_type, TransformType::Identity);
        assert_eq!(
            cur.sources,
            vec![SqlColRef {
                table: None,
                column: "currency".into()
            }]
        );
    }

    #[test]
    fn maps_aggregate_select() {
        let sql = r#"
INSERT INTO gold_sales_daily (event_date, total_transactions, unique_customers, total_revenue_usd, avg_transaction_usd)
SELECT event_date, COUNT(*), COUNT(DISTINCT customer_name), SUM(amount_usd), ROUND(AVG(amount_usd), 2)
FROM silver_sales
WHERE is_valid
GROUP BY event_date;
"#;
        let lin = column_lineage_from_sql(sql).unwrap();
        assert_eq!(lin.mappings.len(), 5);
        let m: std::collections::HashMap<_, _> =
            lin.mappings.iter().map(|m| (m.target.clone(), m)).collect();

        assert_eq!(
            m.get("event_date").unwrap().transform_type,
            TransformType::Identity
        );
        assert_eq!(
            m.get("total_transactions").unwrap().transform_type,
            TransformType::Aggregation
        );
        assert_eq!(
            m.get("unique_customers").unwrap().transform_type,
            TransformType::Aggregation
        );
        assert_eq!(
            m.get("unique_customers").unwrap().sources,
            vec![SqlColRef {
                table: None,
                column: "customer_name".into()
            }]
        );
        assert_eq!(
            m.get("total_revenue_usd").unwrap().transform_type,
            TransformType::Aggregation
        );
    }

    #[test]
    fn fan_in_collects_multiple_sources() {
        let sql = "INSERT INTO t (c) SELECT a + b FROM s";
        let lin = column_lineage_from_sql(sql).unwrap();
        assert_eq!(lin.mappings.len(), 1);
        assert_eq!(lin.mappings[0].sources.len(), 2);
        assert_eq!(
            lin.mappings[0].transform_type,
            TransformType::Transformation
        );
    }

    #[test]
    fn unmappable_sql_returns_empty() {
        let lin = column_lineage_from_sql("CREATE TABLE x (a int)").unwrap();
        assert!(lin.mappings.is_empty());
    }

    #[test]
    fn wildcard_with_insert_columns_refuses_to_guess() {
        // Regression for finding #1: `*` mixed with explicit projection items
        // must NOT positionally shift target columns.
        let lin = column_lineage_from_sql("INSERT INTO t (a, b, c) SELECT *, x FROM s").unwrap();
        assert!(lin.wildcard_in_projection);
        assert!(lin.mappings.is_empty(), "must emit no (wrong) mappings");
        assert_eq!(lin.target_table.as_deref(), Some("t"));
    }

    #[test]
    fn arity_mismatch_refuses_to_guess() {
        // Regression for finding #2: projection longer than target list must not
        // fabricate a phantom target column named after the source.
        let lin = column_lineage_from_sql("INSERT INTO t (a, b) SELECT x, y, z FROM s").unwrap();
        assert_eq!(lin.arity_mismatch, Some((3, 2)));
        assert!(lin.mappings.is_empty());
        // ...and the underflow direction must not silently drop columns either.
        let lin2 = column_lineage_from_sql("INSERT INTO t (a, b, c) SELECT x FROM s").unwrap();
        assert_eq!(lin2.arity_mismatch, Some((1, 3)));
        assert!(lin2.mappings.is_empty());
    }

    #[test]
    fn clean_insert_still_maps() {
        let lin = column_lineage_from_sql("INSERT INTO t (a, b) SELECT x, y + z FROM s").unwrap();
        assert!(!lin.wildcard_in_projection && lin.arity_mismatch.is_none());
        assert_eq!(lin.mappings.len(), 2);
        assert_eq!(lin.mappings[0].target, "a");
        assert_eq!(lin.mappings[1].target, "b");
        assert_eq!(lin.mappings[1].sources.len(), 2);
    }
}
