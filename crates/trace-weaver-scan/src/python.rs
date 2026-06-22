//! Static extraction of `@tw.task(...)` declarations from Python source.
//!
//! Uses `rustpython-parser` to build an AST and reads **only literal**
//! arguments (strings, lists, tuples) — no code is executed. The convention is
//! defined in the project README.

use std::collections::HashMap;

use rustpython_parser::ast::{self, Constant, Expr, Ranged, Stmt};
use rustpython_parser::Parse;

use crate::dataflow::{InferredColumn, OpaqueNote};

/// One column-map tuple `(sources, target, function)` from a `column_map=[...]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMapEntry {
    pub sources: Vec<String>,
    pub target: String,
    pub function: Option<String>,
}

/// A single `@tw.task(...)` declaration extracted from source.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskDecl {
    /// Decorated function name, used as the task name.
    pub task_name: String,
    pub dag: Option<String>,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
    /// Raw `engine=` string (e.g. `"sql"`, `"pandas"`).
    pub engine: Option<String>,
    pub sql: Option<String>,
    /// Markdown `description=`.
    pub description: Option<String>,
    /// Short `transform=` label, e.g. `"ENRICH"`.
    pub transform: Option<String>,
    pub column_map: Vec<ColumnMapEntry>,
    /// Bare column names from a `copy=[...]` shortcut — each declares a same-name
    /// identity ("direct copy") column. Expanded into `column_map` after parsing;
    /// an explicit `column_map` entry for the same target wins.
    pub copy: Vec<String>,
    /// 1-based source line of the decorator/def.
    pub line: Option<u32>,
    /// Names of recognised keyword arguments whose value could NOT be resolved
    /// to a string literal or module-level constant (e.g. an f-string,
    /// `.format(...)`, or string concatenation). Such values are dropped, so the
    /// scanner surfaces a `W_NON_LITERAL` diagnostic for each — see finding #6.
    pub unresolved: Vec<String>,
    /// Column lineage statically recovered from the task's function BODY
    /// (pandas/Spark dataflow), tagged inferred-from-code. Fills gaps the engineer
    /// did not declare in `column_map`.
    pub inferred_columns: Vec<InferredColumn>,
    /// Spots in the body the dataflow analyzer could not trace — surfaced as
    /// `W_OPAQUE_COLUMN` so the engineer knows exactly what to declare by hand.
    pub opaque: Vec<OpaqueNote>,
}

/// Per-file defaults from a module-level `tw.configure(...)` call and/or a
/// `with DAG(dag_id=...)` block. Used to expand bare table names into full FQNs
/// and to supply a default `dag` so tasks don't repeat them (Tier 1+2).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModuleConfig {
    pub service: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub dag: Option<String>,
}

/// Everything statically extracted from one Python module.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScannedModule {
    pub config: ModuleConfig,
    pub tasks: Vec<TaskDecl>,
}

/// Which decorator form produced a [`TaskDecl`].
#[derive(Debug, Clone, Copy, PartialEq)]
enum DecoratorKind {
    /// `@task(...)` / `@tw.task(...)`.
    Task,
    /// `@tw.sql(QUERY, ...)` — shortcut implying `engine="sql"`, SQL is arg 0.
    Sql,
}

/// Parse `source` and return the module's `tw.configure(...)` defaults plus
/// every `@tw.task(...)` / `@tw.sql(...)` declaration it contains.
///
/// Recognises decorators whose callee final segment is `task`
/// (the full form) or `sql` (the `@tw.sql(QUERY, …)` shortcut).
pub fn extract_task_decls(source: &str) -> anyhow::Result<ScannedModule> {
    let suite = ast::Suite::parse(source, "<module>")
        .map_err(|e| anyhow::anyhow!("python parse error: {e}"))?;

    // First pass: collect module-level string constants `NAME = "..."`.
    let mut consts: HashMap<String, String> = HashMap::new();
    for stmt in &suite {
        if let Stmt::Assign(assign) = stmt {
            if let (Some(name), Some(s)) = (
                single_target_name(&assign.targets),
                const_str(&assign.value),
            ) {
                consts.insert(name, s);
            }
        }
    }

    let config = extract_module_config(&suite, &consts);

    // Second pass: find decorated functions.
    let mut tasks = Vec::new();
    for stmt in &suite {
        let func = match stmt {
            Stmt::FunctionDef(f) => f,
            _ => continue,
        };
        for deco in &func.decorator_list {
            if let Some((call, kind)) = task_decorator_call(deco) {
                let mut decl = build_decl(func.name.as_str(), call, source, &consts, kind);
                // Trace the function body for column lineage (pandas/Spark). SQL
                // tasks are handled by the SQL analyzer, so skip dataflow there.
                if !matches!(kind, DecoratorKind::Sql) && decl.engine.as_deref() != Some("sql") {
                    let r = crate::dataflow::analyze(&func.body, &consts, source);
                    decl.inferred_columns = r.columns;
                    decl.opaque = r.opaque;
                }
                tasks.push(decl);
                break; // one trace-weaver decorator per function
            }
        }
    }
    Ok(ScannedModule { config, tasks })
}

/// Read a module-level `tw.configure(service=, database=, schema=, dag=)` call
/// and/or the `dag_id` of a `with DAG(dag_id=...)` block (configure wins).
/// Recurses into `if` / `with` / `try` / loop bodies so a `with DAG(...)` guarded
/// by `if DAG is not None:` (the common Airflow pattern) is still found.
fn extract_module_config(suite: &[Stmt], consts: &HashMap<String, String>) -> ModuleConfig {
    let mut cfg = ModuleConfig::default();
    walk_config(suite, consts, &mut cfg);
    cfg
}

fn walk_config(stmts: &[Stmt], consts: &HashMap<String, String>, cfg: &mut ModuleConfig) {
    for stmt in stmts {
        match stmt {
            // tw.configure(...) as a bare expression statement.
            Stmt::Expr(e) => {
                if let Expr::Call(call) = e.value.as_ref() {
                    if callee_final_segment(&call.func).as_deref() == Some("configure") {
                        for kw in &call.keywords {
                            let key = match &kw.arg {
                                Some(id) => id.to_string(),
                                None => continue,
                            };
                            let v = resolve_str(&kw.value, consts);
                            match key.as_str() {
                                "service" => cfg.service = v,
                                "database" => cfg.database = v,
                                "schema" => cfg.schema = v,
                                "dag" => cfg.dag = v,
                                _ => {}
                            }
                        }
                    }
                }
            }
            // with DAG(dag_id="...") : a file-level default dag (configure wins).
            Stmt::With(w) => {
                for item in &w.items {
                    if let Expr::Call(call) = &item.context_expr {
                        if callee_final_segment(&call.func).as_deref() == Some("DAG") {
                            for kw in &call.keywords {
                                if kw.arg.as_ref().map(|i| i.as_str()) == Some("dag_id")
                                    && cfg.dag.is_none()
                                {
                                    if let Some(s) = resolve_str(&kw.value, consts) {
                                        cfg.dag = Some(s);
                                    }
                                }
                            }
                        }
                    }
                }
                walk_config(&w.body, consts, cfg);
            }
            Stmt::If(i) => {
                walk_config(&i.body, consts, cfg);
                walk_config(&i.orelse, consts, cfg);
            }
            Stmt::For(f) => {
                walk_config(&f.body, consts, cfg);
                walk_config(&f.orelse, consts, cfg);
            }
            Stmt::While(w) => {
                walk_config(&w.body, consts, cfg);
                walk_config(&w.orelse, consts, cfg);
            }
            Stmt::Try(t) => {
                walk_config(&t.body, consts, cfg);
                walk_config(&t.orelse, consts, cfg);
                walk_config(&t.finalbody, consts, cfg);
            }
            _ => {}
        }
    }
}

/// If `expr` is `NAME = ...`'s single target, return the name.
fn single_target_name(targets: &[Expr]) -> Option<String> {
    if targets.len() != 1 {
        return None;
    }
    match &targets[0] {
        Expr::Name(n) => Some(n.id.to_string()),
        _ => None,
    }
}

/// Extract a string literal from a Constant expression.
pub(crate) fn const_str(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Constant(c) => match &c.value {
            Constant::Str(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// If `deco` is a recognised trace-weaver decorator Call, return it with its kind:
/// `task` → [`DecoratorKind::Task`]; `sql` → [`DecoratorKind::Sql`].
fn task_decorator_call(deco: &Expr) -> Option<(&ast::ExprCall, DecoratorKind)> {
    let call = match deco {
        Expr::Call(c) => c,
        _ => return None,
    };
    match callee_final_segment(&call.func)?.as_str() {
        "task" => Some((call, DecoratorKind::Task)),
        "sql" => Some((call, DecoratorKind::Sql)),
        _ => None,
    }
}

/// Final dotted segment of a callee expression: `task`, `tw.task` -> "task".
pub(crate) fn callee_final_segment(func: &Expr) -> Option<String> {
    match func {
        Expr::Name(n) => Some(n.id.to_string()),
        Expr::Attribute(a) => Some(a.attr.to_string()),
        _ => None,
    }
}

/// Resolve an expression to a string: either a literal, or a NAME reference
/// into the module-level constant table.
pub(crate) fn resolve_str(expr: &Expr, consts: &HashMap<String, String>) -> Option<String> {
    if let Some(s) = const_str(expr) {
        return Some(s);
    }
    if let Expr::Name(n) = expr {
        return consts.get(n.id.as_str()).cloned();
    }
    None
}

/// True if `expr` is a literal Python `None` (an explicit "no value", which we
/// must not flag as an unresolved/non-literal argument).
fn is_explicit_none(expr: &Expr) -> bool {
    matches!(expr, Expr::Constant(c) if matches!(c.value, Constant::None))
}

/// True if `expr` is a list/tuple literal in which at least one element could
/// not be resolved to a string (e.g. an f-string element), meaning some entries
/// were silently dropped. Non-list expressions (e.g. a bare variable) also count
/// as unresolved.
fn list_has_unresolved(expr: &Expr, consts: &HashMap<String, String>) -> bool {
    match expr {
        Expr::List(l) => l.elts.iter().any(|e| resolve_str(e, consts).is_none()),
        Expr::Tuple(t) => t.elts.iter().any(|e| resolve_str(e, consts).is_none()),
        _ => true,
    }
}

/// Resolve a list expression of strings (literals or NAME refs).
fn resolve_str_list(expr: &Expr, consts: &HashMap<String, String>) -> Vec<String> {
    let elts = match expr {
        Expr::List(l) => &l.elts,
        Expr::Tuple(t) => &t.elts,
        _ => return Vec::new(),
    };
    elts.iter().filter_map(|e| resolve_str(e, consts)).collect()
}

/// Parse a `column_map=[ (..), (..) ]` list into entries.
fn parse_column_map(expr: &Expr, consts: &HashMap<String, String>) -> Vec<ColumnMapEntry> {
    let elts = match expr {
        Expr::List(l) => &l.elts,
        Expr::Tuple(t) => &t.elts,
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for elt in elts {
        let tuple = match elt {
            Expr::Tuple(t) => &t.elts,
            Expr::List(l) => &l.elts,
            _ => continue,
        };
        if tuple.len() < 2 {
            continue;
        }
        // sources may be a list/tuple of names OR a single bare string (sugar for
        // a 1-element list): ("amount", "is_valid", "amount > 0"). A bare string is
        // only accepted HERE, in a column_map source position — not in inputs=/
        // outputs=, which keep requiring a list (resolve_str_list is unchanged).
        let sources = match resolve_str(&tuple[0], consts) {
            Some(s) => vec![s],
            None => resolve_str_list(&tuple[0], consts),
        };
        let target = match resolve_str(&tuple[1], consts) {
            Some(t) => t,
            None => continue,
        };
        let function = tuple.get(2).and_then(|e| resolve_str(e, consts));
        out.push(ColumnMapEntry {
            sources,
            target,
            function,
        });
    }
    out
}

/// Build a TaskDecl from a decorator Call.
fn build_decl(
    name: &str,
    call: &ast::ExprCall,
    source: &str,
    consts: &HashMap<String, String>,
    kind: DecoratorKind,
) -> TaskDecl {
    let mut decl = TaskDecl {
        task_name: name.to_string(),
        line: Some(byte_to_line(source, call.range().start().to_usize())),
        ..Default::default()
    };

    // `@tw.sql(QUERY, …)` shortcut: engine is implicitly "sql" and the first
    // positional argument is the SQL query (literal or module-level constant).
    if matches!(kind, DecoratorKind::Sql) {
        decl.engine = Some("sql".to_string());
        if let Some(first) = call.args.first() {
            match resolve_str(first, consts) {
                Some(s) => decl.sql = Some(s),
                None if is_explicit_none(first) => {}
                None => decl.unresolved.push("sql".to_string()),
            }
        }
    }

    for kw in &call.keywords {
        let key = match &kw.arg {
            Some(id) => id.to_string(),
            None => continue, // **kwargs splat — ignore
        };
        // Helper: resolve a scalar string kwarg, flagging it as unresolved when a
        // value is present but cannot be reduced to a literal/constant.
        let mut scalar =
            |target: &mut Option<String>, value: &Expr| match resolve_str(value, consts) {
                Some(s) => *target = Some(s),
                None if is_explicit_none(value) => {}
                None => decl.unresolved.push(key.clone()),
            };
        match key.as_str() {
            "dag" => scalar(&mut decl.dag, &kw.value),
            "inputs" => {
                decl.inputs = resolve_str_list(&kw.value, consts);
                if list_has_unresolved(&kw.value, consts) {
                    decl.unresolved.push(key.clone());
                }
            }
            "outputs" => {
                decl.outputs = resolve_str_list(&kw.value, consts);
                if list_has_unresolved(&kw.value, consts) {
                    decl.unresolved.push(key.clone());
                }
            }
            "engine" => scalar(&mut decl.engine, &kw.value),
            "sql" => scalar(&mut decl.sql, &kw.value),
            "description" => scalar(&mut decl.description, &kw.value),
            "transform" => scalar(&mut decl.transform, &kw.value),
            "column_map" => decl.column_map = parse_column_map(&kw.value, consts),
            "copy" => {
                decl.copy = resolve_str_list(&kw.value, consts);
                if list_has_unresolved(&kw.value, consts) {
                    decl.unresolved.push(key.clone());
                }
            }
            _ => {}
        }
    }

    // Expand `copy=[...]` into declared same-name identity entries. An explicit
    // `column_map` entry for the same target always wins, so copy never overrides
    // it (dedupe by target). The result is ordinary DECLARED column lineage — the
    // same shape an author could have written by hand as (["x"], "x", "direct copy").
    let copy_names = decl.copy.clone();
    for name in &copy_names {
        if !decl.column_map.iter().any(|e| e.target == *name) {
            decl.column_map.push(ColumnMapEntry {
                sources: vec![name.clone()],
                target: name.clone(),
                function: Some("direct copy".to_string()),
            });
        }
    }
    decl
}

/// Convert a byte offset to a 1-based line number.
pub(crate) fn byte_to_line(source: &str, byte_off: usize) -> u32 {
    let off = byte_off.min(source.len());
    let line = source.as_bytes()[..off]
        .iter()
        .filter(|&&b| b == b'\n')
        .count();
    (line as u32) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_basic_task() {
        let src = r#"
import trace_weaver as tw
MY_SQL = "INSERT INTO t (a) SELECT x FROM s"
@tw.task(
    dag="d",
    inputs=["svc.db.sch.s"],
    outputs=["svc.db.sch.t"],
    engine="sql",
    sql=MY_SQL,
    transform="COPY",
    column_map=[(["x"], "a", "direct")],
)
def build():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.tasks.len(), 1);
        let d = &m.tasks[0];
        assert_eq!(d.task_name, "build");
        assert_eq!(d.dag.as_deref(), Some("d"));
        assert_eq!(d.inputs, vec!["svc.db.sch.s"]);
        assert_eq!(d.outputs, vec!["svc.db.sch.t"]);
        assert_eq!(d.engine.as_deref(), Some("sql"));
        assert_eq!(d.sql.as_deref(), Some("INSERT INTO t (a) SELECT x FROM s"));
        assert_eq!(d.column_map.len(), 1);
        assert_eq!(d.column_map[0].sources, vec!["x"]);
        assert_eq!(d.column_map[0].target, "a");
        assert!(d.line.is_some());
    }

    #[test]
    fn supports_from_import_task_form() {
        let src = r#"
from trace_weaver import task
@task(inputs=["a.b.c.d"], outputs=["a.b.c.e"], engine="pandas")
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.tasks.len(), 1);
        assert_eq!(m.tasks[0].engine.as_deref(), Some("pandas"));
    }

    #[test]
    fn ignores_non_task_decorators() {
        let src = r#"
@property
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert!(m.tasks.is_empty());
    }

    #[test]
    fn configure_defaults_and_with_dag_and_sql_shortcut() {
        // Tier 1+2: configure() defaults, with DAG(dag_id=) fallback, @tw.sql shortcut.
        let src = r#"
import trace_weaver as tw
tw.configure(service="Test Database", database="poc_db", schema="public")
B_SQL = "INSERT INTO bronze (a) SELECT x FROM landing"
with DAG(dag_id="medallion") as dag:
    pass
@tw.sql(B_SQL, inputs=["landing_sales"], outputs=["bronze_sales"], transform="COPY")
def build_bronze():
    pass
@tw.task(inputs=["bronze_sales"], outputs=["silver_sales"], column_map=[(["a"], "b")])
def build_silver():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.config.service.as_deref(), Some("Test Database"));
        assert_eq!(m.config.database.as_deref(), Some("poc_db"));
        assert_eq!(m.config.schema.as_deref(), Some("public"));
        assert_eq!(m.config.dag.as_deref(), Some("medallion"));
        assert_eq!(m.tasks.len(), 2);
        // @tw.sql sets engine + positional sql.
        let bronze = &m.tasks[0];
        assert_eq!(bronze.engine.as_deref(), Some("sql"));
        assert_eq!(
            bronze.sql.as_deref(),
            Some("INSERT INTO bronze (a) SELECT x FROM landing")
        );
        // @tw.task with no engine is fine (engine inferred downstream).
        assert!(m.tasks[1].engine.is_none());
    }

    #[test]
    fn copy_shortcut_and_bare_string_sources() {
        // copy=[...] expands to declared same-name identity entries; a bare-string
        // source in column_map is sugar for a 1-element list; an explicit
        // column_map entry for a target also named in copy WINS (copy is skipped).
        let src = r#"
import trace_weaver as tw
@tw.task(
    inputs=["svc.db.sch.bronze"],
    outputs=["svc.db.sch.silver"],
    engine="pandas",
    copy=["event_id", "amount"],
    column_map=[
        ("event_ts", "event_date", "CAST ts -> date"),
        (["amount"], "amount", "ROUND(amount, 2)"),
    ],
)
def build_silver():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.tasks.len(), 1);
        let d = &m.tasks[0];
        assert_eq!(d.copy, vec!["event_id", "amount"]);

        // Bare-string source resolves to a 1-element sources vec (no brackets needed).
        let event_date = d
            .column_map
            .iter()
            .find(|e| e.target == "event_date")
            .unwrap();
        assert_eq!(event_date.sources, vec!["event_ts"]);
        assert_eq!(event_date.function.as_deref(), Some("CAST ts -> date"));

        // copy "event_id" expanded to a declared same-name identity ("direct copy").
        let event_id = d
            .column_map
            .iter()
            .find(|e| e.target == "event_id")
            .unwrap();
        assert_eq!(event_id.sources, vec!["event_id"]);
        assert_eq!(event_id.function.as_deref(), Some("direct copy"));

        // "amount" is in BOTH copy and column_map — column_map wins: exactly one
        // entry, keeping the explicit function (no duplicate same-name copy added).
        let amounts: Vec<_> = d
            .column_map
            .iter()
            .filter(|e| e.target == "amount")
            .collect();
        assert_eq!(amounts.len(), 1);
        assert_eq!(amounts[0].function.as_deref(), Some("ROUND(amount, 2)"));
    }

    #[test]
    fn copy_with_non_literal_element_is_flagged() {
        // A non-literal element in copy=[...] is dropped and surfaced as W_NON_LITERAL
        // (same contract as inputs=/outputs=).
        let src = r#"
import trace_weaver as tw
COL = "amount"
@tw.task(inputs=["a.b.c.d"], outputs=["a.b.c.e"], engine="pandas",
         copy=[COL, f"dynamic_{COL}"])
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        // The constant ref resolves; the f-string does not, so it's dropped + flagged.
        assert_eq!(d.copy, vec!["amount"]);
        assert!(d.unresolved.contains(&"copy".to_string()));
    }
}
