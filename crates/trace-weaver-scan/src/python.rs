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
    /// True when this declaration was synthesized by decorator-FREE discovery
    /// (Pass B) from a raw Airflow operator, rather than authored via `@tw`. The
    /// whole task — its job, edges and datasets, not just its columns — is then a
    /// machine inference, so `scan_decl` stamps those structural elements with an
    /// inferred `Origin` instead of declared.
    pub discovered: bool,
    /// True when this declaration came from the `@lineage(...)` authoring
    /// decorator — declarative *dataset-level* lineage — rather than
    /// `@tw.task`/`@tw.sql` or decorator-free discovery. Such tasks declare
    /// input/output datasets only (no column_map / SQL), so they are exempt
    /// from the "must declare inputs= and outputs=" and OM-FQN hygiene checks.
    pub lineage: bool,
    /// Dataset entries (from a `@lineage` `inputs=`/`outputs=`) that were NOT
    /// string literals/constants. They are kept as a best-effort source-text
    /// representation, and recorded here so the scanner stamps them
    /// medium-confidence (inferred) provenance instead of declared/high.
    pub nonliteral: std::collections::HashSet<String>,
    /// True when this declaration is a COLUMN/dataflow-discovery result (Pass C):
    /// an undecorated, un-wired top-level function (e.g. the `run(spark, …)`
    /// transform protocol) whose body was analyzed purely to recover column
    /// mappings. Such a decl contributes datasets + column-carrying edges but
    /// **no job**, so it never enters the gate's task denominator.
    pub column_only: bool,
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

/// Maximum `NAME = OTHER_NAME` alias hops followed when resolving a module-level
/// constant. Deep chains are unusual; the cap plus the visited-set cycle guard
/// keep resolution bounded and terminate on `A = B; B = A`.
const MAX_ALIAS_DEPTH: usize = 8;

/// A repo-wide table of module-level string constants, keyed by the dotted
/// module path a file would be imported under (e.g. `config.datasets`). Built by
/// [`scan_path`](crate::scan_path) before any file is scanned, so a `@lineage`
/// dataset declared as `from config.datasets import RAW_SALES` resolves to the
/// literal defined in the other file — staying declared/HIGH confidence rather
/// than falling back to inferred/MEDIUM.
///
/// Values are fully resolved string literals: local `NAME = OTHER_NAME` aliasing
/// (bounded by [`MAX_ALIAS_DEPTH`] with a cycle guard) is collapsed at build
/// time, so a lookup returns the final string or nothing.
#[derive(Debug, Clone, Default)]
pub struct ConstTable {
    modules: HashMap<String, HashMap<String, String>>,
}

impl ConstTable {
    /// Record one module's resolved constants under its dotted path.
    pub fn insert_module(
        &mut self,
        module_path: impl Into<String>,
        consts: HashMap<String, String>,
    ) {
        if consts.is_empty() {
            return;
        }
        self.modules.insert(module_path.into(), consts);
    }

    /// Look up `name` in `module`. Tries an exact module match first, then a
    /// unique suffix match so a scan root that sits above the Python import root
    /// (or below it) still resolves — but only when exactly ONE module carries
    /// the name, never guessing between ambiguous candidates.
    fn lookup(&self, module: &str, name: &str) -> Option<&String> {
        if let Some(v) = self.modules.get(module).and_then(|m| m.get(name)) {
            return Some(v);
        }
        let dotted_module = format!(".{module}");
        let mut hit = None;
        let mut count = 0usize;
        for (key, consts) in &self.modules {
            let related = key == module
                || key.ends_with(&dotted_module)
                || module.ends_with(&format!(".{key}"));
            if related {
                if let Some(v) = consts.get(name) {
                    hit = Some(v);
                    count += 1;
                }
            }
        }
        (count == 1).then_some(hit).flatten()
    }
}

/// Collect the module-level string constants of one source file, with one-level
/// (`NAME = OTHER_NAME`) aliasing resolved. Used to seed [`ConstTable`].
pub fn collect_module_constants(source: &str) -> HashMap<String, String> {
    match ast::Suite::parse(source, "<module>") {
        Ok(suite) => constants_from_suite(&suite),
        Err(_) => HashMap::new(),
    }
}

/// Gather module-level `NAME = "literal"` constants from a parsed suite, folding
/// `NAME = OTHER_NAME` aliases into their ultimate literal value. Alias chains
/// are followed up to [`MAX_ALIAS_DEPTH`] hops with a visited-set cycle guard;
/// a dangling or cyclic alias simply resolves to nothing.
fn constants_from_suite(suite: &[Stmt]) -> HashMap<String, String> {
    let mut literals: HashMap<String, String> = HashMap::new();
    let mut aliases: HashMap<String, String> = HashMap::new();
    for stmt in suite {
        if let Stmt::Assign(assign) = stmt {
            let name = match single_target_name(&assign.targets) {
                Some(n) => n,
                None => continue,
            };
            if let Some(s) = const_str(&assign.value) {
                literals.insert(name, s);
            } else if let Expr::Name(n) = assign.value.as_ref() {
                // NAME = OTHER_NAME — a one-level alias (chains resolved below).
                aliases.insert(name, n.id.to_string());
            }
        }
    }
    let mut out = literals.clone();
    for (name, first) in &aliases {
        if out.contains_key(name) {
            continue; // a literal binding always wins over an alias
        }
        let mut target = first.clone();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        seen.insert(name.clone());
        for _ in 0..MAX_ALIAS_DEPTH {
            if !seen.insert(target.clone()) {
                break; // cycle
            }
            if let Some(s) = literals.get(&target) {
                out.insert(name.clone(), s.clone());
                break;
            }
            match aliases.get(&target) {
                Some(next) => target = next.clone(),
                None => break, // dangling
            }
        }
    }
    out
}

/// Import bindings collected from a consuming file, used to resolve dataset
/// references that name a constant living in another module.
#[derive(Debug, Default)]
struct ImportEnv {
    /// `from MODULE import NAME [as LOCAL]` → LOCAL ⇒ (MODULE, NAME).
    from_imports: HashMap<String, (String, String)>,
    /// `import MODULE as ALIAS` → ALIAS ⇒ MODULE (the dotted module path).
    module_aliases: HashMap<String, String>,
}

/// Bundle of everything the decl-builders need to resolve decorator arguments:
/// the current file's local constants, its import bindings, and the repo-wide
/// [`ConstTable`]. Local/same-module resolution never needs the table.
struct Ctx<'a> {
    consts: &'a HashMap<String, String>,
    imports: ImportEnv,
    table: &'a ConstTable,
}

impl Ctx<'_> {
    /// Resolve an expression used in a **dataset** position (`@lineage`/`@tw`
    /// `inputs=`/`outputs=`) to a string literal, following module-level
    /// constants across files:
    ///   * a string literal, or
    ///   * a bare `NAME` defined in this module, or
    ///   * a bare `NAME` brought in by `from MODULE import NAME [as ..]`, or
    ///   * an attribute `m.NAME` / `pkg.mod.NAME` where the module part was
    ///     `import`ed (aliased or dotted).
    ///
    /// Returns `None` for anything else (calls, f-strings, subscripts, unknown
    /// names) so callers keep today's medium/inferred fallback — never a guess.
    fn dataset_str(&self, expr: &Expr) -> Option<String> {
        if let Some(s) = const_str(expr) {
            return Some(s);
        }
        match expr {
            Expr::Name(n) => {
                let id = n.id.as_str();
                if let Some(s) = self.consts.get(id) {
                    return Some(s.clone());
                }
                if let Some((module, orig)) = self.imports.from_imports.get(id) {
                    return self.table.lookup(module, orig).cloned();
                }
                None
            }
            Expr::Attribute(a) => {
                let base = dotted_path(&a.value)?;
                let module = self
                    .imports
                    .module_aliases
                    .get(&base)
                    .cloned()
                    .unwrap_or(base);
                self.table.lookup(&module, a.attr.as_str()).cloned()
            }
            _ => None,
        }
    }

    /// Resolve a list/tuple of dataset strings, dropping any element that does
    /// not resolve (mirrors [`resolve_str_list`], but constant-aware across
    /// files). Non-list expressions yield an empty vec.
    fn dataset_str_list(&self, expr: &Expr) -> Vec<String> {
        let elts = match expr {
            Expr::List(l) => &l.elts,
            Expr::Tuple(t) => &t.elts,
            _ => return Vec::new(),
        };
        elts.iter().filter_map(|e| self.dataset_str(e)).collect()
    }

    /// True if `expr` is a list/tuple with at least one element that could not
    /// be resolved to a dataset string (so some entries were dropped), or is not
    /// a list at all.
    fn dataset_list_has_unresolved(&self, expr: &Expr) -> bool {
        match expr {
            Expr::List(l) => l.elts.iter().any(|e| self.dataset_str(e).is_none()),
            Expr::Tuple(t) => t.elts.iter().any(|e| self.dataset_str(e).is_none()),
            _ => true,
        }
    }
}

/// Reconstruct a dotted module path (`a.b.c`) from a `Name`/`Attribute` chain,
/// or `None` if the expression is anything else (e.g. a subscript or call).
fn dotted_path(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Name(n) => Some(n.id.to_string()),
        Expr::Attribute(a) => Some(format!("{}.{}", dotted_path(&a.value)?, a.attr)),
        _ => None,
    }
}

/// Collect a consuming file's `import` / `from ... import` bindings. `module_path`
/// is the current file's own dotted path, used to resolve relative imports
/// (`from .datasets import X`) against its package.
fn collect_imports(suite: &[Stmt], module_path: &str) -> ImportEnv {
    let mut env = ImportEnv::default();
    for stmt in suite {
        match stmt {
            Stmt::Import(imp) => {
                for alias in &imp.names {
                    let module = alias.name.to_string();
                    // `import a.b.c as m` binds m ⇒ a.b.c. Without `as`, `a.b.c.NAME`
                    // is reconstructed directly by `dotted_path`, so no binding is
                    // needed — but record the leading name for completeness.
                    if let Some(asname) = &alias.asname {
                        env.module_aliases.insert(asname.to_string(), module);
                    }
                }
            }
            Stmt::ImportFrom(imp) => {
                let level = imp.level.as_ref().map(|l| l.to_usize()).unwrap_or(0);
                let base = match &imp.module {
                    Some(m) => m.to_string(),
                    None => String::new(),
                };
                let module = resolve_import_module(module_path, level, &base);
                for alias in &imp.names {
                    let orig = alias.name.to_string();
                    if orig == "*" {
                        continue; // star import — nothing statically nameable
                    }
                    let local = alias
                        .asname
                        .as_ref()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| orig.clone());
                    env.from_imports.insert(local, (module.clone(), orig));
                }
            }
            _ => {}
        }
    }
    env
}

/// Resolve the target module of a (possibly relative) `from` import to an
/// absolute dotted path. `level` is the number of leading dots; `base` is the
/// text after them (may be empty for `from . import x`).
fn resolve_import_module(current: &str, level: usize, base: &str) -> String {
    if level == 0 {
        return base.to_string();
    }
    // A relative import is anchored at the current module's package. `current`
    // is the file's own module path, so drop its final segment (the module
    // name) plus one for each extra dot beyond the first.
    let mut parts: Vec<&str> = current.split('.').filter(|s| !s.is_empty()).collect();
    for _ in 0..level {
        parts.pop();
    }
    let pkg = parts.join(".");
    match (pkg.is_empty(), base.is_empty()) {
        (true, _) => base.to_string(),
        (false, true) => pkg,
        (false, false) => format!("{pkg}.{base}"),
    }
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
    extract_task_decls_with(source, "", &ConstTable::default())
}

/// Like [`extract_task_decls`], but resolves dataset references against the
/// repo-wide [`ConstTable`]. `module_path` is this file's own dotted module path
/// (used to anchor relative `from . import` statements). Dataset strings in
/// `@lineage`/`@tw` `inputs=`/`outputs=` that name a module-level string
/// constant — locally or via `import`/`from ... import` — resolve to the literal
/// and stay declared/HIGH confidence.
pub fn extract_task_decls_with(
    source: &str,
    module_path: &str,
    table: &ConstTable,
) -> anyhow::Result<ScannedModule> {
    let suite = ast::Suite::parse(source, "<module>")
        .map_err(|e| anyhow::anyhow!("python parse error: {e}"))?;

    // First pass: collect module-level string constants (`NAME = "..."`, plus
    // one-level `NAME = OTHER_NAME` aliasing), and this file's import bindings.
    let consts = constants_from_suite(&suite);
    let ctx = Ctx {
        consts: &consts,
        imports: collect_imports(&suite, module_path),
        table,
    };

    let config = extract_module_config(&suite, &consts);

    // Module-level function defs, for resolving `python_callable=fn`.
    let mut funcs: HashMap<&str, &ast::StmtFunctionDef> = HashMap::new();
    for stmt in &suite {
        if let Stmt::FunctionDef(f) = stmt {
            funcs.insert(f.name.as_str(), f);
        }
    }

    // Local names bound to the `@lineage` authoring decorator (all import forms).
    let lineage_names = collect_lineage_names(&suite);

    // Pass A: explicit `@lineage` / `@tw.task` / `@tw.sql` declarations.
    let mut tasks = Vec::new();
    let mut decorated: std::collections::HashSet<String> = std::collections::HashSet::new();
    'func: for stmt in &suite {
        let func = match stmt {
            Stmt::FunctionDef(f) => f,
            _ => continue,
        };

        // Prefer `@lineage`: a declarative dataset-level marker. When present it
        // owns the declaration (a co-located Airflow `@task` carries no lineage),
        // so build from it and skip the `@tw.task`/`@tw.sql` path for this func.
        for deco in &func.decorator_list {
            if let Some(call) = lineage_decorator(deco, &lineage_names) {
                decorated.insert(func.name.to_string());
                let mut decl = build_lineage_decl(func.name.as_str(), call, source, &ctx);
                // Un-suppress dataflow under @lineage: the declared datasets stay
                // declared/HIGH, but we still trace the body so inferable column
                // mappings (e.g. a literal-dict .rename(), a spark.sql INSERT)
                // attach beneath the declaration. Purely additive — no new tasks.
                // Opaque notes are intentionally NOT surfaced here: @lineage is a
                // deliberate dataset-level declaration, so "declare this column in
                // column_map" advisories would contradict its intent (mirroring the
                // suppressed W_NO_COLUMN_LINEAGE) and would perturb diagnostics.
                let r = crate::dataflow::analyze(&func.body, &consts, source);
                decl.inferred_columns = r.columns;
                tasks.push(decl);
                continue 'func;
            }
        }

        for deco in &func.decorator_list {
            if let Some((call, kind)) = task_decorator_call(deco) {
                decorated.insert(func.name.to_string());
                let mut decl = build_decl(func.name.as_str(), call, source, &ctx, kind);
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

    // Pass B: decorator-FREE discovery from raw Airflow operators. A plain DAG with
    // no `@tw` still yields lineage: PythonOperator(python_callable=fn) analyzes the
    // callable's body, and SQL operators (sql=/query=) parse their query. A `@tw`
    // decorator on the same function overrides (deduped by name).
    discover_operators(&suite, source, &consts, &funcs, &decorated, &mut tasks);

    // Pass C: COLUMN/dataflow discovery for top-level functions that neither a
    // trace-weaver decorator (Pass A) nor an Airflow operator (Pass B) reached —
    // notably the `def run(spark, src_path, …)` transform protocol dispatched via
    // importlib at runtime. These are analyzed for column lineage ONLY: each
    // yields datasets + column-carrying edges but NO job, so it never enters the
    // gate's task denominator (a machine-inferred column map, not a task).
    let mut claimed = decorated.clone();
    collect_python_callables(&suite, &mut claimed);
    discover_dataflow_columns(&suite, source, &consts, &claimed, &mut tasks);

    Ok(ScannedModule { config, tasks })
}

/// Collect every function name bound to a `python_callable=<Name>` on any
/// operator constructor, recursing into block bodies. These functions are
/// already reached by Pass B, so Pass C must not re-analyze them.
fn collect_python_callables(stmts: &[Stmt], out: &mut std::collections::HashSet<String>) {
    for stmt in stmts {
        let call = match stmt {
            Stmt::Assign(a) => match a.value.as_ref() {
                Expr::Call(c) => Some(c),
                _ => None,
            },
            Stmt::Expr(e) => match e.value.as_ref() {
                Expr::Call(c) => Some(c),
                _ => None,
            },
            _ => None,
        };
        if let Some(c) = call {
            if let Some(Expr::Name(f)) = kwarg(c, "python_callable") {
                out.insert(f.id.to_string());
            }
        }
        match stmt {
            Stmt::With(w) => collect_python_callables(&w.body, out),
            Stmt::If(i) => {
                collect_python_callables(&i.body, out);
                collect_python_callables(&i.orelse, out);
            }
            Stmt::For(f) => collect_python_callables(&f.body, out),
            Stmt::While(w) => collect_python_callables(&w.body, out),
            Stmt::Try(t) => {
                collect_python_callables(&t.body, out);
                collect_python_callables(&t.finalbody, out);
            }
            _ => {}
        }
    }
}

/// Pass C worker: for every module-top-level function not already `claimed`
/// (decorated, or wired as a `python_callable`), trace its body for column
/// lineage and — when the trace found any column mappings — emit a
/// `column_only` [`TaskDecl`]. Functions whose body yields no columns are
/// skipped entirely (no datasets, no edges, no diagnostics, no job).
fn discover_dataflow_columns(
    suite: &[Stmt],
    source: &str,
    consts: &HashMap<String, String>,
    claimed: &std::collections::HashSet<String>,
    tasks: &mut Vec<TaskDecl>,
) {
    for stmt in suite {
        let func = match stmt {
            Stmt::FunctionDef(f) => f,
            _ => continue,
        };
        let name = func.name.as_str();
        if claimed.contains(name) || tasks.iter().any(|t| t.task_name == name) {
            continue;
        }
        let r = crate::dataflow::analyze(&func.body, consts, source);
        if r.columns.is_empty() {
            continue; // nothing column-level we can see — not a data transform
        }
        tasks.push(TaskDecl {
            task_name: name.to_string(),
            inputs: r.inputs,
            outputs: r.outputs,
            engine: Some(r.engine.unwrap_or_else(|| "python".to_string())),
            inferred_columns: r.columns,
            line: Some(byte_to_line(source, func.range().start().to_usize())),
            discovered: true,
            column_only: true,
            // Opaque notes are intentionally dropped: these are discovery
            // targets, not authored tasks, so "declare this column" advisories
            // would be noise and would perturb diagnostic counts.
            ..Default::default()
        });
    }
}

/// One keyword argument of a call by name.
fn kwarg<'a>(call: &'a ast::ExprCall, name: &str) -> Option<&'a Expr> {
    call.keywords
        .iter()
        .find(|k| k.arg.as_ref().map(|i| i.as_str()) == Some(name))
        .map(|k| &k.value)
}

/// Walk `with DAG(...)` / module statements for Airflow operator constructors and
/// synthesize a [`TaskDecl`] for each data-producing one (no `@tw` required).
fn discover_operators(
    stmts: &[Stmt],
    source: &str,
    consts: &HashMap<String, String>,
    funcs: &HashMap<&str, &ast::StmtFunctionDef>,
    decorated: &std::collections::HashSet<String>,
    tasks: &mut Vec<TaskDecl>,
) {
    for stmt in stmts {
        // Operators appear as `x = XOperator(...)` or a bare `XOperator(...)`.
        let call = match stmt {
            Stmt::Assign(a) => match a.value.as_ref() {
                Expr::Call(c) => Some(c),
                _ => None,
            },
            Stmt::Expr(e) => match e.value.as_ref() {
                Expr::Call(c) => Some(c),
                _ => None,
            },
            _ => None,
        };
        if let Some(c) = call {
            if let Some(decl) = build_operator_task(c, source, consts, funcs, decorated) {
                if !tasks.iter().any(|t| t.task_name == decl.task_name) {
                    tasks.push(decl);
                }
            }
        }
        // Recurse into block bodies (with DAG(...): / if / for / while / try).
        match stmt {
            Stmt::With(w) => discover_operators(&w.body, source, consts, funcs, decorated, tasks),
            Stmt::If(i) => {
                discover_operators(&i.body, source, consts, funcs, decorated, tasks);
                discover_operators(&i.orelse, source, consts, funcs, decorated, tasks);
            }
            Stmt::For(f) => discover_operators(&f.body, source, consts, funcs, decorated, tasks),
            Stmt::While(w) => discover_operators(&w.body, source, consts, funcs, decorated, tasks),
            Stmt::Try(t) => {
                discover_operators(&t.body, source, consts, funcs, decorated, tasks);
                discover_operators(&t.finalbody, source, consts, funcs, decorated, tasks);
            }
            _ => {}
        }
    }
}

/// Synthesize a [`TaskDecl`] from one Airflow operator constructor call, or `None`
/// if it is not a recognised data-producing operator.
fn build_operator_task(
    call: &ast::ExprCall,
    source: &str,
    consts: &HashMap<String, String>,
    funcs: &HashMap<&str, &ast::StmtFunctionDef>,
    decorated: &std::collections::HashSet<String>,
) -> Option<TaskDecl> {
    let callee = callee_final_segment(&call.func)?;
    if !callee.ends_with("Operator") {
        return None;
    }
    let task_id = kwarg(call, "task_id").and_then(const_str);
    let line = Some(byte_to_line(source, call.range().start().to_usize()));

    // PythonOperator(python_callable=fn) — analyze the callable's body.
    if let Some(Expr::Name(fname)) = kwarg(call, "python_callable") {
        let fname = fname.id.as_str();
        if decorated.contains(fname) {
            return None; // an explicit @tw decorator on this function overrides
        }
        let func = funcs.get(fname)?;
        let r = crate::dataflow::analyze(&func.body, consts, source);
        if r.inputs.is_empty() && r.outputs.is_empty() && r.columns.is_empty() {
            return None; // nothing data-producing we can see statically
        }
        return Some(TaskDecl {
            task_name: task_id.unwrap_or_else(|| fname.to_string()),
            inputs: r.inputs,
            outputs: r.outputs,
            engine: Some(r.engine.unwrap_or_else(|| "python".to_string())),
            inferred_columns: r.columns,
            opaque: r.opaque,
            line,
            discovered: true,
            ..Default::default()
        });
    }

    // SQL operator (sql=/query=) — parse the query for table + column lineage.
    let sql = kwarg(call, "sql")
        .or_else(|| kwarg(call, "query"))
        .and_then(|e| resolve_str(e, consts));
    if let Some(sql_text) = sql {
        let lineage = crate::sql::column_lineage_from_sql(&sql_text).ok()?;
        let outputs = lineage.target_table.into_iter().collect();
        return Some(TaskDecl {
            task_name: task_id.unwrap_or_else(|| "sql_task".to_string()),
            engine: Some("sql".to_string()),
            sql: Some(sql_text),
            inputs: lineage.source_tables,
            outputs,
            line,
            discovered: true,
            ..Default::default()
        });
    }
    None
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

/// Local names that refer to the trace-weaver `@lineage` decorator.
///
/// The bare name `"lineage"` is always included (mirroring how `task`/`sql` are
/// recognised by their final segment), plus any alias bound by
/// `from ... import lineage as X`. Combined with attribute-form matching in
/// [`lineage_decorator`], this resolves all documented import forms:
/// `from traceweaver import lineage`, `... import lineage as X`,
/// `import traceweaver` + `@traceweaver.lineage`, and
/// `import traceweaver as tw` + `@tw.lineage` (the real package is
/// `trace_weaver`, but matching is by symbol/final segment, so either spelling
/// works).
fn collect_lineage_names(suite: &[Stmt]) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::new();
    names.insert("lineage".to_string());
    for stmt in suite {
        if let Stmt::ImportFrom(imp) = stmt {
            for alias in &imp.names {
                if alias.name.as_str() == "lineage" {
                    let bound = alias
                        .asname
                        .as_ref()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| "lineage".to_string());
                    names.insert(bound);
                }
            }
        }
    }
    names
}

/// If `deco` is a recognised `@lineage` decorator, return `Some(call)` where the
/// inner is `Some(..)` for the `@lineage(...)` call form and `None` for the bare
/// `@lineage` / `@tw.lineage` form. Matches a bare/aliased name against
/// `names`, and any `*.lineage` attribute by its final segment.
fn lineage_decorator<'a>(
    deco: &'a Expr,
    names: &std::collections::HashSet<String>,
) -> Option<Option<&'a ast::ExprCall>> {
    match deco {
        // Bare name: @lineage or an aliased @X.
        Expr::Name(n) if names.contains(n.id.as_str()) => Some(None),
        // Bare attribute: @tw.lineage / @traceweaver.lineage.
        Expr::Attribute(a) if a.attr.as_str() == "lineage" => Some(None),
        // Call form: @lineage(...) / @tw.lineage(...) / @X(...).
        Expr::Call(c) => {
            let is_lineage = match c.func.as_ref() {
                Expr::Name(n) => names.contains(n.id.as_str()),
                Expr::Attribute(a) => a.attr.as_str() == "lineage",
                _ => false,
            };
            is_lineage.then_some(Some(c))
        }
        _ => None,
    }
}

/// Build a [`TaskDecl`] from an `@lineage(...)` decorator (or bare `@lineage`).
///
/// String-literal dataset entries are kept as-is (declared → high confidence);
/// non-literal entries (f-strings, names, calls) are kept as a best-effort
/// source-text representation and recorded in `nonliteral` so the scan stamps
/// them medium confidence. `name=` overrides the task name; `description=` is
/// carried through.
fn build_lineage_decl(
    name: &str,
    call: Option<&ast::ExprCall>,
    source: &str,
    ctx: &Ctx,
) -> TaskDecl {
    let mut decl = TaskDecl {
        task_name: name.to_string(),
        lineage: true,
        ..Default::default()
    };
    let call = match call {
        Some(c) => c,
        // Bare @lineage: the function is marked, with no declared datasets.
        None => return decl,
    };
    decl.line = Some(byte_to_line(source, call.range().start().to_usize()));

    for kw in &call.keywords {
        let key = match &kw.arg {
            Some(id) => id.to_string(),
            None => continue, // **kwargs splat
        };
        match key.as_str() {
            "inputs" => {
                let (vals, nl) = resolve_uri_list(&kw.value, ctx, source);
                decl.inputs = vals;
                decl.nonliteral.extend(nl);
            }
            "outputs" => {
                let (vals, nl) = resolve_uri_list(&kw.value, ctx, source);
                decl.outputs = vals;
                decl.nonliteral.extend(nl);
            }
            "name" => {
                if let Some(s) = resolve_str(&kw.value, ctx.consts) {
                    if !s.is_empty() {
                        decl.task_name = s;
                    }
                }
            }
            "description" => match resolve_str(&kw.value, ctx.consts) {
                Some(s) => decl.description = Some(s),
                None if is_explicit_none(&kw.value) => {}
                None => decl.description = Some(expr_source_text(&kw.value, source)),
            },
            _ => {}
        }
    }
    decl
}

/// Resolve a `@lineage` `inputs=`/`outputs=` list into `(values, nonliteral)`.
///
/// Unlike [`resolve_str_list`], non-literal elements are NOT dropped: each is
/// kept as its best-effort source text and also returned in the second vec. A
/// non-list value (e.g. a bare variable) is treated as one non-literal entry.
fn resolve_uri_list(expr: &Expr, ctx: &Ctx, source: &str) -> (Vec<String>, Vec<String>) {
    let elts = match expr {
        Expr::List(l) => &l.elts,
        Expr::Tuple(t) => &t.elts,
        _ => {
            let text = expr_source_text(expr, source);
            return (vec![text.clone()], vec![text]);
        }
    };
    let mut vals = Vec::new();
    let mut nonlit = Vec::new();
    for e in elts {
        match ctx.dataset_str(e) {
            Some(s) => vals.push(s),
            None => {
                let text = expr_source_text(e, source);
                vals.push(text.clone());
                nonlit.push(text);
            }
        }
    }
    (vals, nonlit)
}

/// Best-effort textual representation of an expression: its exact source slice
/// (trimmed). Used to keep non-literal `@lineage` datasets readable.
fn expr_source_text(expr: &Expr, source: &str) -> String {
    let r = expr.range();
    let start = r.start().to_usize();
    let end = r.end().to_usize();
    source
        .get(start..end)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
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
    ctx: &Ctx,
    kind: DecoratorKind,
) -> TaskDecl {
    let consts = ctx.consts;
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
                decl.inputs = ctx.dataset_str_list(&kw.value);
                if ctx.dataset_list_has_unresolved(&kw.value) {
                    decl.unresolved.push(key.clone());
                }
            }
            "outputs" => {
                decl.outputs = ctx.dataset_str_list(&kw.value);
                if ctx.dataset_list_has_unresolved(&kw.value) {
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
        // Authored via @tw -> NOT a decorator-free discovery.
        assert!(!d.discovered);
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

    #[test]
    fn discovers_plain_airflow_operators_without_decorators() {
        // A plain Airflow DAG with NO @tw: SQL operator + PythonOperator callable.
        let src = r#"
from airflow import DAG
BRONZE_SQL = "INSERT INTO bronze_sales (event_id) SELECT raw_id FROM landing_sales"

def build_silver():
    bronze = pd.read_sql("SELECT * FROM bronze_sales", con=E)
    silver = pd.DataFrame()
    silver["event_id"]   = bronze["event_id"]
    silver["amount_usd"] = bronze["amount"] * 1.08
    silver.to_sql("silver_sales", con=E)

with DAG("medallion") as dag:
    bronze = PostgresOperator(task_id="bronze", sql=BRONZE_SQL)
    silver = PythonOperator(task_id="silver", python_callable=build_silver)
    bronze >> silver
"#;
        let m = extract_task_decls(src).unwrap();
        let names: Vec<&str> = m.tasks.iter().map(|t| t.task_name.as_str()).collect();
        assert!(
            names.contains(&"bronze") && names.contains(&"silver"),
            "{names:?}"
        );

        let bronze = m.tasks.iter().find(|t| t.task_name == "bronze").unwrap();
        assert_eq!(bronze.engine.as_deref(), Some("sql"));
        assert_eq!(bronze.inputs, vec!["landing_sales"]);
        assert_eq!(bronze.outputs, vec!["bronze_sales"]);

        let silver = m.tasks.iter().find(|t| t.task_name == "silver").unwrap();
        assert_eq!(silver.engine.as_deref(), Some("pandas"));
        assert_eq!(silver.inputs, vec!["bronze_sales"]);
        assert_eq!(silver.outputs, vec!["silver_sales"]);
        assert!(silver
            .inferred_columns
            .iter()
            .any(|c| c.target == "amount_usd"));

        // Both were recovered decorator-free, so the flag is set — this is what
        // drives the inferred job/edge/dataset origin in `scan_decl`.
        assert!(bronze.discovered && silver.discovered);
    }

    #[test]
    fn lineage_recognised_under_all_import_forms() {
        // The four documented import forms must all be recognised as one lineage
        // task each: from-import, from-import-as, module + attr, module-as + attr.
        let forms = [
            r#"
from traceweaver import lineage
@lineage(inputs=["s3://b/in"], outputs=["iceberg://w.db.out"])
def f():
    pass
"#,
            r#"
from traceweaver import lineage as track
@track(inputs=["s3://b/in"], outputs=["iceberg://w.db.out"])
def f():
    pass
"#,
            r#"
import traceweaver
@traceweaver.lineage(inputs=["s3://b/in"], outputs=["iceberg://w.db.out"])
def f():
    pass
"#,
            r#"
import traceweaver as tw
@tw.lineage(inputs=["s3://b/in"], outputs=["iceberg://w.db.out"])
def f():
    pass
"#,
        ];
        for (i, src) in forms.iter().enumerate() {
            let m = extract_task_decls(src).unwrap();
            assert_eq!(m.tasks.len(), 1, "form {i}: {src}");
            let d = &m.tasks[0];
            assert!(d.lineage, "form {i} should be a lineage decl");
            assert_eq!(d.inputs, vec!["s3://b/in"], "form {i}");
            assert_eq!(d.outputs, vec!["iceberg://w.db.out"], "form {i}");
            assert!(d.nonliteral.is_empty(), "form {i}: all literal");
        }
    }

    #[test]
    fn bare_lineage_marks_with_no_datasets() {
        let src = r#"
from traceweaver import lineage
@lineage
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.tasks.len(), 1);
        let d = &m.tasks[0];
        assert!(d.lineage);
        assert!(d.inputs.is_empty() && d.outputs.is_empty());
    }

    #[test]
    fn lineage_name_and_description_and_templates() {
        // name= overrides the task name; a string literal WITH a {placeholder}
        // is still a literal template (high confidence, not flagged non-literal).
        let src = r#"
from traceweaver import lineage
@lineage(
    inputs=["s3://raw/sales/{date}.parquet"],
    outputs=["iceberg://warehouse.sales.bronze"],
    name="ingest_sales",
    description="daily ingest",
)
def build():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.task_name, "ingest_sales");
        assert_eq!(d.description.as_deref(), Some("daily ingest"));
        assert_eq!(d.inputs, vec!["s3://raw/sales/{date}.parquet"]);
        assert!(
            d.nonliteral.is_empty(),
            "a templated literal is still literal"
        );
    }

    #[test]
    fn lineage_keeps_non_literal_entries_as_text() {
        // A non-literal entry (f-string) is KEPT as best-effort source text and
        // recorded as non-literal; a sibling literal stays literal.
        let src = r#"
from traceweaver import lineage
DAY = "2024-01-01"
@lineage(
    inputs=[f"s3://raw/{DAY}.parquet", "s3://raw/static.parquet"],
    outputs=["iceberg://w.db.t"],
)
def build():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs.len(), 2);
        assert_eq!(d.inputs[1], "s3://raw/static.parquet");
        // The f-string is kept verbatim as source text and flagged non-literal.
        assert!(d.inputs[0].contains("f\"s3://raw/"));
        assert!(d.nonliteral.contains(&d.inputs[0]));
        assert!(!d.nonliteral.contains(&d.inputs[1]));
    }

    /// A `ConstTable` with one module's constants, for cross-module tests.
    fn table(module: &str, pairs: &[(&str, &str)]) -> ConstTable {
        let mut t = ConstTable::default();
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|&(k, v)| (k.to_string(), v.to_string()))
            .collect();
        t.insert_module(module, m);
        t
    }

    #[test]
    fn lineage_resolves_bare_name_in_same_module() {
        // Form (a): a bare Name defined in the SAME module resolves to its literal
        // and stays declared (NOT recorded as non-literal).
        let src = r#"
from trace_weaver import lineage
RAW = "s3://raw/sales.parquet"
OUT = "iceberg://w.db.bronze"
@lineage(inputs=[RAW], outputs=[OUT])
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert_eq!(d.outputs, vec!["iceberg://w.db.bronze"]);
        assert!(d.nonliteral.is_empty(), "resolved constants are declared");
    }

    #[test]
    fn lineage_resolves_from_import_constant() {
        // Form (b): `from config.datasets import RAW_SALES`.
        let src = r#"
from trace_weaver import lineage
from config.datasets import RAW_SALES, BRONZE
@lineage(inputs=[RAW_SALES], outputs=[BRONZE])
def f():
    pass
"#;
        let t = table(
            "config.datasets",
            &[
                ("RAW_SALES", "s3://raw/sales.parquet"),
                ("BRONZE", "iceberg://w.db.bronze"),
            ],
        );
        let m = extract_task_decls_with(src, "services.x", &t).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert_eq!(d.outputs, vec!["iceberg://w.db.bronze"]);
        assert!(d.nonliteral.is_empty());
    }

    #[test]
    fn lineage_resolves_from_import_alias() {
        // Form (b) with `as`: `from config.datasets import RAW_SALES as RAW`.
        let src = r#"
from trace_weaver import lineage
from config.datasets import RAW_SALES as RAW
@lineage(inputs=[RAW], outputs=["iceberg://w.db.bronze"])
def f():
    pass
"#;
        let t = table(
            "config.datasets",
            &[("RAW_SALES", "s3://raw/sales.parquet")],
        );
        let m = extract_task_decls_with(src, "services.x", &t).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert!(d.nonliteral.is_empty());
    }

    #[test]
    fn lineage_resolves_import_module_alias_attribute() {
        // Form (c): `import config.datasets as ds` + `ds.RAW_SALES`.
        let src = r#"
from trace_weaver import lineage
import config.datasets as ds
@lineage(inputs=[ds.RAW_SALES], outputs=[ds.BRONZE])
def f():
    pass
"#;
        let t = table(
            "config.datasets",
            &[
                ("RAW_SALES", "s3://raw/sales.parquet"),
                ("BRONZE", "iceberg://w.db.bronze"),
            ],
        );
        let m = extract_task_decls_with(src, "services.x", &t).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert_eq!(d.outputs, vec!["iceberg://w.db.bronze"]);
        assert!(d.nonliteral.is_empty());
    }

    #[test]
    fn lineage_resolves_import_dotted_attribute_without_alias() {
        // Form (c) without `as`: `import config.datasets` + `config.datasets.RAW_SALES`.
        let src = r#"
from trace_weaver import lineage
import config.datasets
@lineage(inputs=[config.datasets.RAW_SALES], outputs=["iceberg://w.db.bronze"])
def f():
    pass
"#;
        let t = table(
            "config.datasets",
            &[("RAW_SALES", "s3://raw/sales.parquet")],
        );
        let m = extract_task_decls_with(src, "services.x", &t).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert!(d.nonliteral.is_empty());
    }

    #[test]
    fn one_level_constant_aliasing_resolves() {
        // `NAME = OTHER_NAME` aliasing (with a further hop) collapses to the literal.
        let src = r#"
from trace_weaver import lineage
BASE = "s3://raw/sales.parquet"
MID = BASE
RAW = MID
@lineage(inputs=[RAW], outputs=["iceberg://w.db.bronze"])
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["s3://raw/sales.parquet"]);
        assert!(d.nonliteral.is_empty());
    }

    #[test]
    fn constant_alias_cycle_does_not_hang_or_resolve() {
        // A cyclic alias (A = B; B = A) resolves to nothing and is kept as text.
        let src = r#"
from trace_weaver import lineage
A = B
B = A
@lineage(inputs=[A], outputs=["iceberg://w.db.bronze"])
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        let d = &m.tasks[0];
        // Unresolved => kept verbatim and flagged non-literal (medium).
        assert_eq!(d.inputs[0], "A");
        assert!(d.nonliteral.contains("A"));
    }

    #[test]
    fn unsupported_forms_fall_back_to_nonliteral_text() {
        // Each unsupported reference is kept verbatim and flagged non-literal:
        // an unknown name, a function call, an f-string, and a subscript.
        let src = r#"
from trace_weaver import lineage
KNOWN = "s3://raw/known.parquet"
DAY = "2024-01-01"
CFG = {"raw": "s3://x"}
@lineage(
    inputs=[
        MISSING,
        make_uri(),
        f"s3://raw/{DAY}.parquet",
        CFG["raw"],
        KNOWN,
    ],
    outputs=["iceberg://w.db.bronze"],
)
def f():
    pass
"#;
        // Table has no matching module, so cross-module lookups also miss.
        let m = extract_task_decls_with(src, "services.x", &ConstTable::default()).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs.len(), 5);
        // Only the last (literal constant) resolved; the four others are text.
        assert_eq!(d.inputs[4], "s3://raw/known.parquet");
        assert!(!d.nonliteral.contains(&d.inputs[4]));
        assert!(d.nonliteral.contains("MISSING"));
        assert!(d.nonliteral.contains("make_uri()"));
        assert!(d.nonliteral.iter().any(|s| s.contains("f\"s3://raw/")));
        assert!(d.nonliteral.iter().any(|s| s.contains("CFG[")));
    }

    #[test]
    fn tw_task_inputs_resolve_from_import_without_flag() {
        // @tw.task shares the dataset extraction path: a from-imported constant in
        // inputs=/outputs= resolves and is NOT flagged W_NON_LITERAL (unresolved).
        let src = r#"
import trace_weaver as tw
from config.tables import BRONZE, SILVER
@tw.task(inputs=[BRONZE], outputs=[SILVER], engine="pandas")
def build():
    pass
"#;
        let t = table(
            "config.tables",
            &[
                ("BRONZE", "svc.db.sch.bronze"),
                ("SILVER", "svc.db.sch.silver"),
            ],
        );
        let m = extract_task_decls_with(src, "dags.build", &t).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["svc.db.sch.bronze"]);
        assert_eq!(d.outputs, vec!["svc.db.sch.silver"]);
        assert!(
            d.unresolved.is_empty(),
            "resolved constants must not flag W_NON_LITERAL: {:?}",
            d.unresolved
        );
    }

    #[test]
    fn tw_task_unresolved_input_still_flagged() {
        // Contrast: an unknown name in @tw.task inputs is dropped and flagged.
        let src = r#"
import trace_weaver as tw
@tw.task(inputs=[MISSING, "svc.db.sch.ok"], outputs=["svc.db.sch.out"], engine="pandas")
def build():
    pass
"#;
        let m = extract_task_decls_with(src, "dags.build", &ConstTable::default()).unwrap();
        let d = &m.tasks[0];
        assert_eq!(d.inputs, vec!["svc.db.sch.ok"]);
        assert!(d.unresolved.contains(&"inputs".to_string()));
    }

    #[test]
    fn lineage_wins_over_colocated_airflow_task() {
        // Stacked with Airflow's @task (final segment "task"): the @lineage
        // declaration owns the task, so inputs/outputs come from @lineage.
        let src = r#"
from traceweaver import lineage
from airflow.decorators import task
@task
@lineage(inputs=["s3://b/in"], outputs=["s3://b/out"])
def f():
    pass
"#;
        let m = extract_task_decls(src).unwrap();
        assert_eq!(m.tasks.len(), 1);
        assert!(m.tasks[0].lineage);
        assert_eq!(m.tasks[0].outputs, vec!["s3://b/out"]);
    }
}
