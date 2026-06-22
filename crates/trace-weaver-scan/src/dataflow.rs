//! Static column-lineage extraction from a task's **function body** (pandas /
//! PySpark), reusing the AST `rustpython-parser` already produced.
//!
//! Where [`crate::sql`] reads the SQL a task runs, this reads the *DataFrame*
//! code a task runs and traces column → column flow without executing it:
//!
//! * **pandas** (mutate-in-place): `df = pd.read_sql("… FROM t")` binds `df` to a
//!   source table; `out["c"] = df["a"] * 2` records `c <- a`; `out.to_sql("u")`
//!   names the output.
//! * **PySpark** (immutable chain): `df = spark.read.table("t")`;
//!   `out = df.withColumn("c", col("a") + col("b"))` records `c <- a, b`;
//!   `out.write.saveAsTable("u")` names the output.
//!
//! Everything produced here is provenance
//! [`trace_weaver_core::OriginSource::InferredCode`] — a best-effort static guess,
//! never authoritative. Anything the analyzer cannot follow (a dynamic column
//! name, `.apply(...)`, a UDF, `merge`/`pivot`, …) is **not** silently dropped: it
//! is reported as an [`OpaqueNote`] so the caller can warn the engineer to declare
//! that column by hand.

use std::collections::HashMap;

use rustpython_parser::ast::{self, Expr, Ranged, Stmt};

use trace_weaver_core::TransformType;

use crate::python::{byte_to_line, callee_final_segment, const_str, resolve_str};

/// One column mapping inferred from code: `target <- sources`.
#[derive(Debug, Clone, PartialEq)]
pub struct InferredColumn {
    /// Source columns as `"table.col"` (when the frame's table is known) or bare
    /// `"col"` (resolves against the edge's single input downstream).
    pub sources: Vec<String>,
    pub target: String,
    /// Short human label — the source slice of the producing expression.
    pub function: Option<String>,
    pub transform_type: TransformType,
    /// Output table this column belongs to (from `to_sql`/`saveAsTable`), if known.
    pub output_table: Option<String>,
}

/// A spot the analyzer could not trace — surfaced as a `W_OPAQUE_COLUMN` warning.
#[derive(Debug, Clone, PartialEq)]
pub struct OpaqueNote {
    pub line: Option<u32>,
    pub detail: String,
}

/// Column lineage statically recovered from a function body.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DataflowResult {
    pub columns: Vec<InferredColumn>,
    pub opaque: Vec<OpaqueNote>,
}

/// A column reference with an optional owning table.
#[derive(Debug, Clone, PartialEq)]
struct ColRef {
    table: Option<String>,
    column: String,
}

impl ColRef {
    fn render(&self) -> String {
        match &self.table {
            Some(t) => format!("{t}.{}", self.column),
            None => self.column.clone(),
        }
    }
}

/// DataFrame method / accessor names that must NOT be read as columns when seen
/// as `df.<name>` attribute access.
const DF_METHODS: &[&str] = &[
    "to_sql",
    "to_parquet",
    "to_csv",
    "merge",
    "join",
    "groupby",
    "agg",
    "apply",
    "rename",
    "assign",
    "astype",
    "fillna",
    "dropna",
    "drop",
    "reset_index",
    "copy",
    "columns",
    "values",
    "loc",
    "iloc",
    "head",
    "tail",
    "map",
    "round",
    "str",
    "dt",
    "alias",
    "cast",
    "withColumn",
    "withColumnRenamed",
    "select",
    "selectExpr",
    "filter",
    "where",
    "write",
    "read",
    "sql",
    "table",
    "parquet",
    "load",
    "format",
    "mode",
    "saveAsTable",
    "insertInto",
    "distinct",
    "limit",
    "orderBy",
    "sort",
    "union",
    "pivot",
    "melt",
    "explode",
    "withColumnsRenamed",
];

/// Reader call segments that bind a variable to a *source* dataset.
const READERS: &[&str] = &[
    "read_sql",
    "read_sql_query",
    "read_sql_table",
    "read_parquet",
    "read_csv",
    "read_json",
    "read_table",
    "table",
    "sql",
    "parquet",
    "load",
    "json",
    "csv",
];

/// Spark/pandas chain operations that rewrite columns. (`groupby`/`agg` are
/// handled separately in `bind_call` because they share state across the chain.)
fn is_transform_op(seg: &str) -> bool {
    matches!(
        seg,
        "withColumn" | "withColumnRenamed" | "select" | "selectExpr" | "rename" | "assign"
    )
}

/// Chain operations that pass the frame through unchanged (column set preserved).
fn is_passthrough_op(seg: &str) -> bool {
    matches!(
        seg,
        "filter"
            | "where"
            | "orderBy"
            | "sort"
            | "limit"
            | "distinct"
            | "dropDuplicates"
            | "cache"
            | "persist"
            | "repartition"
            | "coalesce"
            | "alias"
            | "fillna"
            | "dropna"
            | "na"
    )
}

/// Operations that change columns in ways we deliberately do not guess (their
/// output columns are runtime-data-dependent, or the logic lives in a callable we
/// don't enter). Flagged W_OPAQUE_COLUMN — never silently dropped, never guessed.
fn is_opaque_op(seg: &str) -> bool {
    matches!(
        seg,
        "merge"
            | "join"
            | "apply"
            | "applymap"
            | "pivot"
            | "melt"
            | "explode"
            | "stack"
            | "unstack"
            | "transform"
            | "pipe"
            | "rdd"
    )
}

struct Analyzer<'a> {
    consts: &'a HashMap<String, String>,
    source: &'a str,
    /// var name -> source table (None when the frame's origin table is unknown).
    frames: HashMap<String, Option<String>>,
    /// (owner var, column) collected in source order.
    columns: Vec<(String, InferredColumn)>,
    /// var name -> output table it was written to (`to_sql`/`saveAsTable`).
    writes: HashMap<String, Option<String>>,
    opaque: Vec<OpaqueNote>,
    /// (loop var, literal value) while unrolling `for c in ["a","b"]: …`.
    loop_subst: Option<(String, String)>,
    /// (row-lambda param, its frame's table) while reading an inline
    /// `df.apply(lambda r: …, axis=1)` — `r["x"]`/`r.x` are columns of that table.
    row_alias: Option<(String, Option<String>)>,
}

/// Statically analyse a function `body` for column lineage.
pub fn analyze(body: &[Stmt], consts: &HashMap<String, String>, source: &str) -> DataflowResult {
    let mut a = Analyzer {
        consts,
        source,
        frames: HashMap::new(),
        columns: Vec::new(),
        writes: HashMap::new(),
        opaque: Vec::new(),
        loop_subst: None,
        row_alias: None,
    };
    a.block(body);
    a.finish()
}

impl<'a> Analyzer<'a> {
    fn line_of(&self, e: &impl Ranged) -> Option<u32> {
        Some(byte_to_line(self.source, e.range().start().to_usize()))
    }

    fn slice(&self, e: &impl Ranged) -> String {
        let (s, t) = (e.range().start().to_usize(), e.range().end().to_usize());
        let raw = self.source.get(s..t).unwrap_or("").trim();
        // Collapse internal whitespace/newlines for a compact one-line label.
        let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.chars().count() > 80 {
            let mut s: String = collapsed.chars().take(77).collect();
            s.push('…');
            s
        } else {
            collapsed
        }
    }

    fn block(&mut self, stmts: &[Stmt]) {
        for st in stmts {
            self.stmt(st);
        }
    }

    fn stmt(&mut self, st: &Stmt) {
        match st {
            Stmt::Assign(asg) if asg.targets.len() == 1 => {
                self.assign(&asg.targets[0], &asg.value);
            }
            Stmt::Expr(e) => {
                // Statement-position call: detect a write (df.to_sql / saveAsTable).
                self.detect_write(&e.value);
            }
            Stmt::With(w) => self.block(&w.body),
            Stmt::If(i) => {
                self.block(&i.body);
                self.block(&i.orelse);
            }
            Stmt::For(f) if self.block_assigns_columns(&f.body) => self.handle_for(f),
            _ => {}
        }
    }

    /// Unroll `for c in ["a", "b"]: out[c] = src[c] …` when the loop variable is a
    /// single name and the iterable is an inline list of string literals — each
    /// iteration becomes a literal-named column. Anything dynamic (a computed
    /// list, a tuple target, a nested loop) stays opaque.
    fn handle_for(&mut self, f: &ast::StmtFor) {
        let var = match f.target.as_ref() {
            Expr::Name(n) => n.id.to_string(),
            _ => return self.flag_opaque_loop(f),
        };
        let values = match string_list(&f.iter) {
            Some(v) => v,
            None => return self.flag_opaque_loop(f),
        };
        if self.loop_subst.is_some() {
            return self.flag_opaque_loop(f); // nested loop — don't unroll
        }
        for v in values {
            self.loop_subst = Some((var.clone(), v));
            self.block(&f.body);
        }
        self.loop_subst = None;
    }

    fn flag_opaque_loop(&mut self, f: &ast::StmtFor) {
        self.opaque.push(OpaqueNote {
            line: self.line_of(f),
            detail: "column assigned in a loop the analyzer can't unroll — declare it, \
                     or iterate a literal list: `for c in [\"a\", \"b\"]:`"
                .into(),
        });
    }

    /// Resolve a subscript key to a column name: a string literal, or the current
    /// unrolled loop variable substituted with its literal value.
    fn col_name(&self, e: &Expr) -> Option<String> {
        if let Some(s) = const_str(e) {
            return Some(s);
        }
        if let (Expr::Name(n), Some((var, val))) = (e, &self.loop_subst) {
            if n.id.as_str() == var {
                return Some(val.clone());
            }
        }
        None
    }

    /// Table binding for a Name used as a column-access base: a known DataFrame
    /// variable, or the active row-lambda parameter. `None` if it is neither
    /// (so it isn't treated as a column source). Outer `Option` = "is a frame/row";
    /// inner `Option<String>` = the table (unknown when the frame's origin is).
    fn base_table_of(&self, id: &str) -> Option<Option<String>> {
        if self.frames.contains_key(id) {
            return Some(self.frames.get(id).cloned().flatten());
        }
        if let Some((param, table)) = &self.row_alias {
            if param == id {
                return Some(table.clone());
            }
        }
        None
    }

    /// Handle `target = value`.
    fn assign(&mut self, target: &Expr, value: &Expr) {
        match target {
            // out["c"] = expr   (pandas column assignment)
            Expr::Subscript(s) => {
                if let Expr::Name(owner) = s.value.as_ref() {
                    let owner = owner.id.to_string();
                    match self.col_name(&s.slice) {
                        Some(col) => self.record_column(&owner, col, value),
                        None => self.opaque.push(OpaqueNote {
                            line: self.line_of(s),
                            detail: format!(
                                "`{}[…] = …` has a non-literal column name — declare it in column_map",
                                owner
                            ),
                        }),
                    }
                }
            }
            // out = <rhs>   (binding / derivation)
            Expr::Name(n) => self.bind(n.id.to_string(), value),
            // out.columns = [...]  — wholesale positional column rename; the new
            // names depend on the frame's current column order, which we don't track.
            Expr::Attribute(a) if a.attr.as_str() == "columns" => {
                if let Expr::Name(owner) = a.value.as_ref() {
                    if self.frames.contains_key(owner.id.as_str()) {
                        self.opaque.push(OpaqueNote {
                            line: self.line_of(a),
                            detail: format!(
                                "`{}.columns = …` rewrites columns positionally — declare them in column_map",
                                owner.id
                            ),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    /// Record `owner[col] = value` as one inferred column.
    fn record_column(&mut self, owner: &str, col: String, value: &Expr) {
        let owner_table = self.frames.get(owner).cloned().flatten();
        let mut srcs = Vec::new();
        // Inline row lambda: out["c"] = df.apply(lambda r: r["a"] + r["b"], axis=1).
        // A named-callable apply (.apply(my_udf)) yields nothing -> opaque below.
        if !self.collect_apply_lambda(value, &mut srcs) {
            self.collect_cols(value, &mut srcs);
        }
        // A bare reference to a column whose frame we don't know resolves to the
        // single input downstream; that is fine. But if there are NO sources at
        // all and the RHS isn't a literal, we couldn't trace it.
        if srcs.is_empty() && !is_literalish(value) {
            self.opaque.push(OpaqueNote {
                line: self.line_of(value),
                detail: format!(
                    "couldn't trace sources of `{}[\"{}\"]` — declare it in column_map",
                    owner, col
                ),
            });
            return;
        }
        let tt = self.classify(value, &srcs);
        self.columns.push((
            owner.to_string(),
            InferredColumn {
                sources: srcs.iter().map(ColRef::render).collect(),
                target: col,
                function: Some(self.slice(value)),
                transform_type: tt,
                output_table: owner_table, // tentative; overwritten by write target
            },
        ));
    }

    /// If `value` is `<frame>.apply(lambda r: …, axis=1)`, bind the row parameter
    /// to the frame's table and collect the lambda body's column refs into `out`,
    /// returning `true`. A named-callable apply (`.apply(my_udf)`) returns `true`
    /// but collects nothing, so the caller flags it opaque. Returns `false` when
    /// `value` is not an `.apply(...)` call (caller falls back to normal collection).
    fn collect_apply_lambda(&mut self, value: &Expr, out: &mut Vec<ColRef>) -> bool {
        let c = match value {
            Expr::Call(c) => c,
            _ => return false,
        };
        if callee_final_segment(&c.func).as_deref() != Some("apply") {
            return false;
        }
        let recv = match c.func.as_ref() {
            Expr::Attribute(a) => a.value.as_ref(),
            _ => return false,
        };
        let recv_table = innermost_name(recv).and_then(|v| self.frames.get(v).cloned().flatten());
        if let Some(Expr::Lambda(lam)) = c.args.first() {
            if let Some(param) = lambda_single_param(lam) {
                self.row_alias = Some((param, recv_table));
                self.collect_cols(lam.body.as_ref(), out);
                self.row_alias = None;
            }
        }
        true
    }

    /// Handle `var = <rhs>` — bind the variable to a frame and process chains.
    fn bind(&mut self, var: String, rhs: &Expr) {
        // Plain alias: `out = src`
        if let Expr::Name(src) = rhs {
            let t = self.frames.get(src.id.as_str()).cloned().flatten();
            self.frames.insert(var, t);
            return;
        }
        // `out = src[[ "a", "b" ]]`  -> column subset (identity passthrough)
        if let Expr::Subscript(s) = rhs {
            if let Expr::Name(src) = s.value.as_ref() {
                let base = self.frames.get(src.id.as_str()).cloned().flatten();
                if let Some(cols) = string_list(&s.slice) {
                    for c in cols {
                        self.columns.push((
                            var.clone(),
                            InferredColumn {
                                sources: vec![ColRef {
                                    table: base.clone(),
                                    column: c.clone(),
                                }
                                .render()],
                                target: c,
                                function: None,
                                transform_type: TransformType::Identity,
                                output_table: None,
                            },
                        ));
                    }
                    self.frames.insert(var, base);
                    return;
                }
            }
        }
        if let Expr::Call(_) = rhs {
            self.bind_call(var, rhs);
            return;
        }
        // Unknown RHS shape: treat as an unknown frame.
        self.frames.insert(var, None);
    }

    /// Handle `var = <call-chain>` (readers + transform chains).
    fn bind_call(&mut self, var: String, rhs: &Expr) {
        let (mut segs, base) = unwind(rhs);
        segs.reverse(); // source order: innermost first

        // Resolve the base table: a frame variable, or an innermost reader call.
        let mut base_table = match base {
            Expr::Name(n) => self.frames.get(n.id.as_str()).cloned().flatten(),
            _ => None,
        };
        // If the innermost segment is a reader (e.g. spark.read.table("t")), the
        // base is that reader, not a transform op.
        let mut start = 0;
        if let Some((seg, call)) = segs.first() {
            if READERS.contains(seg) {
                base_table = self.reader_table(seg, call);
                start = 1;
            }
        }

        // Process the remaining ops in source order. `group_keys` carries the
        // grouping columns from `.groupby(...)` to the following `.agg(...)`.
        let mut group_keys: Vec<String> = Vec::new();
        for (seg, call) in &segs[start..] {
            match *seg {
                "groupby" | "groupBy" => {
                    self.collect_group_keys(&var, call, &base_table, &mut group_keys)
                }
                "agg" => self.apply_agg(&var, call, &base_table),
                s if is_transform_op(s) => self.apply_op(&var, s, call, &base_table),
                s if is_passthrough_op(s) => {} // column set unchanged
                s if is_opaque_op(s) => self.opaque.push(OpaqueNote {
                    line: self.line_of(*call),
                    detail: format!(
                        "`.{s}(…)` can't be traced statically — declare the columns it produces"
                    ),
                }),
                _ => {} // unknown segs: frame passes through
            }
        }
        self.frames.insert(var, base_table);
    }

    /// `.groupby("k")` / `.groupBy("k", col("k2"))` — record group keys and emit
    /// them as identity output columns (they survive the aggregation).
    fn collect_group_keys(
        &mut self,
        owner: &str,
        call: &ast::ExprCall,
        base_table: &Option<String>,
        keys: &mut Vec<String>,
    ) {
        let mut names = Vec::new();
        for arg in &call.args {
            if let Some(s) = const_str(arg) {
                names.push(s);
            } else {
                let mut c = Vec::new();
                self.collect_cols(arg, &mut c);
                names.extend(c.into_iter().map(|cr| cr.column));
            }
        }
        for k in names {
            let srcs = vec![ColRef {
                table: base_table.clone(),
                column: k.clone(),
            }];
            self.push_op_col(
                owner,
                k.clone(),
                &srcs,
                base_table,
                TransformType::Identity,
                None,
            );
            keys.push(k);
        }
    }

    /// `.agg(...)` — pandas dict `{"amount": "sum"}`, pandas named-agg kwargs
    /// `total=("amount", "sum")`, or Spark positional `F.sum(col("a")).alias("t")`.
    fn apply_agg(&mut self, owner: &str, call: &ast::ExprCall, base_table: &Option<String>) {
        // pandas: agg({"amount": "sum", "qty": "max"})
        if let Some(Expr::Dict(d)) = call.args.first() {
            for (k, v) in d.keys.iter().zip(d.values.iter()) {
                if let Some(colname) = k.as_ref().and_then(const_str) {
                    let func = const_str(v).unwrap_or_else(|| "agg".into());
                    let srcs = vec![ColRef {
                        table: base_table.clone(),
                        column: colname.clone(),
                    }];
                    self.push_op_col(
                        owner,
                        colname,
                        &srcs,
                        base_table,
                        TransformType::Aggregation,
                        Some(func),
                    );
                }
            }
        }
        // pandas named agg: agg(total=("amount", "sum"))
        for kw in &call.keywords {
            if let (Some(name), Expr::Tuple(t)) = (&kw.arg, &kw.value) {
                if let Some(srccol) = t.elts.first().and_then(const_str) {
                    let func = t
                        .elts
                        .get(1)
                        .and_then(const_str)
                        .unwrap_or_else(|| "agg".into());
                    let srcs = vec![ColRef {
                        table: base_table.clone(),
                        column: srccol,
                    }];
                    self.push_op_col(
                        owner,
                        name.to_string(),
                        &srcs,
                        base_table,
                        TransformType::Aggregation,
                        Some(func),
                    );
                }
            }
        }
        // Spark: agg(F.sum(col("amount")).alias("total"), ...)
        for arg in &call.args {
            if let Expr::Call(c) = arg {
                if callee_final_segment(&c.func).as_deref() == Some("alias") {
                    if let (Expr::Attribute(a), Some(target)) =
                        (c.func.as_ref(), c.args.first().and_then(const_str))
                    {
                        let mut srcs = Vec::new();
                        self.collect_cols(a.value.as_ref(), &mut srcs);
                        let label = self.slice(a.value.as_ref());
                        self.push_op_col(
                            owner,
                            target,
                            &srcs,
                            base_table,
                            TransformType::Aggregation,
                            Some(label),
                        );
                    }
                } else {
                    // A bare aggregate without .alias() gets a runtime-generated
                    // name (e.g. "sum(amount)") we cannot reproduce — flag it.
                    self.opaque.push(OpaqueNote {
                        line: self.line_of(c),
                        detail:
                            "aggregate without .alias() — name it (.alias(\"x\")) or declare it"
                                .into(),
                    });
                }
            }
        }
    }

    /// Resolve the source table named by a reader call.
    fn reader_table(&self, seg: &str, call: &ast::ExprCall) -> Option<String> {
        let arg0 = call.args.first();
        match seg {
            // SQL-bearing readers: parse the query for its source table.
            "read_sql" | "read_sql_query" | "sql" => arg0
                .and_then(|a| resolve_str(a, self.consts))
                .and_then(|q| crate::sql::column_lineage_from_sql(&q).ok())
                .and_then(|l| l.source_tables.into_iter().next()),
            // Table-name readers.
            "read_sql_table" | "read_table" | "table" => {
                arg0.and_then(|a| resolve_str(a, self.consts))
            }
            // Path readers — the dataset is a path, not a table name; leave unknown.
            _ => None,
        }
    }

    /// Apply one transform op (`withColumn`, `select`, `rename`, `assign`, …).
    fn apply_op(
        &mut self,
        owner: &str,
        seg: &str,
        call: &ast::ExprCall,
        base_table: &Option<String>,
    ) {
        match seg {
            "withColumn" => {
                if let (Some(tgt), Some(expr)) =
                    (call.args.first().and_then(const_str), call.args.get(1))
                {
                    let mut srcs = Vec::new();
                    self.collect_cols(expr, &mut srcs);
                    if srcs.is_empty() && !is_literalish(expr) {
                        self.opaque.push(OpaqueNote {
                            line: self.line_of(call),
                            detail: format!(
                                "couldn't trace sources of withColumn(\"{tgt}\", …) — declare it in column_map"
                            ),
                        });
                    } else {
                        let tt = self.classify(expr, &srcs);
                        let label = self.slice(expr);
                        self.push_op_col(owner, tgt, &srcs, base_table, tt, Some(label));
                    }
                }
            }
            "selectExpr" => {
                // Spark SQL-string projection, e.g. selectExpr("amount * 1.08 AS amt").
                for arg in &call.args {
                    if let Some(frag) = const_str(arg) {
                        let mappings = parse_sql_fragment(&frag);
                        if mappings.is_empty() {
                            self.opaque.push(OpaqueNote {
                                line: self.line_of(call),
                                detail: format!(
                                    "selectExpr(\"{frag}\") couldn't be parsed — declare it in column_map"
                                ),
                            });
                        }
                        for (srcs, tt, target) in mappings {
                            if !target.is_empty() {
                                self.push_op_col(
                                    owner,
                                    target,
                                    &srcs,
                                    base_table,
                                    tt,
                                    Some(frag.clone()),
                                );
                            }
                        }
                    }
                }
            }
            "withColumnRenamed" => {
                if let (Some(from), Some(to)) = (
                    call.args.first().and_then(const_str),
                    call.args.get(1).and_then(const_str),
                ) {
                    let srcs = vec![ColRef {
                        table: base_table.clone(),
                        column: from,
                    }];
                    self.push_op_col(
                        owner,
                        to,
                        &srcs,
                        base_table,
                        TransformType::Identity,
                        Some("rename".into()),
                    );
                }
            }
            "rename" => {
                // pandas: rename(columns={"a": "b"})
                for kw in &call.keywords {
                    if kw.arg.as_ref().map(|i| i.as_str()) == Some("columns") {
                        if let Expr::Dict(d) = &kw.value {
                            for (k, v) in d.keys.iter().zip(d.values.iter()) {
                                if let (Some(from), Some(to)) =
                                    (k.as_ref().and_then(const_str), const_str(v))
                                {
                                    let srcs = vec![ColRef {
                                        table: base_table.clone(),
                                        column: from,
                                    }];
                                    self.push_op_col(
                                        owner,
                                        to,
                                        &srcs,
                                        base_table,
                                        TransformType::Identity,
                                        Some("rename".into()),
                                    );
                                }
                            }
                        } else {
                            self.opaque.push(OpaqueNote {
                                line: self.line_of(call),
                                detail: "rename(columns=…) is not a literal dict — declare the renames in column_map".into(),
                            });
                        }
                    }
                }
            }
            "assign" => {
                // pandas: assign(c=expr, ...)
                for kw in &call.keywords {
                    if let Some(name) = &kw.arg {
                        let mut srcs = Vec::new();
                        self.collect_cols(&kw.value, &mut srcs);
                        let tt = self.classify(&kw.value, &srcs);
                        self.push_op_col(
                            owner,
                            name.to_string(),
                            &srcs,
                            base_table,
                            tt,
                            Some(self.slice(&kw.value)),
                        );
                    }
                }
            }
            "select" => {
                for arg in &call.args {
                    self.select_item(owner, arg, base_table);
                }
            }
            _ => {}
        }
    }

    /// One `select(...)` argument: `"a"`, `col("a")`, or `col("a").alias("c")`.
    fn select_item(&mut self, owner: &str, arg: &Expr, base_table: &Option<String>) {
        // String column name -> identity passthrough.
        if let Some(name) = const_str(arg) {
            let srcs = vec![ColRef {
                table: base_table.clone(),
                column: name.clone(),
            }];
            self.push_op_col(
                owner,
                name,
                &srcs,
                base_table,
                TransformType::Identity,
                None,
            );
            return;
        }
        // `<expr>.alias("c")` -> rename/derive into c.
        if let Expr::Call(c) = arg {
            if callee_final_segment(&c.func).as_deref() == Some("alias") {
                if let (Expr::Attribute(a), Some(target)) =
                    (c.func.as_ref(), c.args.first().and_then(const_str))
                {
                    let mut srcs = Vec::new();
                    self.collect_cols(&a.value, &mut srcs);
                    let tt = if is_pure_colref(&a.value) {
                        TransformType::Identity
                    } else {
                        TransformType::Transformation
                    };
                    let label = self.slice(a.value.as_ref());
                    self.push_op_col(owner, target, &srcs, base_table, tt, Some(label));
                    return;
                }
            }
        }
        // Bare `col("a")` -> identity a.
        let mut srcs = Vec::new();
        self.collect_cols(arg, &mut srcs);
        if srcs.len() == 1 {
            let c = srcs[0].column.clone();
            self.push_op_col(owner, c, &srcs, base_table, TransformType::Identity, None);
        }
    }

    /// Push a column produced by a chain op, defaulting unknown source tables to
    /// the chain's base table (Spark `col("x")` refers to the base frame).
    fn push_op_col(
        &mut self,
        owner: &str,
        target: String,
        srcs: &[ColRef],
        base_table: &Option<String>,
        tt: TransformType,
        function: Option<String>,
    ) {
        let sources: Vec<String> = srcs
            .iter()
            .map(|c| {
                let table = c.table.clone().or_else(|| base_table.clone());
                ColRef {
                    table,
                    column: c.column.clone(),
                }
                .render()
            })
            .collect();
        self.columns.push((
            owner.to_string(),
            InferredColumn {
                sources,
                target,
                function,
                transform_type: tt,
                output_table: None,
            },
        ));
    }

    /// Detect a write statement: `df.to_sql("t")`, `df.write.saveAsTable("t")`, …
    fn detect_write(&mut self, expr: &Expr) {
        let (segs, base) = unwind(expr);
        // segs are outer-first; the OUTERMOST segment is the write verb.
        let writer = segs.first();
        let (seg, call) = match writer {
            Some(x) => x,
            None => return,
        };
        let table = match *seg {
            "to_sql" | "saveAsTable" | "insertInto" => call.args.first().and_then(const_str),
            "to_parquet" | "to_csv" | "parquet" | "csv" | "save" => None, // path output
            _ => return,
        };
        // Resolve which frame variable is being written. The receiver may be
        // several attributes deep, e.g. `df.write.mode(...).saveAsTable(...)`, so
        // walk down to the innermost Name.
        if let Some(var) = innermost_name(base) {
            self.writes.insert(var.to_string(), table);
        }
    }

    /// Collect column references inside an expression.
    fn collect_cols(&self, expr: &Expr, out: &mut Vec<ColRef>) {
        match expr {
            Expr::Subscript(s) => {
                if let Expr::Name(n) = s.value.as_ref() {
                    if let Some(table) = self.base_table_of(n.id.as_str()) {
                        if let Some(col) = self.col_name(&s.slice) {
                            out.push(ColRef { table, column: col });
                            return;
                        }
                    }
                }
                self.collect_cols(&s.value, out);
            }
            Expr::Attribute(a) => {
                if let Expr::Name(n) = a.value.as_ref() {
                    if !DF_METHODS.contains(&a.attr.as_str()) {
                        if let Some(table) = self.base_table_of(n.id.as_str()) {
                            out.push(ColRef {
                                table,
                                column: a.attr.to_string(),
                            });
                            return;
                        }
                    }
                }
                self.collect_cols(&a.value, out);
            }
            Expr::Call(c) => {
                // Spark `col("x")` / `F.col("x")` / `column("x")`.
                if let Some(seg) = callee_final_segment(&c.func) {
                    if (seg == "col" || seg == "column") && c.args.len() == 1 {
                        if let Some(name) = const_str(&c.args[0]) {
                            out.push(ColRef {
                                table: None,
                                column: name,
                            });
                            return;
                        }
                    }
                    // `expr("a + b")` — parse the SQL fragment for its columns. We
                    // only need its SOURCES here, so force a throwaway alias so the
                    // SQL analyzer always yields a mapping (a bare expression has no
                    // target name of its own).
                    if seg == "expr" && c.args.len() == 1 {
                        if let Some(frag) = const_str(&c.args[0]) {
                            for (srcs, _, _) in parse_sql_fragment(&format!("{frag} AS __tw_expr"))
                            {
                                out.extend(srcs);
                            }
                            return;
                        }
                    }
                }
                // Method call: collect from the receiver (e.g. df["a"].astype(int)).
                if let Expr::Attribute(a) = c.func.as_ref() {
                    self.collect_cols(&a.value, out);
                }
                for arg in &c.args {
                    self.collect_cols(arg, out);
                }
                for kw in &c.keywords {
                    self.collect_cols(&kw.value, out);
                }
            }
            Expr::BinOp(b) => {
                self.collect_cols(&b.left, out);
                self.collect_cols(&b.right, out);
            }
            Expr::UnaryOp(u) => self.collect_cols(&u.operand, out),
            Expr::List(l) => {
                for e in &l.elts {
                    self.collect_cols(e, out);
                }
            }
            Expr::Tuple(t) => {
                for e in &t.elts {
                    self.collect_cols(e, out);
                }
            }
            _ => {}
        }
    }

    /// Identity if the RHS is a single pure column reference; aggregation if it
    /// looks like an aggregate; otherwise a transformation.
    fn classify(&self, expr: &Expr, srcs: &[ColRef]) -> TransformType {
        if is_pure_colref(expr) && srcs.len() == 1 {
            return TransformType::Identity;
        }
        let label = self.slice(expr).to_ascii_lowercase();
        if [
            "sum(", "mean(", "avg(", "count(", "min(", "max(", ".agg(", "groupby",
        ]
        .iter()
        .any(|kw| label.contains(kw))
        {
            return TransformType::Aggregation;
        }
        TransformType::Transformation
    }

    /// True if any statement in `body` assigns a frame column (`x[...] = ...`).
    fn block_assigns_columns(&self, body: &[Stmt]) -> bool {
        body.iter().any(|st| match st {
            Stmt::Assign(a) => a.targets.iter().any(|t| matches!(t, Expr::Subscript(_))),
            Stmt::If(i) => {
                self.block_assigns_columns(&i.body) || self.block_assigns_columns(&i.orelse)
            }
            Stmt::With(w) => self.block_assigns_columns(&w.body),
            Stmt::For(f) => self.block_assigns_columns(&f.body),
            _ => false,
        })
    }

    /// Finalise: attach the discovered output table to each column and drop the
    /// per-owner bookkeeping.
    fn finish(self) -> DataflowResult {
        let Analyzer {
            writes,
            columns,
            opaque,
            ..
        } = self;
        let cols = columns
            .into_iter()
            .map(|(owner, mut c)| {
                if let Some(t) = writes.get(&owner) {
                    c.output_table = t.clone();
                }
                c
            })
            .collect();
        DataflowResult {
            columns: cols,
            opaque,
        }
    }
}

/// Unwind a method-call chain into `[(segment, call), …]` (outer-first) and the
/// innermost receiver expression.
fn unwind(expr: &Expr) -> (Vec<(&str, &ast::ExprCall)>, &Expr) {
    let mut segs = Vec::new();
    let mut e = expr;
    loop {
        match e {
            Expr::Call(c) => {
                if let Expr::Attribute(a) = c.func.as_ref() {
                    segs.push((a.attr.as_str(), c));
                    e = a.value.as_ref();
                } else {
                    return (segs, e);
                }
            }
            _ => return (segs, e),
        }
    }
}

/// Parse a SQL-string fragment (a SELECT item, e.g. `"a + b AS c"`) into its
/// `(sources, transform_type, target)` tuples via the SQL analyzer (reusing
/// sql.rs). Empty when unparseable. Lets `selectExpr(...)` / `expr("...")` ride
/// the same engine as embedded SQL.
fn parse_sql_fragment(fragment: &str) -> Vec<(Vec<ColRef>, TransformType, String)> {
    crate::sql::select_expr_lineage(fragment)
        .into_iter()
        .map(|m| {
            let srcs = m
                .sources
                .into_iter()
                .map(|s| ColRef {
                    table: s.table,
                    column: s.column,
                })
                .collect();
            (srcs, m.transform_type, m.target)
        })
        .collect()
}

/// The single parameter name of a one-arg lambda (`lambda r: …` -> `"r"`).
fn lambda_single_param(lam: &ast::ExprLambda) -> Option<String> {
    lam.args.args.first().map(|a| a.def.arg.to_string())
}

/// Walk down a receiver chain (`a.b.c(...).d`) to its innermost variable name.
fn innermost_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Name(n) => Some(n.id.as_str()),
        Expr::Attribute(a) => innermost_name(a.value.as_ref()),
        Expr::Call(c) => innermost_name(c.func.as_ref()),
        Expr::Subscript(s) => innermost_name(s.value.as_ref()),
        _ => None,
    }
}

/// True if `expr` is a single, pure column reference (no operation around it).
fn is_pure_colref(expr: &Expr) -> bool {
    match expr {
        Expr::Subscript(_) | Expr::Attribute(_) => true,
        Expr::Call(c) => callee_final_segment(&c.func)
            .map(|s| s == "col" || s == "column")
            .unwrap_or(false),
        _ => false,
    }
}

/// A literal-ish RHS (constant / call to a literal) that legitimately has no
/// source columns, e.g. `out["const"] = 0`.
fn is_literalish(expr: &Expr) -> bool {
    matches!(expr, Expr::Constant(_))
}

/// A list/tuple of string literals, e.g. `["a", "b"]` in `df[["a","b"]]`.
fn string_list(expr: &Expr) -> Option<Vec<String>> {
    let elts = match expr {
        Expr::List(l) => &l.elts,
        Expr::Tuple(t) => &t.elts,
        _ => return None,
    };
    let cols: Vec<String> = elts.iter().filter_map(const_str).collect();
    if cols.len() == elts.len() && !cols.is_empty() {
        Some(cols)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustpython_parser::{ast::Suite, Parse};

    fn body_of(src: &str) -> Vec<Stmt> {
        // Wrap statements in a function and return its body.
        let suite = Suite::parse(src, "<t>").unwrap();
        match suite.into_iter().next().unwrap() {
            Stmt::FunctionDef(f) => f.body,
            _ => panic!("expected a function def"),
        }
    }

    fn run(src: &str) -> DataflowResult {
        let body = body_of(src);
        analyze(&body, &HashMap::new(), src)
    }

    fn col<'a>(r: &'a DataflowResult, target: &str) -> &'a InferredColumn {
        r.columns
            .iter()
            .find(|c| c.target == target)
            .expect("column")
    }

    #[test]
    fn pandas_build_silver_is_fully_traced() {
        // The exact function the user asked about — zero annotation.
        let src = r#"
def build_silver():
    bronze = pd.read_sql("SELECT * FROM bronze_sales", con=ENGINE)
    silver = pd.DataFrame()
    silver["event_id"]   = bronze["event_id"]
    silver["amount_usd"] = bronze["amount"] * 1.08
    silver.to_sql("silver_sales", con=ENGINE)
"#;
        let r = run(src);
        assert!(r.opaque.is_empty(), "should trace cleanly: {:?}", r.opaque);

        let event_id = col(&r, "event_id");
        assert_eq!(event_id.sources, vec!["bronze_sales.event_id"]);
        assert_eq!(event_id.transform_type, TransformType::Identity);
        assert_eq!(event_id.output_table.as_deref(), Some("silver_sales"));

        let amount_usd = col(&r, "amount_usd");
        assert_eq!(amount_usd.sources, vec!["bronze_sales.amount"]);
        assert_eq!(amount_usd.transform_type, TransformType::Transformation);
        assert_eq!(
            amount_usd.function.as_deref(),
            Some("bronze[\"amount\"] * 1.08")
        );
        assert_eq!(amount_usd.output_table.as_deref(), Some("silver_sales"));
    }

    #[test]
    fn spark_withcolumn_and_rename_traced() {
        let src = r#"
def build_silver():
    bronze = spark.read.table("bronze_sales")
    silver = bronze.withColumn("amount_usd", col("amount") * 1.08).withColumnRenamed("id", "event_id")
    silver.write.saveAsTable("silver_sales")
"#;
        let r = run(src);
        assert!(r.opaque.is_empty(), "clean: {:?}", r.opaque);

        let amount_usd = col(&r, "amount_usd");
        assert_eq!(amount_usd.sources, vec!["bronze_sales.amount"]);
        assert_eq!(amount_usd.transform_type, TransformType::Transformation);
        assert_eq!(amount_usd.output_table.as_deref(), Some("silver_sales"));

        let event_id = col(&r, "event_id");
        assert_eq!(event_id.sources, vec!["bronze_sales.id"]);
        assert_eq!(event_id.transform_type, TransformType::Identity);
        assert_eq!(event_id.output_table.as_deref(), Some("silver_sales"));
    }

    #[test]
    fn spark_select_with_alias() {
        let src = r#"
def f():
    b = spark.table("bronze")
    s = b.select("event_id", col("amount").alias("amt"))
    s.write.saveAsTable("silver")
"#;
        let r = run(src);
        assert_eq!(col(&r, "event_id").sources, vec!["bronze.event_id"]);
        assert_eq!(col(&r, "event_id").transform_type, TransformType::Identity);
        assert_eq!(col(&r, "amt").sources, vec!["bronze.amount"]);
    }

    #[test]
    fn pandas_fan_in_transformation() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    out["total"] = b["x"] + b["y"]
    out.to_sql("gold")
"#;
        let r = run(src);
        let total = col(&r, "total");
        assert_eq!(total.sources, vec!["bronze.x", "bronze.y"]);
        assert_eq!(total.transform_type, TransformType::Transformation);
    }

    #[test]
    fn opaque_apply_is_flagged_not_dropped() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    out["score"] = b.apply(lambda r: secret(r), axis=1)
    out.to_sql("silver")
"#;
        let r = run(src);
        // `.apply` over the whole frame yields no resolvable source columns -> opaque.
        assert!(
            r.opaque.iter().any(|o| o.detail.contains("score")),
            "expected an opaque note for the apply: {:?}",
            r
        );
    }

    #[test]
    fn opaque_dynamic_column_name_is_flagged() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    out[target_col] = b["x"]
    out.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque
                .iter()
                .any(|o| o.detail.contains("non-literal column name")),
            "expected opaque for dynamic column: {:?}",
            r
        );
    }

    #[test]
    fn spark_selectexpr_and_expr_traced() {
        // SQL-string expressions ride sql.rs: selectExpr + withColumn(expr(...)).
        let src = r#"
def f():
    b = spark.read.table("bronze")
    s = b.selectExpr("event_id", "amount * 1.08 AS amount_usd")
    g = b.withColumn("amount_x2", expr("amount * 2"))
    s.write.saveAsTable("silver")
"#;
        let r = run(src);
        assert!(r.opaque.is_empty(), "clean: {:?}", r.opaque);
        let usd = col(&r, "amount_usd");
        assert_eq!(usd.sources, vec!["bronze.amount"]);
        assert_eq!(usd.transform_type, TransformType::Transformation);
        assert_eq!(col(&r, "event_id").transform_type, TransformType::Identity);
        assert_eq!(col(&r, "amount_x2").sources, vec!["bronze.amount"]);
    }

    #[test]
    fn pandas_groupby_agg_dict_traced() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM sales", con=E)
    g = b.groupby("region").agg({"amount": "sum", "qty": "max"})
    g.to_sql("region_totals")
"#;
        let r = run(src);
        assert_eq!(col(&r, "region").transform_type, TransformType::Identity);
        let amount = col(&r, "amount");
        assert_eq!(amount.transform_type, TransformType::Aggregation);
        assert_eq!(amount.sources, vec!["sales.amount"]);
        assert_eq!(amount.function.as_deref(), Some("sum"));
        assert_eq!(col(&r, "qty").transform_type, TransformType::Aggregation);
    }

    #[test]
    fn spark_groupby_agg_alias_traced() {
        let src = r#"
def f():
    b = spark.read.table("sales")
    g = b.groupBy("region").agg(F.sum(col("amount")).alias("total"))
    g.write.saveAsTable("gold")
"#;
        let r = run(src);
        assert_eq!(col(&r, "region").transform_type, TransformType::Identity);
        let total = col(&r, "total");
        assert_eq!(total.transform_type, TransformType::Aggregation);
        assert_eq!(total.sources, vec!["sales.amount"]);
    }

    #[test]
    fn withcolumn_sourceless_is_flagged_not_emitted() {
        let src = r#"
def f():
    b = spark.read.table("bronze")
    g = b.withColumn("score", rand())
    g.write.saveAsTable("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.iter().any(|o| o.detail.contains("score")),
            "expected opaque note for untraceable withColumn: {:?}",
            r
        );
        assert!(
            r.columns.iter().all(|c| c.target != "score"),
            "sourceless column must not be emitted"
        );
    }

    #[test]
    fn literal_loop_is_unrolled() {
        let src = r#"
def f():
    src = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    for c in ["event_id", "amount"]:
        out[c] = src[c]
    out.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.is_empty(),
            "loop should unroll cleanly: {:?}",
            r.opaque
        );
        assert_eq!(col(&r, "event_id").sources, vec!["bronze.event_id"]);
        assert_eq!(col(&r, "amount").sources, vec!["bronze.amount"]);
        assert_eq!(col(&r, "event_id").output_table.as_deref(), Some("silver"));
    }

    #[test]
    fn dynamic_loop_stays_opaque() {
        let src = r#"
def f():
    src = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    for c in runtime_cols():
        out[c] = src[c]
    out.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.iter().any(|o| o.detail.contains("can't unroll")),
            "dynamic loop must stay opaque: {:?}",
            r
        );
    }

    #[test]
    fn inline_row_lambda_is_traced() {
        // out["c"] = df.apply(lambda r: r["a"] + r["b"], axis=1) — read the body.
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    out["full"] = b.apply(lambda r: r["first"] + r["last"], axis=1)
    out["amt"]  = b.apply(lambda row: row["amount"] * 1.08, axis=1)
    out.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.is_empty(),
            "inline lambda should trace: {:?}",
            r.opaque
        );
        assert_eq!(col(&r, "full").sources, vec!["bronze.first", "bronze.last"]);
        assert_eq!(col(&r, "amt").sources, vec!["bronze.amount"]);
    }

    #[test]
    fn named_callable_apply_stays_opaque() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    out = pd.DataFrame()
    out["score"] = b.apply(secret_udf, axis=1)
    out.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.iter().any(|o| o.detail.contains("score")),
            "named-callable apply must stay opaque: {:?}",
            r
        );
        assert!(r.columns.iter().all(|c| c.target != "score"));
    }

    #[test]
    fn df_columns_assignment_is_flagged() {
        let src = r#"
def f():
    b = pd.read_sql("SELECT * FROM bronze", con=E)
    b.columns = ["x", "y"]
    b.to_sql("silver")
"#;
        let r = run(src);
        assert!(
            r.opaque.iter().any(|o| o.detail.contains(".columns")),
            "df.columns= must be flagged: {:?}",
            r
        );
    }
}
