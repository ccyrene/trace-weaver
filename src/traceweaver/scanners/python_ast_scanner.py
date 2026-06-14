from __future__ import annotations

import ast
import re
from pathlib import Path

from traceweaver.models import (
    Dataset,
    FunctionCall,
    LineageEdge,
    LineageJob,
    ScanResult,
    TaskDependency,
)
from traceweaver.scanners.datasets import connection_dataset, datasets_from_text
from traceweaver.scanners.sql_lineage import dialect_for_operator, extract_sql_lineage

# Operator-style call suffixes that we treat as Airflow tasks.
_TASK_SUFFIXES = ("Operator", "Sensor")

# Keyword arguments whose string value should be parsed as SQL.
_SQL_KEYS = ("sql", "query", "statement", "stmt")

# Keyword arguments whose string value should be scanned for dataset URIs.
_TEXT_KEYS = (
    "bash_command",
    "filename",
    "key",
    "bucket_key",
    "source_objects",
    "destination_object",
    "object_name",
    "remote_filepath",
    "path",
)


class PythonAstScanner:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = Path(repo_root)

    def scan_file(self, file_path: Path) -> ScanResult:
        rel_path = str(Path(file_path).relative_to(self.repo_root))
        source = Path(file_path).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=rel_path)
        module = self._module_name(file_path)

        visitor = _DagAstVisitor(file_path=rel_path, module=module)
        visitor.prescan(tree)
        visitor.visit(tree)

        result = ScanResult(repo_path=str(self.repo_root))
        result.jobs.extend(visitor.jobs)
        result.datasets.extend(visitor.datasets)
        result.edges.extend(visitor.edges)
        result.task_dependencies.extend(visitor.task_dependencies)

        # SQL lineage per task, using the operator's dialect when known.
        for (dag_id, task_id), statements in visitor.sql_by_task.items():
            for sql, operator_class in statements:
                dialect = dialect_for_operator(operator_class)
                datasets, edges, _backend = extract_sql_lineage(
                    sql, dag_id=dag_id, task_id=task_id, dialect=dialect
                )
                result.datasets.extend(datasets)
                result.edges.extend(edges)

        # SQL stored in external .sql files referenced by an operator
        # (e.g. PostgresOperator(sql="sql/load_orders.sql")).
        for (dag_id, task_id), refs in visitor.sql_files_by_task.items():
            for sql_path, operator_class in refs:
                content = self._read_repo_sql_file(sql_path, rel_path)
                if not content:
                    continue
                dialect = dialect_for_operator(operator_class)
                datasets, edges, _backend = extract_sql_lineage(
                    content, dag_id=dag_id, task_id=task_id, dialect=dialect
                )
                result.datasets.extend(datasets)
                result.edges.extend(edges)

        result.function_calls.extend(
            self._attribute_function_calls(visitor.function_calls, visitor.jobs)
        )

        result.dedupe()
        return result

    def _attribute_function_calls(
        self, calls: list[FunctionCall], jobs: list[LineageJob]
    ) -> list[FunctionCall]:
        """Tag function calls with the dag/task whose callable they run under.

        A call is attributed when its ``caller_function`` matches either a
        task's ``task_id`` (TaskFlow) or the final component of its
        ``callable_path`` (classic ``python_callable``).
        """
        # A callable_path binds an actual function body to a task, so it is
        # authoritative; fall back to a task_id match only when no callable
        # name matches (handles a task_id colliding with another task's
        # python_callable name).
        callable_map: dict[str, tuple[str, str]] = {}
        task_map: dict[str, tuple[str, str]] = {}
        for job in jobs:
            if job.callable_path:
                callable_map.setdefault(
                    job.callable_path.rsplit(".", 1)[-1], (job.dag_id, job.task_id)
                )
            task_map.setdefault(job.task_id, (job.dag_id, job.task_id))

        attributed: list[FunctionCall] = []
        for call in calls:
            caller = call.caller_function or ""
            owner = callable_map.get(caller) or task_map.get(caller)
            if owner and call.dag_id is None and call.task_id is None:
                dag_id, task_id = owner
                attributed.append(
                    FunctionCall(
                        function_name=call.function_name,
                        file_path=call.file_path,
                        line_no=call.line_no,
                        dag_id=dag_id,
                        task_id=task_id,
                        module=call.module,
                        caller_function=call.caller_function,
                        method=call.method,
                    )
                )
            else:
                attributed.append(call)
        return attributed

    def _read_repo_sql_file(self, sql_path: str, current_rel: str) -> str | None:
        """Read an external .sql file referenced from a DAG, if it exists.

        Tries the path relative to the repo root and to the referencing DAG
        file, then falls back to a basename search anywhere under the repo. All
        reads are confined to the repo tree (no path traversal).
        """
        repo_root = self.repo_root.resolve()
        rel = Path(sql_path)
        candidates = [repo_root / rel, (repo_root / current_rel).parent / rel]
        for candidate in candidates:
            text = self._read_if_within(candidate, repo_root)
            if text is not None:
                return text
        for match in sorted(repo_root.rglob(rel.name)):
            text = self._read_if_within(match, repo_root)
            if text is not None:
                return text
        return None

    @staticmethod
    def _read_if_within(path: Path, repo_root: Path) -> str | None:
        try:
            resolved = path.resolve()
        except OSError:
            return None
        if resolved != repo_root and repo_root not in resolved.parents:
            return None  # outside the scanned repo — refuse to read
        if not resolved.is_file():
            return None
        try:
            return resolved.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None

    def _module_name(self, file_path: Path) -> str:
        try:
            rel = file_path.relative_to(self.repo_root).with_suffix("")
            return ".".join(rel.parts)
        except ValueError:
            return file_path.stem


class _DagAstVisitor(ast.NodeVisitor):
    def __init__(self, file_path: str, module: str) -> None:
        self.file_path = file_path
        self.module = module
        self.dag_stack: list[str] = []
        self.current_function: str | None = None

        self.jobs: list[LineageJob] = []
        self.datasets: list[Dataset] = []
        self.edges: list[LineageEdge] = []
        self.task_dependencies: list[TaskDependency] = []
        self.function_calls: list[FunctionCall] = []

        # task_id keyed sql, with the operator class so we can pick a dialect.
        self.sql_by_task: dict[tuple[str, str], list[tuple[str, str | None]]] = {}
        # task_id keyed references to external ``.sql`` files (resolved + read
        # by PythonAstScanner, which knows the repo root).
        self.sql_files_by_task: dict[tuple[str, str], list[tuple[str, str | None]]] = {}

        # Resolution tables populated during the prescan.
        self.dag_var_to_id: dict[str, str] = {}
        self.all_dag_ids: list[str] = []

        self.var_to_task: dict[str, tuple[str, str]] = {}
        self.decorated_task_functions: dict[str, str] = {}  # function name -> task_id
        self.decorated_dag_functions: dict[str, str] = {}
        self.string_constants: dict[str, str] = {}
        # TaskFlow tasks that already produced a job (avoid double-counting the
        # definition and its invocation).
        self._taskflow_emitted: set[str] = set()

    # ------------------------------------------------------------------
    # Prescan: discover DAGs and string constants before resolving tasks.
    # ------------------------------------------------------------------
    def prescan(self, tree: ast.AST) -> None:
        seen: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                literal = self._literal_string(node.value)
                if literal is not None:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            self.string_constants[target.id] = literal
                if (
                    isinstance(node.value, ast.Call)
                    and self._call_name(node.value.func) == "DAG"
                ):
                    dag_id = self._extract_dag_id(node.value)
                    if dag_id:
                        seen.append(dag_id)
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                self.dag_var_to_id[target.id] = dag_id
            elif isinstance(node, ast.With):
                for item in node.items:
                    dag_id = self._dag_id_from_context_expr(item.context_expr)
                    if dag_id:
                        seen.append(dag_id)
                        if isinstance(item.optional_vars, ast.Name):
                            self.dag_var_to_id[item.optional_vars.id] = dag_id
            elif isinstance(node, ast.FunctionDef):
                dag_id = self._dag_id_from_decorators(
                    node.decorator_list, fallback=node.name
                )
                if dag_id:
                    seen.append(dag_id)
                    self.decorated_dag_functions[node.name] = dag_id
        # Preserve order, drop duplicates.
        self.all_dag_ids = list(dict.fromkeys(seen))

    # ------------------------------------------------------------------
    # Main visit pass.
    # ------------------------------------------------------------------
    def visit_With(self, node: ast.With) -> None:
        dag_ids = [
            d
            for item in node.items
            if (d := self._dag_id_from_context_expr(item.context_expr))
        ]
        if dag_ids:
            self.dag_stack.extend(dag_ids)
            for stmt in node.body:
                self.visit(stmt)
            for _ in dag_ids:
                self.dag_stack.pop()
        else:
            self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self._maybe_extract_task_call(node.value, node.targets)
        self._maybe_extract_dependency(node.value)
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> None:
        self._maybe_extract_task_call(node.value, [])
        self._maybe_extract_dependency(node.value)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        dag_id = self.decorated_dag_functions.get(node.name)
        if dag_id:
            self.dag_stack.append(dag_id)
            previous = self.current_function
            self.current_function = node.name
            for stmt in node.body:
                self.visit(stmt)
            self.current_function = previous
            self.dag_stack.pop()
            return

        if self._is_task_decorated(node.decorator_list):
            task_id = self._task_id_from_decorators(node.decorator_list, node.name)
            self.decorated_task_functions[node.name] = task_id
            if self.dag_stack:
                dag_id = self.dag_stack[-1]
                self.jobs.append(
                    LineageJob(
                        dag_id=dag_id,
                        task_id=task_id,
                        operator_class="TaskFlowTask",
                        callable_path=f"{self.module}.{node.name}",
                        file_path=self.file_path,
                        line_no=node.lineno,
                    )
                )
                self._taskflow_emitted.add(node.name)
                self.var_to_task.setdefault(node.name, (dag_id, task_id))

        previous = self.current_function
        self.current_function = node.name
        self.generic_visit(node)
        self.current_function = previous

    def visit_Call(self, node: ast.Call) -> None:
        call_name = self._call_name(node.func)
        if call_name:
            if self.current_function and not self._is_task_call(call_name):
                self.function_calls.append(
                    FunctionCall(
                        function_name=call_name,
                        caller_function=self.current_function,
                        module=self.module,
                        file_path=self.file_path,
                        line_no=getattr(node, "lineno", None),
                    )
                )

            # TaskFlow invocation: extract_orders().
            if call_name in self.decorated_task_functions and self.dag_stack:
                dag_id = self.dag_stack[-1]
                task_id = self.decorated_task_functions[call_name]
                if call_name not in self._taskflow_emitted:
                    self.jobs.append(
                        LineageJob(
                            dag_id=dag_id,
                            task_id=task_id,
                            operator_class="TaskFlowTask",
                            callable_path=f"{self.module}.{call_name}",
                            file_path=self.file_path,
                            line_no=getattr(node, "lineno", None),
                        )
                    )
                    self._taskflow_emitted.add(call_name)
                # Implicit XCom data dependency: a task receiving another task's
                # output as an argument depends on that producing task.
                self._extract_taskflow_data_deps(node, (dag_id, task_id))

        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self._record_datasets(node.value)
        self.generic_visit(node)

    # ------------------------------------------------------------------
    # Task extraction.
    # ------------------------------------------------------------------
    def _maybe_extract_task_call(self, value: ast.AST, targets: list[ast.expr]) -> None:
        if not isinstance(value, ast.Call):
            return

        # TaskFlow assignment: order = extract_orders()
        callee = self._call_name(value.func)
        if callee in self.decorated_task_functions:
            dag_id = self._resolve_dag_for_call(value)
            task_id = self.decorated_task_functions[callee]
            for target in targets:
                if isinstance(target, ast.Name):
                    self.var_to_task[target.id] = (dag_id, task_id)
            return

        operator_class = callee
        if not operator_class or not operator_class.endswith(_TASK_SUFFIXES):
            return

        task_id = self._get_keyword_string(value, "task_id")
        if not task_id:
            return

        dag_id = self._resolve_dag_for_call(value)
        callable_path = self._get_keyword_name(value, "python_callable")
        if callable_path and "." not in callable_path:
            callable_path = f"{self.module}.{callable_path}"

        self.jobs.append(
            LineageJob(
                dag_id=dag_id,
                task_id=task_id,
                operator_class=operator_class,
                callable_path=callable_path,
                file_path=self.file_path,
                line_no=getattr(value, "lineno", None),
            )
        )
        for target in targets:
            if isinstance(target, ast.Name):
                self.var_to_task[target.id] = (dag_id, task_id)

        self._collect_sql(value, dag_id, task_id, operator_class)
        self._collect_connection_ids(value, dag_id, task_id)
        self._collect_keyword_datasets(value, dag_id, task_id)

    def _resolve_dag_for_call(self, call: ast.Call) -> str:
        explicit = self._get_keyword_string(call, "dag_id")
        if explicit:
            return explicit
        dag_var = self._get_keyword_name(call, "dag")
        if dag_var and dag_var in self.dag_var_to_id:
            return self.dag_var_to_id[dag_var]
        if self.dag_stack:
            return self.dag_stack[-1]
        if len(self.all_dag_ids) == 1:
            return self.all_dag_ids[0]
        return "unknown_dag"

    def _collect_sql(
        self, call: ast.Call, dag_id: str, task_id: str, operator_class: str
    ) -> None:
        seen: set[str] = set()
        # Top-level operator keyword arguments (sql=, query=, ...).
        for key in _SQL_KEYS:
            value = self._get_keyword_string(call, key)
            if value:
                self._route_sql_value(value, dag_id, task_id, operator_class, seen)
        # SQL nested inside config dicts/lists, e.g. op_kwargs={"sql": ...} or
        # BigQueryInsertJobOperator(configuration={"query": {"query": ...}}).
        for node in [*call.args, *(kw.value for kw in call.keywords)]:
            if isinstance(node, (ast.Dict, ast.List, ast.Tuple)):
                for value in self._iter_dict_sql(node):
                    self._route_sql_value(value, dag_id, task_id, operator_class, seen)

    def _route_sql_value(
        self,
        value: str,
        dag_id: str,
        task_id: str,
        operator_class: str,
        seen: set[str],
    ) -> None:
        """Send a discovered string to inline-SQL or external-.sql-file buckets."""
        if value in seen:
            return
        if self._looks_like_sql(value):
            seen.add(value)
            self.sql_by_task.setdefault((dag_id, task_id), []).append(
                (value, operator_class)
            )
        elif self._is_sql_file_ref(value):
            seen.add(value)
            self.sql_files_by_task.setdefault((dag_id, task_id), []).append(
                (value.strip(), operator_class)
            )

    def _iter_dict_sql(self, node: ast.AST, depth: int = 0):
        """Yield string literals stored under SQL-named keys in nested dicts."""
        if depth > 5:
            return
        if isinstance(node, ast.Dict):
            for key_node, value_node in zip(node.keys, node.values):
                key = self._literal_string(key_node) if key_node is not None else None
                if key and key.lower() in _SQL_KEYS:
                    text = self._literal_string(value_node)
                    if text:
                        yield text
                yield from self._iter_dict_sql(value_node, depth + 1)
        elif isinstance(node, (ast.List, ast.Tuple)):
            for element in node.elts:
                yield from self._iter_dict_sql(element, depth + 1)

    @staticmethod
    def _is_sql_file_ref(value: str) -> bool:
        """True for a string that names an external ``.sql`` file to load."""
        token = value.strip()
        return (
            token.lower().endswith(".sql")
            and "\n" not in token
            and bool(re.fullmatch(r"[\w./\-]+", token))
        )

    def _collect_connection_ids(
        self, call: ast.Call, dag_id: str, task_id: str
    ) -> None:
        for kw in call.keywords:
            if not kw.arg:
                continue
            if kw.arg == "conn_id" or kw.arg.endswith("_conn_id"):
                conn = self._literal_string(kw.value)
                if conn:
                    self.datasets.append(connection_dataset(conn))
                    self.edges.append(
                        LineageEdge(
                            dag_id=dag_id,
                            task_id=task_id,
                            source_dataset=conn,
                            target_dataset=None,
                            extraction_method="conn_id",
                            confidence="low",
                        )
                    )

    def _collect_keyword_datasets(
        self, call: ast.Call, dag_id: str, task_id: str
    ) -> None:
        for kw in call.keywords:
            if kw.arg not in _TEXT_KEYS:
                continue
            text = self._literal_string(kw.value)
            if not text:
                continue
            for dataset in datasets_from_text(text):
                self.datasets.append(dataset)
                self.edges.append(
                    LineageEdge(
                        dag_id=dag_id,
                        task_id=task_id,
                        source_dataset=dataset.name,
                        target_dataset=None,
                        extraction_method="dataset_pattern",
                        confidence="low",
                    )
                )

    # ------------------------------------------------------------------
    # Dependency extraction.
    # ------------------------------------------------------------------
    def _maybe_extract_dependency(self, node: ast.AST) -> None:
        if isinstance(node, ast.BinOp) and isinstance(
            node.op, (ast.RShift, ast.LShift)
        ):
            pairs, _anchor = self._collect_shift(node)
            for upstream, downstream in pairs:
                self._add_dependency(upstream, downstream)
        elif isinstance(node, ast.Call):
            self._maybe_set_relation(node)

    def _collect_shift(self, node: ast.AST):
        """Return ``(pairs, anchor)`` for a chain of ``>>`` / ``<<`` operators.

        ``anchor`` is the resolved task set of the right-most operand, which is
        what Airflow returns and therefore what a longer chain continues from.
        """
        if isinstance(node, ast.BinOp) and isinstance(
            node.op, (ast.RShift, ast.LShift)
        ):
            left_pairs, left_anchor = self._collect_shift(node.left)
            right_pairs, right_anchor = self._collect_shift(node.right)
            pairs = list(left_pairs) + list(right_pairs)
            if isinstance(node.op, ast.RShift):
                pairs += [(u, d) for u in left_anchor for d in right_anchor]
            else:  # LShift: right operand is upstream of left.
                pairs += [(u, d) for u in right_anchor for d in left_anchor]
            return pairs, right_anchor
        return [], self._resolve_operand(node)

    def _maybe_set_relation(self, call: ast.Call) -> None:
        if not isinstance(call.func, ast.Attribute):
            self._maybe_chain(call)
            return
        method = call.func.attr
        if method not in {"set_upstream", "set_downstream"}:
            self._maybe_chain(call)
            return
        anchor = self._resolve_operand(call.func.value)
        others: set[tuple[str, str]] = set()
        for arg in call.args:
            others |= self._resolve_operand(arg)
        for this in anchor:
            for other in others:
                if method == "set_upstream":
                    self._add_dependency(other, this)  # other is upstream
                else:
                    self._add_dependency(this, other)  # this is upstream

    def _maybe_chain(self, call: ast.Call) -> None:
        if self._call_name(call.func) != "chain":
            return
        groups = [self._chain_group(arg) for arg in call.args]
        for left, right in zip(groups, groups[1:]):
            self._chain_pair(left, right)

    def _chain_group(self, node: ast.AST) -> list[set[tuple[str, str]]]:
        """Resolve a chain() argument to an ordered list of task-token sets."""
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return [self._resolve_operand(elt) for elt in node.elts]
        return [self._resolve_operand(node)]

    def _chain_pair(self, left: list, right: list) -> None:
        """Pair two consecutive chain() groups the way Airflow's chain() does.

        Equal-length lists are zipped elementwise; a scalar fans out across an
        adjacent list. Mismatched multi-element lists fall back to a cross
        product (Airflow itself raises here, so this is a best effort).
        """
        if len(left) > 1 and len(right) > 1 and len(left) == len(right):
            pairs = zip(left, right)
        elif len(left) == 1:
            pairs = ((left[0], r) for r in right)
        elif len(right) == 1:
            pairs = ((lt, right[0]) for lt in left)
        else:
            pairs = ((lt, r) for lt in left for r in right)
        for up_set, down_set in pairs:
            for up in up_set:
                for down in down_set:
                    self._add_dependency(up, down)

    def _resolve_operand(self, node: ast.AST) -> set[tuple[str, str]]:
        if isinstance(node, ast.Name):
            task = self.var_to_task.get(node.id)
            return {task} if task else set()
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            resolved: set[tuple[str, str]] = set()
            for elt in node.elts:
                resolved |= self._resolve_operand(elt)
            return resolved
        if isinstance(node, ast.Call):
            callee = self._call_name(node.func)
            if callee in self.decorated_task_functions:
                return {
                    (
                        self._resolve_dag_for_call(node),
                        self.decorated_task_functions[callee],
                    )
                }
            if callee and callee.endswith(_TASK_SUFFIXES):
                task_id = self._get_keyword_string(node, "task_id")
                if task_id:
                    return {(self._resolve_dag_for_call(node), task_id)}
        return set()

    def _extract_taskflow_data_deps(
        self, call: ast.Call, consumer: tuple[str, str]
    ) -> None:
        """Infer dependencies from TaskFlow XCom argument passing.

        When a decorated task's output (a variable bound to its invocation, or
        an inline invocation) is passed as an argument to another task, the
        receiving task depends on the producing one.
        """
        producers: set[tuple[str, str]] = set()
        for arg in list(call.args) + [kw.value for kw in call.keywords]:
            for sub in ast.walk(arg):
                if isinstance(sub, ast.Name) and sub.id in self.var_to_task:
                    producers.add(self.var_to_task[sub.id])
                elif isinstance(sub, ast.Call):
                    callee = self._call_name(sub.func)
                    if callee in self.decorated_task_functions:
                        producers.add(
                            (
                                self._resolve_dag_for_call(sub),
                                self.decorated_task_functions[callee],
                            )
                        )
        for producer in producers:
            self._add_dependency(producer, consumer, method="taskflow_data")

    def _add_dependency(
        self,
        upstream: tuple[str, str],
        downstream: tuple[str, str],
        method: str = "static_ast",
    ) -> None:
        up_dag, up_task = upstream
        down_dag, down_task = downstream
        if up_task == down_task and up_dag == down_dag:
            return
        dag_id = down_dag if down_dag != "unknown_dag" else up_dag
        self.task_dependencies.append(
            TaskDependency(
                dag_id=dag_id,
                upstream_task_id=up_task,
                downstream_task_id=down_task,
                extraction_method=method,
            )
        )

    # ------------------------------------------------------------------
    # Helpers.
    # ------------------------------------------------------------------
    def _dag_id_from_context_expr(self, expr: ast.AST) -> str | None:
        if isinstance(expr, ast.Call) and self._call_name(expr.func) == "DAG":
            return self._extract_dag_id(expr)
        # `with dag:` where `dag` was bound by an earlier `dag = DAG(...)`.
        if isinstance(expr, ast.Name):
            return self.dag_var_to_id.get(expr.id)
        return None

    def _extract_dag_id(self, call: ast.Call) -> str | None:
        dag_id = self._get_keyword_string(call, "dag_id")
        if dag_id:
            return dag_id
        if call.args:
            return self._literal_string(call.args[0])
        return None

    def _dag_id_from_decorators(
        self, decorators: list[ast.expr], fallback: str
    ) -> str | None:
        for dec in decorators:
            if isinstance(dec, ast.Call) and self._call_name(dec.func) == "dag":
                return self._get_keyword_string(dec, "dag_id") or (
                    self._literal_string(dec.args[0]) if dec.args else fallback
                )
            if self._call_name(dec) == "dag":
                return fallback
        return None

    def _is_task_decorated(self, decorators: list[ast.expr]) -> bool:
        for dec in decorators:
            name = (
                self._call_name(dec.func)
                if isinstance(dec, ast.Call)
                else self._call_name(dec)
            )
            if name == "task" or (name and name.startswith("task.")):
                return True
        return False

    def _task_id_from_decorators(self, decorators: list[ast.expr], default: str) -> str:
        """Return the explicit ``@task(task_id=...)`` if given, else the fn name."""
        for dec in decorators:
            if isinstance(dec, ast.Call):
                name = self._call_name(dec.func)
                if name == "task" or (name and name.startswith("task.")):
                    return self._get_keyword_string(dec, "task_id") or default
        return default

    def _is_task_call(self, name: str) -> bool:
        return (
            name in {"DAG", "dag", "task"}
            or name.startswith("task.")
            or name.endswith(_TASK_SUFFIXES)
            or name in self.decorated_task_functions
        )

    def _get_keyword_string(self, call: ast.Call, name: str) -> str | None:
        for kw in call.keywords:
            if kw.arg == name:
                return self._literal_string(kw.value)
        return None

    def _get_keyword_name(self, call: ast.Call, name: str) -> str | None:
        for kw in call.keywords:
            if kw.arg == name:
                return self._expr_name(kw.value)
        return None

    def _literal_string(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name) and node.id in self.string_constants:
            return self.string_constants[node.id]
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts = [
                p.value
                for p in node.values
                if isinstance(p, ast.Constant) and isinstance(p.value, str)
            ]
            return "".join(parts) if parts else None
        return None

    def _expr_name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._expr_name(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        if isinstance(node, ast.Lambda):
            return "<lambda>"
        return self._literal_string(node)

    def _call_name(self, node: ast.AST) -> str | None:
        return self._expr_name(node)

    def _looks_like_sql(self, text: str) -> bool:
        lowered = text.lower()
        return any(
            token in lowered
            for token in [
                "select ",
                "insert ",
                "create table",
                "create or replace",
                "merge into",
                " from ",
                "update ",
                "delete from",
            ]
        )

    def _record_datasets(self, text: str) -> None:
        for dataset in datasets_from_text(text):
            self.datasets.append(dataset)
            # Only attribute a free-standing string literal to a task when it
            # lives inside a real function body. Operator keyword strings are
            # already attributed by _collect_keyword_datasets, so emitting an
            # edge here with no current_function would create spurious
            # 'unknown_task' duplicates.
            if self.dag_stack and self.current_function:
                self.edges.append(
                    LineageEdge(
                        dag_id=self.dag_stack[-1],
                        task_id=self.current_function,
                        source_dataset=dataset.name,
                        target_dataset=None,
                        extraction_method="dataset_pattern",
                        confidence="low",
                    )
                )
