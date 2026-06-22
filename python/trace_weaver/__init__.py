"""trace-weaver -- the trace-weaver authoring SDK.

A tiny, dependency-free package that lets data engineers annotate data-producing
Airflow tasks so that column-level lineage can be **statically** extracted by the
trace-weaver compiler.

The single public entry point is the :func:`task` decorator::

    from trace_weaver import task

    @task(
        dag="medallion_lineage",
        inputs=["Test Database.poc_db.public.landing_sales"],
        outputs=["Test Database.poc_db.public.bronze_sales"],
        engine="sql",
        sql=BRONZE_SQL,
        transform="CAST / DEDUPE",
        column_map=[
            (["raw_event_id"], "event_id", "CAST text -> bigint"),
        ],
    )
    def build_bronze():
        ...

or, equivalently::

    import trace_weaver as tw

    @tw.task(...)
    def build_bronze():
        ...

Design contract
---------------
* **Runtime no-op.** ``@task(...)`` returns a decorator that returns the wrapped
  function *unchanged*. Annotated DAGs therefore run normally under Airflow; the
  decorator adds no behaviour, no wrapping, and no import-time side effects beyond
  appending an inert record to :data:`tw.registry`.
* **Statically parseable.** Every argument is a plain literal (or a name that
  references a module-level string constant), so the trace-weaver scanner can read the
  declarations from source without executing user code.
* **stdlib only**, Python 3.9+.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar, Union

__all__ = ["task", "sql", "configure", "config", "Dataset", "registry", "Declaration", "__version__"]

__version__ = "0.1.0"

#: Allowed values for the ``engine`` argument. Kept here so the compiler and the
#: SDK agree on a single source of truth, but the decorator does **not** enforce
#: it at runtime (forward-compat: unknown engines are tolerated).
ENGINES = ("sql", "pandas", "spark", "python", "bash")

F = TypeVar("F", bound=Callable[..., Any])

# A single column-lineage rule: (source columns, target column, function label).
ColumnMapEntry = Tuple[Sequence[str], str, str]


@dataclass(frozen=True)
class Dataset:
    """A thin, literal-friendly wrapper around an OpenMetadata dataset FQN.

    The fully-qualified name uses OpenMetadata style
    ``"service.database.schema.table"``. The *service* segment may contain
    spaces, e.g. ``"Test Database.poc_db.public.bronze_sales"``.

    This is pure sugar so engineers can write::

        Dataset("Test Database.poc_db.public.bronze_sales")

    instead of a bare string. ``inputs=`` / ``outputs=`` accept either form.
    Keep usages literal (a single string argument) so the static scanner can
    resolve them without executing code.

    Examples
    --------
    >>> Dataset("Test Database.poc_db.public.bronze_sales").table
    'bronze_sales'
    >>> str(Dataset("svc.db.public.t"))
    'svc.db.public.t'
    """

    fqn: str

    def __str__(self) -> str:  # so ``str(ds)`` yields the bare FQN
        return self.fqn

    def _parts(self) -> List[str]:
        return self.fqn.split(".")

    @property
    def service(self) -> str:
        """The service segment (the part before the first ``.``)."""
        return self._parts()[0]

    @property
    def database(self) -> Optional[str]:
        """The database segment, or ``None`` if the FQN is too short."""
        parts = self._parts()
        return parts[1] if len(parts) > 1 else None

    @property
    def schema(self) -> Optional[str]:
        """The schema segment, or ``None`` if the FQN is too short."""
        parts = self._parts()
        return parts[2] if len(parts) > 2 else None

    @property
    def table(self) -> Optional[str]:
        """The table segment (everything after the third ``.``)."""
        parts = self._parts()
        return parts[3] if len(parts) > 3 else None


@dataclass(frozen=True)
class Declaration:
    """An inert, introspectable record of one ``@task(...)`` declaration.

    Instances are appended to :data:`tw.registry` purely for users who want
    runtime introspection. They never affect the decorated function's behaviour.
    """

    func_name: str
    dag: Optional[str]
    inputs: Tuple[str, ...]
    outputs: Tuple[str, ...]
    engine: Optional[str]
    sql: Optional[str]
    description: Optional[str]
    transform: Optional[str]
    column_map: Tuple[ColumnMapEntry, ...]
    copy: Tuple[str, ...] = ()
    extra: dict = field(default_factory=dict)


#: Optional runtime registry of every declaration seen this process. Appending
#: here is the decorator's *only* side effect; it never changes behaviour. The
#: trace-weaver compiler does not rely on it -- it reads source statically.
registry: List[Declaration] = []


def _as_fqn(value: Union[str, "Dataset"]) -> str:
    """Coerce a string or :class:`Dataset` into a bare FQN string."""
    return value.fqn if isinstance(value, Dataset) else str(value)


def _norm_sources(sources: Union[str, Sequence[str]]) -> Tuple[str, ...]:
    """Normalize a ``column_map`` entry's sources.

    A source may be written as a **bare string** (one source) or as a list/tuple
    of names, so ``("amount", "is_valid", "amount > 0")`` and
    ``(["amount"], "is_valid", "amount > 0")`` are equivalent.
    """
    return (sources,) if isinstance(sources, str) else tuple(sources)


def _normalize_datasets(
    values: Optional[Sequence[Union[str, "Dataset"]]],
) -> Tuple[str, ...]:
    if not values:
        return ()
    return tuple(_as_fqn(v) for v in values)


def task(
    func: Optional[F] = None,
    *,
    dag: Optional[str] = None,
    inputs: Optional[Sequence[Union[str, "Dataset"]]] = None,
    outputs: Optional[Sequence[Union[str, "Dataset"]]] = None,
    engine: Optional[str] = None,
    sql: Optional[str] = None,
    description: Optional[str] = None,
    transform: Optional[str] = None,
    column_map: Optional[Sequence[ColumnMapEntry]] = None,
    copy: Optional[Sequence[str]] = None,
    **kwargs: Any,
) -> Union[F, Callable[[F], F]]:
    """Annotate a data-producing Airflow task for trace-weaver lineage extraction.

    This is a **runtime no-op**: it returns the wrapped function unchanged so
    annotated DAGs run normally under Airflow. Its sole purpose is to carry
    *statically parseable* lineage metadata that the trace-weaver compiler reads from
    source. The only runtime effect is appending a :class:`Declaration` to
    :data:`tw.registry`.

    Usage::

        from trace_weaver import task

        @task(
            dag="medallion_lineage",
            inputs=["Test Database.poc_db.public.landing_sales"],
            outputs=["Test Database.poc_db.public.bronze_sales"],
            engine="sql",
            sql=BRONZE_SQL,                 # str literal OR a module-level constant name
            description="### markdown ...", # str literal OR a constant name
            transform="CAST / DEDUPE",
            column_map=[
                (["raw_event_id"], "event_id", "CAST text -> bigint"),
                (["amount", "currency"], "amount_usd", "ROUND(amount * fx[currency], 2)"),
            ],
        )
        def build_bronze():
            ...

    It also supports a bare form (carrying no metadata)::

        @task
        def build_bronze():
            ...

    Parameters
    ----------
    dag:
        DAG / pipeline id this task belongs to.
    inputs:
        Source dataset FQNs (``list[str]`` or :class:`Dataset`). OpenMetadata
        style ``"service.database.schema.table"``; the *service* segment may
        contain spaces.
    outputs:
        Output dataset FQNs (same format as ``inputs``).
    engine:
        One of ``"sql"``, ``"pandas"``, ``"spark"``, ``"python"`` or ``"bash"``.
        Not enforced at runtime; unknown values are tolerated for forward-compat.
    sql:
        The SQL text, as a string literal or a name referencing a module-level
        ``str`` constant. When ``engine="sql"`` the compiler parses this to infer
        column lineage for targets not already covered by ``column_map``.
    description:
        Rich (markdown) edge description; string literal or constant name.
    transform:
        Short transform-kind label, e.g. ``"CAST / DEDUPE"``.
    column_map:
        Declared (authoritative) column lineage as a list of
        ``(sources, target, function)`` tuples. ``sources`` is a list of bare
        column names (qualified ``"table.col"`` when there are multiple inputs);
        ``target`` is a bare output column name; ``function`` is a label.
        ``sources`` may also be a **bare string** for a single source, so
        ``("amount", "amount_usd", "ROUND(...)")`` is equivalent to
        ``(["amount"], "amount_usd", "ROUND(...)")``.
    copy:
        Shortcut for **same-name passthrough** columns: a flat list of bare column
        names, each declaring an identity edge equivalent to
        ``([name], name, "direct copy")``. List the trivial 1:1 columns here and
        reserve ``column_map`` for the columns that actually change. An explicit
        ``column_map`` entry for a name always wins over ``copy``.
    **kwargs:
        Tolerated and recorded under :attr:`Declaration.extra` for
        forward-compatibility. Never alters behaviour.

    Returns
    -------
    The original function, unchanged -- either directly (bare ``@task`` form)
    or via the returned decorator (``@task(...)`` form).
    """

    def _record(target: F) -> F:
        # Accept 3-tuples (sources, target, function) AND 2-tuples (sources,
        # target) — the scanner treats the function as optional, so the SDK must
        # too (finding #17) — plus a bare-string source as sugar for a 1-element
        # list. Never raise at DAG import.
        cmap = [
            (_norm_sources(entry[0]), entry[1], entry[2] if len(entry) > 2 else None)
            for entry in (column_map or ())
        ]
        # copy=[...] : declared same-name identity ("direct copy") columns. An
        # explicit column_map entry for the same target wins, so copy is skipped
        # for it (dedupe by target).
        declared_targets = {t for _, t, _ in cmap}
        for name in copy or ():
            if name not in declared_targets:
                cmap.append(((name,), name, "direct copy"))
                declared_targets.add(name)
        decl = Declaration(
            func_name=getattr(target, "__name__", repr(target)),
            dag=dag,
            inputs=_normalize_datasets(inputs),
            outputs=_normalize_datasets(outputs),
            engine=engine,
            sql=sql,
            description=description,
            transform=transform,
            column_map=tuple(cmap),
            copy=tuple(copy or ()),
            extra=dict(kwargs),
        )
        registry.append(decl)
        # No wrapping: return the function exactly as authored.
        return target

    # Bare usage: @task  (func is the decorated function, no metadata supplied).
    if func is not None and callable(func):
        return _record(func)

    # Parameterized usage: @task(...) -> returns the real decorator.
    return _record


#: Per-file defaults set by :func:`configure`. Inert at runtime; the trace-weaver scanner
#: reads the ``configure(...)`` call statically to expand bare table names into
#: full FQNs and to supply a default ``dag``.
config: dict = {}


def configure(
    *,
    service: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    dag: Optional[str] = None,
) -> dict:
    """Set per-file defaults so tasks can use **bare table names** instead of full FQNs.

    Call once near the top of a DAG module::

        import trace_weaver as tw
        tw.configure(service="Test Database", database="poc_db", schema="public")

    After this, ``inputs=["bronze_sales"]`` is expanded by the scanner to
    ``"Test Database.poc_db.public.bronze_sales"``. A ``dag=`` here (or a
    ``with DAG(dag_id=...)`` block) becomes the default DAG for every task in the
    file, so you don't repeat it. Runtime no-op beyond recording into
    :data:`tw.config`. Keep the arguments string literals so the scanner can
    read them without executing code.
    """
    for k, v in dict(
        service=service, database=database, schema=schema, dag=dag
    ).items():
        if v is not None:
            config[k] = v
    return config


def sql(
    query: str,
    *,
    inputs: Optional[Sequence[Union[str, "Dataset"]]] = None,
    outputs: Optional[Sequence[Union[str, "Dataset"]]] = None,
    dag: Optional[str] = None,
    description: Optional[str] = None,
    transform: Optional[str] = None,
    column_map: Optional[Sequence[ColumnMapEntry]] = None,
    copy: Optional[Sequence[str]] = None,
    **kwargs: Any,
) -> Callable[[F], F]:
    """Shortcut decorator for a **SQL transform** — equivalent to
    ``@task(engine="sql", sql=query, ...)``.

    Column lineage is auto-extracted from ``query`` (no ``column_map`` needed),
    so this is the lowest-friction way to annotate a SQL step::

        @tw.sql(BRONZE_SQL, inputs=["landing_sales"], outputs=["bronze_sales"])
        def build_bronze():
            ...

    Runtime no-op, same as :func:`task`. ``query`` may be a string literal or a
    name that references a module-level string constant.
    """
    return task(
        dag=dag,
        inputs=inputs,
        outputs=outputs,
        engine="sql",
        sql=query,
        description=description,
        transform=transform,
        column_map=column_map,
        copy=copy,
        **kwargs,
    )


def _self_check() -> bool:
    """Tiny self-check: prove the decorator is an importable runtime no-op.

    Returns ``True`` on success. Can be run via ``python -m`` style execution
    of this module or simply called from a REPL.
    """
    BRONZE_SQL = "SELECT raw_event_id AS event_id FROM landing_sales"

    @task(
        dag="medallion_lineage",
        inputs=[Dataset("Test Database.poc_db.public.landing_sales")],
        outputs=["Test Database.poc_db.public.bronze_sales"],
        engine="sql",
        sql=BRONZE_SQL,
        transform="CAST / DEDUPE",
        column_map=[(["raw_event_id"], "event_id", "CAST text -> bigint")],
    )
    def build_bronze(x: int) -> int:
        return x + 1

    # Runtime behaviour must be untouched.
    assert build_bronze(41) == 42
    assert build_bronze.__name__ == "build_bronze"

    # Bare form is also a no-op.
    @task
    def passthrough(y: int) -> int:
        return y

    assert passthrough(7) == 7

    # A declaration was recorded for introspection.
    assert registry and registry[0].outputs == (
        "Test Database.poc_db.public.bronze_sales",
    )
    return True


if __name__ == "__main__":  # pragma: no cover
    print("umap self-check:", "ok" if _self_check() else "FAILED")
