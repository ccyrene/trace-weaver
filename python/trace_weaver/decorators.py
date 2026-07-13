"""The ``@lineage`` authoring decorator.

``@lineage`` is a **declarative, statically-parseable** marker that records the
datasets a function reads and writes so the trace-weaver compiler can attribute
dataset-level lineage to the task without executing any code.

Contract
--------
::

    from trace_weaver import lineage

    @lineage(
        inputs=["s3://raw-bucket/sales/{date}.parquet"],
        outputs=["iceberg://warehouse.sales.bronze"],
        name=None,
        description=None,
    )
    def build_bronze(...):
        ...

It is also usable **bare** (no call), marking the function for lineage with no
declared datasets::

    @lineage
    def build_bronze(...):
        ...

Runtime behaviour
-----------------
* **Zero overhead, no side effects.** It returns the *original* function object
  unchanged and only attaches a plain ``dict`` to
  :data:`trace_weaver.decorators.LINEAGE_ATTR` (``f.__traceweaver_lineage__``).
* **Never raises at decoration time.** Bad metadata is normalised leniently
  (coerced / dropped), so an annotated DAG always imports.
* **stdlib only**, Python 3.9+. Safe to stack with Airflow's ``@task`` in any
  order — each decorator returns the same function object, so the attribute
  survives.

``inputs`` / ``outputs`` are lists of dataset URI strings (``s3://``,
``mongodb://``, ``iceberg://``, ``postgresql://``, ``file://`` or an Airflow
conn-id reference). A string MAY contain template placeholders such as
``{date}`` — it is still a declared dataset (a template), and the static scanner
reads it verbatim.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, TypeVar, Union

__all__ = ["lineage", "LINEAGE_ATTR"]

F = TypeVar("F", bound=Callable[..., Any])

#: The attribute the decorator attaches to the wrapped function. The static
#: scanner never reads this (it works purely from the AST); it exists for
#: optional runtime introspection and to make the contract observable in tests.
LINEAGE_ATTR = "__traceweaver_lineage__"


def _normalize_uris(values: Optional[Union[str, Sequence[Any]]]) -> List[str]:
    """Coerce an ``inputs=`` / ``outputs=`` value into a list of URI strings.

    Lenient by design — this must never raise at decoration time:

    * ``None`` -> ``[]``
    * a bare ``str`` -> ``[str]`` (a common single-dataset shorthand)
    * any other iterable -> each element coerced to ``str`` (``None`` dropped)
    * anything non-iterable -> ``[]``
    """
    if values is None:
        return []
    if isinstance(values, str):
        return [values]
    try:
        items = list(values)
    except TypeError:
        return []
    out: List[str] = []
    for v in items:
        if v is None:
            continue
        out.append(v if isinstance(v, str) else str(v))
    return out


def _normalize_text(value: Any) -> Optional[str]:
    """Normalise an optional free-text field (``name=`` / ``description=``).

    Lenient: a ``str`` is kept; anything else (including ``None``) becomes
    ``None``. These fields are human metadata, so a non-string is a mistake we
    drop rather than store as a meaningless ``repr``.
    """
    return value if isinstance(value, str) else None


def lineage(
    func: Optional[F] = None,
    *,
    inputs: Optional[Union[str, Sequence[Any]]] = None,
    outputs: Optional[Union[str, Sequence[Any]]] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> Union[F, Callable[[F], F]]:
    """Declare the datasets a function reads (``inputs``) and writes (``outputs``).

    Supports both the bare form (``@lineage``) and the call form
    (``@lineage(...)``). Returns the original function unchanged, attaching the
    declared metadata as ``func.__traceweaver_lineage__``. See the module
    docstring for the full contract.
    """
    meta: Dict[str, Any] = {
        "inputs": _normalize_uris(inputs),
        "outputs": _normalize_uris(outputs),
        "name": _normalize_text(name),
        "description": _normalize_text(description),
    }

    def _apply(target: F) -> F:
        try:
            # A fresh dict (with fresh list copies) per function so two functions
            # sharing one decorator object never alias each other's datasets.
            setattr(
                target,
                LINEAGE_ATTR,
                {
                    "inputs": list(meta["inputs"]),
                    "outputs": list(meta["outputs"]),
                    "name": meta["name"],
                    "description": meta["description"],
                },
            )
        except (AttributeError, TypeError):
            # Some callables (builtins, certain C objects) reject attribute
            # assignment. The contract is "never raise": return unchanged.
            pass
        return target

    # Bare form: @lineage  (func is the decorated callable, no metadata supplied).
    if func is not None and callable(func):
        return _apply(func)

    # Call form: @lineage(...) -> return the real decorator.
    return _apply
