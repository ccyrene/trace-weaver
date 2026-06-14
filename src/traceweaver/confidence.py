"""Central definitions and rules for lineage confidence levels.

A single source of truth so every scanner labels lineage the same way. The
scale is three ordered levels and the rules below decide which one a given
piece of evidence earns. Import the constants/helpers here instead of writing
bare ``"high"`` / ``"medium"`` / ``"low"`` literals at the call site.
"""

from __future__ import annotations

# Ordered from most to least trustworthy.
HIGH = "high"
MEDIUM = "medium"
LOW = "low"

LEVELS = (HIGH, MEDIUM, LOW)

# --- Fixed levels for specific evidence kinds --------------------------------
# Control-flow dependencies are read verbatim from the AST, so they are exact.
TASK_DEPENDENCY = HIGH
# Heuristic, single-sided signals: a connection id reference, or a dataset
# recognised only by a URI / file-path pattern.
CONNECTION = LOW
PATTERN = LOW
# Operation-level inference: the KIND of source/sink is read from a library
# call (pandas/boto3/SQLAlchemy/hook) even though the exact name is runtime-built.
OPERATION = LOW


def sql_confidence(*, high_confidence: bool, paired: bool) -> str:
    """Confidence for one SQL-derived lineage edge.

    ``high_confidence`` is True for sqlglot parse-tree results and False for the
    regex fallback. ``paired`` is True when the edge links a concrete
    source→target pair and False for a single-sided half-edge.

        sqlglot : paired -> high,   half -> medium
        regex   : paired -> medium, half -> low
    """
    if high_confidence:
        return HIGH if paired else MEDIUM
    return MEDIUM if paired else LOW
