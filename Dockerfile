# TraceWeaver — lean image (< 100 MB).
#
# Build:  docker build -t traceweaver:latest .
# Scan :  docker run --rm -v "$PWD/dags:/dags:ro" -v "$PWD/out:/out" traceweaver:latest
#
# Scans /dags and writes CSV + JSON + a Mermaid graph (lineage.mmd) to /out.
# View/render the .mmd at https://mermaid.live or with mermaid-cli. For an image
# that renders PNG/SVG itself, build the heavier Dockerfile.render instead.
FROM python:3.11-alpine

WORKDIR /work

# sqlglot is pure-Python (dialect-aware SQL lineage); install it first so a
# source edit does not invalidate this layer. No psycopg/sqlalchemy (no DB
# export), no Node/Chromium (no image rendering), no git — keeps the image tiny.
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir "sqlglot>=25.0.0"

COPY src ./src
RUN pip install --no-cache-dir --no-deps .

COPY examples ./examples

# Defaults: read-only DAGs in, artifacts out (CSV + JSON + lineage.mmd).
ENV TRACEWEAVER_REPO_PATH=/dags \
    TRACEWEAVER_OUTPUT_DIR=/out \
    TRACEWEAVER_OUTPUT_FORMAT=all \
    PYTHONUNBUFFERED=1
RUN mkdir -p /dags /out

# Runs as root so writes to a bind-mounted /out always succeed; for host-owned
# files run with: --user "$(id -u):$(id -g)"
ENTRYPOINT ["traceweaver"]
CMD ["scan"]
