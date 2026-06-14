.PHONY: install install-dev scan scan-json scan-csv scan-mermaid scan-all test pytest clean zip \
        docker-build docker-build-render docker-run-sample compose-up compose-db

install:
	pip install -e .

install-dev:
	pip install -e ".[dev,all]"

scan: scan-all

scan-json:
	python -m traceweaver scan --repo-path examples/sample_dags --output json --output-dir outputs/json

scan-csv:
	python -m traceweaver scan --repo-path examples/sample_dags --output csv --output-dir outputs/csv

scan-mermaid:
	python -m traceweaver scan --repo-path examples/sample_dags --output mermaid --output-dir outputs

scan-all:
	python -m traceweaver scan --repo-path examples/sample_dags --output all --output-dir outputs

test:
	python -m unittest discover -s tests

pytest:
	pytest -q

clean:
	rm -rf outputs/csv outputs/json outputs/*.csv outputs/*.json build dist *.egg-info .pytest_cache *.db out

zip:
	cd .. && zip -r TraceWeaver.zip TraceWeaver -x 'TraceWeaver/.venv/*' 'TraceWeaver/__pycache__/*' 'TraceWeaver/out/*' 'TraceWeaver/dags/*'

docker-build:
	docker build -t traceweaver:latest .

docker-build-render:
	docker build -f Dockerfile.render -t traceweaver:render .

docker-run-sample:
	docker run --rm -v $(PWD)/examples/sample_dags:/dags:ro -v $(PWD)/out:/out traceweaver:latest

compose-up:
	docker compose run --rm traceweaver

compose-db:
	docker compose --profile db up --build
