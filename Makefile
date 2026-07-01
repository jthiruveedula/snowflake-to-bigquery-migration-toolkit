.PHONY: install test lint assess translate-sql create-schema transfer sync validate cutover clean

install:
	pip install -r requirements.txt

test:
	pytest tests/ -v

lint:
	python -m py_compile src/*.py

assess:
	python -m src.cli assess

translate-sql:
	python -m src.cli translate-sql

create-schema:
	python -m src.cli create-schema

transfer:
	python -m src.cli transfer

sync:
	python -m src.cli sync

validate:
	python -m src.cli validate

cutover:
	python -m src.cli cutover

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -f migration_state.json assessment_report.md translation_issues.md validation_report.md cutover_runbook.md
