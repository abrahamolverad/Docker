.PHONY: setup test format lint type run-api
setup:
	pip install -r requirements.txt
	pip install pytest ruff mypy pre-commit
	pre-commit install

format:
	ruff check . --fix
	ruff format .

lint:
	ruff check .

type:
	mypy src

test:
	pytest -q

run-api:
	python -m src.api
