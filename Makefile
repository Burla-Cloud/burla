.ONESHELL:
.SILENT:

test:
	poetry run pytest -s -k test_base

test_gpu:
	poetry run pytest -s -k test_gpu

test_pandas:
	poetry run pytest -s -k test_pandas
