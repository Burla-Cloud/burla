.ONESHELL:
.SILENT:

test:
	poetry run pytest -s -k test_base

test_gpu:
	poetry run pytest -s -k test_gpu

test_nas:
	poetry run pytest -s -k test_nas

test_pandas:
	poetry run pytest -s -k test_pandas

test_spacy:
	poetry run pytest -s -k test_spacy

test_trinity:
	poetry run pytest -s -k test_trinity

