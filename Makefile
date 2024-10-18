.ONESHELL:
.SILENT:

test:
	poetry run pytest -s -k test_base
