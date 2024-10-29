.ONESHELL:
.SILENT:

# The Node service does not have a docker image.
# When deployed it is run inside a raw vm image inside it's poetry environment.
# When developed locally it is run inside it's poetry environment.

test:
	poetry run python -m pytest -s --tb=short --disable-warnings -k test_everything_simple

test_all:
	poetry run python -m pytest -s --tb=short --disable-warnings