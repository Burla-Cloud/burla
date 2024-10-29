### How to test the main webservice

This service has no test suite,
To test it we run the tests for the burla python package, while pointing to a locally running instance of this service.

1. Run `make container`, then run `make service`
2. In the python package, in `__init__.py` ensure `_BURLA_SERVICE_URL = "http://127.0.0.1:5000"`
3. Run `make test` from `burla/python_package`
4. observe `main_service` errors in the terminal where the service is running.