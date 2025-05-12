ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}

ADD . /burla/worker_service
WORKDIR /burla/worker_service
RUN python${PYTHON_VERSION} -m pip install -e .

ENTRYPOINT []
