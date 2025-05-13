ARG PV
FROM python:${PV}

ADD . /burla/worker_service
WORKDIR /burla/worker_service
RUN python${PV} -m pip install -e .

ENTRYPOINT []
