ARG POETRY_VERSION=1.5.1

FROM python:3.11.4-bullseye
ARG POETRY_VERSION

ENV DEBIAN_FRONTEND noninteractive
ENV TZ=Etc/GMT

WORKDIR /burla/main_service

# This bakes the current codebase into the image which isn't ideal but seems necessary to make
# sure the local project is installed as a python package.
RUN pip install poetry==$POETRY_VERSION
ADD . /burla/main_service
RUN poetry install --sync

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

ENV IN_PROD True
ENTRYPOINT poetry run uvicorn main_service:app --host 0.0.0.0 --port 8080
