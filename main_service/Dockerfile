ARG POETRY_VERSION=1.5.1

FROM python:3.11.4-bullseye
ARG POETRY_VERSION

ENV DEBIAN_FRONTEND noninteractive
ENV TZ=Etc/GMT

WORKDIR /home/pkg_dev/app

# set custom bash prompt cause the default one is 56 characters long
ENV SHELL bash
ENV VIRTUAL_ENV_DISABLE_PROMPT 1
RUN echo 'PS1="\e[0;34m\u:\W$ \e[0m"' | tee -a /home/pkg_dev/.bashrc /root/.bashrc

# create non-root user
RUN useradd pkg_dev \
    && mkdir -p /home/pkg_dev/.cache/pypoetry \
    && chown -R pkg_dev /home/pkg_dev
ENV PATH="/home/pkg_dev/.local/bin:${PATH}"
USER pkg_dev

# This bakes the current codebase into the image which isn't ideal but seems necessary to make
# sure the local project is installed as a python package.
RUN pip install poetry==$POETRY_VERSION
ADD . /home/pkg_dev/app/
RUN poetry install --sync

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

ENV IN_PROD True
ENTRYPOINT poetry run uvicorn main_service:application --host 0.0.0.0 --port 8080
