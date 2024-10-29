
# This is the default docker image that all user-submitted functions are run in.
# This separate image inherits from `Dockerfile.base` so we can quickly deploy the container_service
# by baking it into & uploading a new image without needing to install python again

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

WORKDIR /burla
RUN rm pyproject.toml && rm -rf ./src
ADD ./src /burla/src
ADD pyproject.toml /burla
