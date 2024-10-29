


# Do I need this ??????



FROM debian:12-slim

# Set to non-interactive (this prevents some prompts)
ENV DEBIAN_FRONTEND=noninteractive
# so we can call `pyenv`
ENV PYENV_ROOT="$HOME/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PATH"
ENV PATH="/.pyenv/versions/3.11.*/bin:$PATH"
# use 6 cores when installing python (becasue 5 versions are installed in parallel)
ENV MAKEOPTS="-j32"

# Install dependencies and pyenv
RUN apt update && apt upgrade && apt-get update && apt-get install -y make curl git wget llvm zlib1g-dev \
    build-essential libbz2-dev libncurses5-dev libffi-dev libreadline-dev libncursesw5-dev \
    xz-utils tk-dev libssl-dev libsqlite3-dev liblzma-dev && \
    curl https://pyenv.run | bash && \
    echo 'eval "$(pyenv init -)"' >> ~/.bashrc

WORKDIR /burla
ADD ./src /burla/src
ADD pyproject.toml /burla

# Install a Python
RUN pyenv install 3.11

RUN /.pyenv/versions/3.11.*/bin/python3.11 -m pip install -e .
