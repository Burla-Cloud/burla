# ================================================
# This Docker image exists to mimic the VM disk image that the node service is designed to run on.
# (named something like: burla-cluster-node-image-6)

# When running the cluster locally, we run the node service inside this image and point the 
# docker python client at the host machines docker service (because you cannot run docker in docker).

# To create a new VM disk image, simply replicate the below commands on a raw debian-12 disk.

# MAKE SURE YOU MAKE ANY NEW IMAGES PUBLIC!
# use this command:
# gcloud compute images add-iam-policy-binding IMAGE_NAME \
#   --member=allAuthenticatedUsers \
#   --role=roles/compute.imageUser

# ================================================

FROM debian:12

# dont ask y/n when installing stuff
ENV DEBIAN_FRONTEND=noninteractive

# install git and python prerequisites
RUN apt-get update && apt-get install -y git build-essential libssl-dev zlib1g-dev \
  libncurses5-dev libncursesw5-dev libreadline-dev libsqlite3-dev \
  libgdbm-dev libdb5.3-dev libbz2-dev libexpat1-dev liblzma-dev tk-dev curl libffi-dev
RUN apt-get update

# Install docker,
# Not done here because you can't run docker in docker, and this image only exists
# to mimic a VM disk image, where docker would be installed using the below commands.
# ================================================
# RUN apt-get update && apt-get install -y docker.io
# RUN systemctl start docker
# RUN systemctl enable docker
# ================================================

# install python 3.13 from source
RUN cd /usr/src && \
    curl -O https://www.python.org/ftp/python/3.13.4/Python-3.13.4.tgz && \
    tar -xf Python-3.13.4.tgz && \
    cd Python-3.13.4 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall
RUN ln -sf /usr/local/bin/python3.13 /usr/bin/python


# Install latest node_service and pip install packages now to make node starts faster
RUN git clone --depth 1 --branch 1.2.2 https://github.com/Burla-Cloud/burla.git --no-checkout
WORKDIR burla
RUN git sparse-checkout init --cone && \
    git sparse-checkout set node_service && \
    git checkout 1.2.2
WORKDIR node_service
RUN python -m pip install --break-system-packages -e .
