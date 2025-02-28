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

# install git, python, and deps needed for gcloud
RUN apt-get update && apt-get install -y git ca-certificates curl gnupg
RUN apt install -y python3-pip

# Install gcloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] \
        http://packages.cloud.google.com/apt cloud-sdk main" | \
        tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
        apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && apt-get install google-cloud-sdk -y

# Install docker,
# Not done here because you can't run docker in docker, and this image only exists
# to mimic a VM disk image, where docker would be installed using the below commands.
# ================================================
# install -m 0755 -d /etc/apt/keyrings
# curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# chmod a+r /etc/apt/keyrings/docker.gpg
# echo \
#   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
#   $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
#   tee /etc/apt/sources.list.d/docker.list > /dev/null
# apt-get update
# apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# # use gcloud to authenticate docker with GAR
# gcloud auth configure-docker us-docker.pkg.dev
# ================================================

# Install latest node_service and pip install packages now to make node starts faster
RUN git clone --depth 1 https://github.com/Burla-Cloud/burla.git --no-checkout
WORKDIR burla
RUN git sparse-checkout init --cone && \
    git sparse-checkout set node_service && \
    git checkout main
WORKDIR node_service
RUN python3.11 -m pip install --break-system-packages -e .
