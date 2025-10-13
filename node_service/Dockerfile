# ================================================
# This Docker image exists to mimic the VM disk image that the node service is designed to run on.
# (named: `burla-node-gpu`, and `burla-node-nogpu`)

# When running the cluster locally, we run the node service inside this image and point the 
# docker python client at the host machines docker service (because you cannot run docker in docker)

### To create a new VM disk image:
# start a 2 vms (one with GPU, one without) from a base image or from the the last disk image:
# (do this in prod so you don't need to move it later.)
#
# FROM NEW IMAGE:
# gcloud compute instances create burla-disk-image-creator \
# --zone=us-central1-a \
# --machine-type=a3-highgpu-1g \  (use a3-highgpu-1g for gpu, n4-standard-2 for nogpu)
# --image-family=debian-12 \
# --image-project=debian-cloud \
# --maintenance-policy=TERMINATE \
# --boot-disk-size=20GB
# 
# FROM EXISTING IMAGE:
# gcloud compute instances create burla-disk-image-creator \
# --zone=us-central1-a \
# --machine-type=n4-standard-2 \
# --image=projects/burla-prod/global/images/burla-node-nogpu \
# --maintenance-policy=TERMINATE \
# --boot-disk-size=10GB
# 
# SSH into vm as root:
# gcloud compute ssh burla-disk-image-creator --zone=us-central1-a -- -t "sudo -i"
# 
# Run the commands in this dockerfile on a new debian-12 disk.
# Then stop the vm and save the diskl as an image.
# 
# MAKE SURE YOU MAKE ANY NEW MACHINE-IMAGES PUBLIC !!!
# use this command:
# gcloud compute images add-iam-policy-binding IMAGE_NAME \
#   --member=allAuthenticatedUsers \
#   --role=roles/compute.imageUser
# 
# ================================================

FROM debian:12

# dont ask y/n when installing stuff
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl ca-certificates git build-essential gnupg lsb-release \
    && rm -rf /var/lib/apt/lists/*

# install gcsfuse
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt gcsfuse-$(lsb_release -c -s) main" \
        > /etc/apt/sources.list.d/gcsfuse.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN apt-get update && apt-get install -y gcsfuse && rm -rf /var/lib/apt/lists/*

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# install Python 3.13 using uv and symlink to system python
RUN uv python install 3.13 && \
    ln -sf "$(uv python find 3.13)/python" /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/python3 /usr/local/bin/python && \
    ln -sf /usr/local/bin/python3 /usr/bin/python3

# Install docker,
# Not done here because you can't run docker in docker, and this image only exists
# to mimic a VM disk image, where docker would be installed using the below commands.
# ================================================
# RUN apt-get update && apt-get install -y docker.io
# RUN systemctl start docker
# RUN systemctl enable docker
# ================================================

# Install latest node_service and pip install packages now to make node starts faster
WORKDIR /opt
RUN git clone --depth 1 --branch 1.3.3 https://github.com/Burla-Cloud/burla.git --no-checkout
WORKDIR burla
RUN git sparse-checkout init --cone && \
    git sparse-checkout set node_service && \
    git checkout 1.3.3
WORKDIR node_service
RUN uv venv /opt/burla/.venv
RUN echo 'export UV_PROJECT_ENVIRONMENT=/opt/burla/.venv' >> /root/.bashrc
ENV UV_PROJECT_ENVIRONMENT=/opt/burla/.venv
RUN uv pip install -e .
