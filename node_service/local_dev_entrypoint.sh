#!/bin/bash
# Boot for a local-dev "fake VM" node container (runs privileged).
# Starts an inner docker daemon so node_service owns its workers exactly like
# it does on a real VM, then execs node_service. Real VMs do the equivalent of
# all this in their startup script / disk image.
set -e

# Delegate cgroup v2 controllers to nested containers (the standard
# docker-in-docker dance), otherwise workers get no memory controller and OOM
# detection via /sys/fs/cgroup/memory.events breaks. Everything that isn't a
# worker (this script, dockerd, node_service) goes in the node-service slice,
# which is what a real VM's startup script does with systemd-run.
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/burla-node-service.slice
    xargs -rn1 < /sys/fs/cgroup/cgroup.procs \
        > /sys/fs/cgroup/burla-node-service.slice/cgroup.procs || true
    sed -e 's/ / +/g' -e 's/^/+/' < /sys/fs/cgroup/cgroup.controllers \
        > /sys/fs/cgroup/cgroup.subtree_control
    # Same weights the VM startup script gives the two slices. Without them
    # node_service competes with its own workers at equal weight and a busy
    # node stops answering the head and the client for minutes at a time.
    mkdir -p /sys/fs/cgroup/burla-workers.slice
    echo 1000 > /sys/fs/cgroup/burla-node-service.slice/cpu.weight
    echo 80 > /sys/fs/cgroup/burla-workers.slice/cpu.weight
fi

# Other nodes' names (node_xxx, for input stealing and nested rpm) only exist
# in the outer docker network's DNS, which listens on this container's loopback
# where workers can't reach it. Forward worker DNS to it from the inner
# bridge's gateway address instead.
dnsmasq --bind-dynamic --listen-address=172.17.0.1 --no-resolv --server=127.0.0.11

dockerd --dns 172.17.0.1 > /var/log/dockerd.log 2>&1 &
for _ in $(seq 1 150); do
    docker info > /dev/null 2>&1 && break
    sleep 0.2
done
if ! docker info > /dev/null 2>&1; then
    echo "inner dockerd failed to start:"
    tail -50 /var/log/dockerd.log
    exit 1
fi

# Seed the default worker image so the inner daemon never pulls it. This node's
# image store survives node replacement (see LocalDockerProvider), so on every
# boot but the first the image is already there and unpacking it again would
# cost ~1GB of writes for nothing.
for tarball in /opt/burla/image-seed/*.tar; do
    [ -f "$tarball" ] || continue
    ref=$(awk '{print $1}' "${tarball%.tar}.ref" 2>/dev/null || true)
    if [ -n "$ref" ] && docker image inspect "$ref" > /dev/null 2>&1; then
        continue
    fi
    docker load < "$tarball"
done

# Paths a real VM's disk image / startup script would provide.
mkdir -p /worker_service_python_env /etc/burla/tls
cp /etc/ssl/certs/ca-certificates.crt /etc/burla/tls/ca-bundle.pem

# Workers (inner containers) can NAT to an IP but cannot resolve the outer
# docker host's alias, so hand node_service an IP-based head URL. v4 only: the
# alias also resolves to an IPv6 address that is invalid un-bracketed in URLs.
HOST_IP=$(getent ahostsv4 host.docker.internal | awk '{print $1; exit}')
export MAIN_SERVICE_URL="${MAIN_SERVICE_URL//host.docker.internal/$HOST_IP}"

# The image's baked venv was built from node_service on `main`, so `uv run`
# always re-syncs it against this checkout's lockfile. Point that at the shared
# host cache (the same one workers use) so it doesn't re-download wheels on
# every node boot.
export UV_CACHE_DIR=/uv_cache
export UV_LINK_MODE=copy

cd /opt/burla/node_service
exec uv run -m uvicorn node_service:app --host 0.0.0.0 --port "$NODE_PORT" \
    --workers 1 --timeout-keep-alive 600 --reload
