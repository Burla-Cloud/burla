#!/bin/bash
# Boot for a local-dev "fake VM" node container (runs privileged).
# Starts an inner docker daemon so node_service owns its workers exactly like
# it does on a real VM, then execs node_service. Real VMs do the equivalent of
# all this in their startup script / disk image.
set -e

# Delegate cgroup v2 controllers to nested containers (the standard
# docker-in-docker dance), otherwise workers get no memory controller and OOM
# detection via /sys/fs/cgroup/memory.events breaks.
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
    mkdir -p /sys/fs/cgroup/init
    xargs -rn1 < /sys/fs/cgroup/cgroup.procs > /sys/fs/cgroup/init/cgroup.procs || true
    sed -e 's/ / +/g' -e 's/^/+/' < /sys/fs/cgroup/cgroup.controllers \
        > /sys/fs/cgroup/cgroup.subtree_control
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

# Seed the default worker image so the inner daemon never pulls it.
for tarball in /opt/burla/image-seed/*.tar; do
    if [ -f "$tarball" ]; then
        docker load < "$tarball"
    fi
done

# Paths a real VM's disk image / startup script would provide.
mkdir -p /worker_service_python_env /etc/burla/tls
cp /etc/ssl/certs/ca-certificates.crt /etc/burla/tls/ca-bundle.pem

# Workers (inner containers) can NAT to an IP but cannot resolve the outer
# docker host's alias, so hand node_service an IP-based head URL. v4 only: the
# alias also resolves to an IPv6 address that is invalid un-bracketed in URLs.
HOST_IP=$(getent ahostsv4 host.docker.internal | awk '{print $1; exit}')
export MAIN_SERVICE_URL="${MAIN_SERVICE_URL//host.docker.internal/$HOST_IP}"

cd /opt/burla/node_service
exec uv run -m uvicorn node_service:app --host 0.0.0.0 --port "$NODE_PORT" \
    --workers 1 --timeout-keep-alive 600 --reload
