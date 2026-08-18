.ONESHELL:
.SILENT:

UV_PROJECT := ./client
PROJECT_ABS := $(abspath $(UV_PROJECT))

# One dev cluster per checkout. Several run side by side on one machine, so
# every daemon-global or host-global name they need (docker network, container
# labels, published head port, published node ports) derives from this name.
BURLA_CLUSTER_NAME ?= $(shell basename "$(CURDIR)" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g')
BURLA_CLUSTER_NETWORK := burla-$(BURLA_CLUSTER_NAME)
# Hashed rather than allocated so a checkout's dashboard URL and node ports stay
# the same across restarts. Override either if two checkouts ever collide.
BURLA_HEAD_PORT ?= $(shell python3 -c 'import hashlib,sys;print(5100+int(hashlib.sha256(sys.argv[1].encode()).hexdigest(),16)%800)' '$(BURLA_CLUSTER_NAME)')
BURLA_NODE_PORT_BASE ?= $(shell python3 -c 'import hashlib,sys;print(9000+(int(hashlib.sha256(sys.argv[1].encode()).hexdigest(),16)%250)*20)' '$(BURLA_CLUSTER_NAME)')
BURLA_DASHBOARD_URL := http://localhost:$(BURLA_HEAD_PORT)

# local-dev's node base image, built here from the Dockerfile rather than
# pulled, so local-dev needs no image registry at all. Node code is
# bind-mounted at runtime, so it only needs rebuilding when the node Dockerfile
# or node_service's locked dependencies change. (The head has no image: it runs
# as a host subprocess straight from this checkout, like remote-dev.)
LOCAL_NODE_IMAGE := burla-node-service:local-dev
# Stamped onto the image as a label so `local-dev` can tell an image built from
# today's Dockerfile from one built from an older one. Without it a stale image
# under the same tag is silently reused and its nodes die on whatever the
# Dockerfile added since (a pre-DinD image exits 127 on `dnsmasq`).
NODE_IMAGE_FINGERPRINT := $(shell python3 -c 'import hashlib;print(hashlib.sha256(open("node_service/Dockerfile","rb").read()).hexdigest()[:16])')
NODE_IMAGE_FINGERPRINT_LABEL := burla.node-image-fingerprint

# A test shell on a chosen interpreter. `--python` is passed per-run instead of
# pinned, so switching versions never edits the tracked `client/.python-version`.
# Each version gets its own venv so switching back and forth (or running two at
# once) doesn't rebuild the default `.venv` every time.
define TEST_SHELL
	set -e
	uv python install $(1) >/dev/null 2>&1
	$(MAKE) -C main_service ensure-frontend
	UV_PROJECT_ENVIRONMENT=$(PROJECT_ABS)/.venv-$(1) \
		uv run --project $(PROJECT_ABS) --group dev --python $(1) \
			python -m burla._test_shell
endef

.PHONY: 3.11-dev 3.12-dev 3.13-dev 3.14-dev local-dev remote-dev local-images \
	image-seed stop-all cluster-info node-logs test test-service test-e2e \
	test-dashboard kill-kernels

3.11-dev:
	$(call TEST_SHELL,3.11)
3.12-dev:
	$(call TEST_SHELL,3.12)
3.13-dev:
	$(call TEST_SHELL,3.13)
3.14-dev:
	$(call TEST_SHELL,3.14)

# Every tier needs a cluster running for THIS checkout: `make local-dev` (or
# `make remote-dev`) in another terminal. Tests reach it at
# BURLA_CLUSTER_DASHBOARD_URL, defaulted here to this checkout's head port so
# they never talk to another checkout's cluster.
#
# DISABLE_BURLA_TELEMETRY silences the client's telemetry (which the backend
# forwards to Slack) for the pytest process and every rpm subprocess it
# spawns. It does NOT reach the already-running head/nodes/workers: those
# inherited their env from the `make local-dev` that started them, so export
# DISABLE_BURLA_TELEMETRY=True before `make local-dev` to silence those too.
test:
	DISABLE_BURLA_TELEMETRY=True \
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -s --disable-warnings

# Service-level tests. Requires a cluster for this checkout.
test-service:
	DISABLE_BURLA_TELEMETRY=True \
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m service -s --disable-warnings

# End-to-end tests. Requires a cluster for this checkout.
test-e2e:
	DISABLE_BURLA_TELEMETRY=True \
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m e2e -s --disable-warnings

# Dashboard-UI tests, driven through real Chromium. Requires a cluster.
test-dashboard:
	uv run --project ./client --group dev playwright install chromium; \
	DISABLE_BURLA_TELEMETRY=True \
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m dashboard -s --disable-warnings

cluster-info:
	echo "cluster:        $(BURLA_CLUSTER_NAME)"; \
	echo "dashboard:      $(BURLA_DASHBOARD_URL)"; \
	echo "docker network: $(BURLA_CLUSTER_NETWORK)"; \
	echo "node ports:     $(BURLA_NODE_PORT_BASE)+"

# Node logs as the head recorded them, oldest last. Unlike `docker logs node_*`
# these cover remote-dev's EC2 nodes and survive the container being replaced
# between jobs. Usage: make node-logs [NODE=<id substring>] [JOB=<job id>] [N=<lines>]
node-logs:
	BURLA_ENVIRONMENT=$${BURLA_ENVIRONMENT:-test} \
	uv run --project $(PROJECT_ABS) --group dev python -m burla._node_logs \
		--node "$(NODE)" --job "$(JOB)" --lines "$${N:-200}" \
		--namespace "$(BURLA_CLUSTER_NAME)" \
		--local-dev-db "$(PWD)/_local_dev_state/history.db"

# Emergency machine-wide cleanup only. Normal dev clusters clean up their own
# nodes; this removes every local cluster's containers and caches.
stop-all:
	set -e; \
	ids=$$(docker ps -aq --filter label=burla-cluster); \
	if [ -n "$$ids" ]; then docker rm -f -v $$ids >/dev/null; fi; \
	vols=$$(docker volume ls -q --filter label=burla-cluster); \
	if [ -n "$$vols" ]; then docker volume rm $$vols >/dev/null; fi; \
	docker volume rm burla-uv-cache >/dev/null 2>&1 || true; \
	echo "Removed every burla dev cluster on this machine."


# Rebuild local-dev's node base image. `local-dev` does this for you when it's
# missing or stale; run it by hand to pick up new node_service dependencies,
# which the fingerprint can't see (they're installed from a git clone, not from
# this checkout, so an unchanged Dockerfile can still produce a new image).
local-images:
	set -e; \
	docker build -t $(LOCAL_NODE_IMAGE) \
		--label $(NODE_IMAGE_FINGERPRINT_LABEL)=$(NODE_IMAGE_FINGERPRINT) \
		./node_service; \
	echo "Built $(LOCAL_NODE_IMAGE)."

# Each node runs its own docker daemon, so without this tarball every node
# would download the default worker image from the registry at boot. Kept
# current with the registry (when online) because nodes always `docker pull`,
# and a stale seed would make that pull a full re-download instead of a no-op.
# The `.ref` sidecar names the image the tarball holds (for the node, which
# skips loading an image it already has) and its id (for the check below).
image-seed:
	set -e; \
	mkdir -p _image_seed; \
	docker pull -q python:3.12 >/dev/null 2>&1 || true; \
	docker image inspect python:3.12 >/dev/null 2>&1 || docker pull python:3.12; \
	current=$$(docker image inspect -f '{{.Id}}' python:3.12); \
	saved=$$(awk '{print $$2}' _image_seed/python-3.12.ref 2>/dev/null || true); \
	if [ "$$current" != "$$saved" ]; then \
		docker save python:3.12 -o _image_seed/python-3.12.tar; \
		echo "python:3.12 $$current" > _image_seed/python-3.12.ref; \
		echo "Saved python:3.12 seed for node-local docker daemons."; \
	fi

# The whole cluster runs on this machine: the head as a host subprocess
# hot-reloading this checkout (like remote-dev), nodes as privileged "fake VM"
# containers on this checkout's own network, each running its own docker
# daemon with its workers inside it (exactly the prod topology). Uses
# `LOCAL_DEV_CONFIG` in `main_service.__init__.py` (1 node by default; raise
# with LOCAL_DEV_NODE_QUANTITY). Needs a working AWS identity + saved cluster
# token: nodes authorize callers against the backend's user list for this
# cluster id, so a bogus id makes every node fail to boot.
local-dev:
	set -e; \
	if nc -z localhost $(BURLA_HEAD_PORT) 2>/dev/null; then \
		echo ""; \
		echo "ERROR: something is already listening on port $(BURLA_HEAD_PORT), probably this"; \
		echo "       checkout's cluster. Starting another would wipe its live state"; \
		echo "       (_local_dev_state) out from under it."; \
		echo ""; \
		echo "  Fix: stop the existing cluster in its terminal, then re-run make local-dev"; \
		echo ""; \
		exit 1; \
	fi; \
	NODE_IMAGE=$${BURLA_NODE_IMAGE:-$(LOCAL_NODE_IMAGE)}; \
	if [ "$${NODE_IMAGE}" = "$(LOCAL_NODE_IMAGE)" ]; then \
		built_from=$$(docker image inspect \
			-f '{{index .Config.Labels "$(NODE_IMAGE_FINGERPRINT_LABEL)"}}' \
			$${NODE_IMAGE} 2>/dev/null || true); \
		if [ "$${built_from}" != "$(NODE_IMAGE_FINGERPRINT)" ]; then \
			$(MAKE) local-images; \
		fi; \
	fi; \
	$(MAKE) image-seed; \
	$(MAKE) -C main_service ensure-frontend; \
	BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}; \
	AWS_ACCOUNT=$$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true); \
	if [ -z "$${AWS_ACCOUNT}" ] && [ -z "$${BURLA_DEV_PROJECT}" ]; then \
		echo ""; \
		echo "ERROR: no usable AWS identity, so this cluster has no real cluster id"; \
		echo "       and its nodes cannot authorize themselves (they would fail to"; \
		echo "       boot with a 401)."; \
		echo ""; \
		echo "  Fix: aws sso login"; \
		echo ""; \
		exit 1; \
	fi; \
	PROJECT_ID=$${BURLA_DEV_PROJECT:-aws-$${AWS_ACCOUNT}}; \
	TOKEN_FILE=$${XDG_DATA_HOME:-$$HOME/.local/share}/burla-test/clusters/$${PROJECT_ID}/cluster_token; \
	[ -f "$$TOKEN_FILE" ] || TOKEN_FILE=$$HOME/Library/Application\ Support/burla-test/clusters/$${PROJECT_ID}/cluster_token; \
	CLUSTER_ID_TOKEN=$${BURLA_CLUSTER_ID_TOKEN:-$$(cat "$$TOKEN_FILE" 2>/dev/null || true)}; \
	if [ -z "$${CLUSTER_ID_TOKEN}" ]; then \
		echo ""; \
		echo "ERROR: no cluster token saved for [$${PROJECT_ID}], so this cluster's"; \
		echo "       nodes cannot authorize themselves (they would fail to boot with"; \
		echo "       a 401)."; \
		echo ""; \
		echo "  Fix: burla login   (registers this cluster and saves its token)"; \
		echo ""; \
		exit 1; \
	fi; \
	echo "Starting cluster [$(BURLA_CLUSTER_NAME)] at $(BURLA_DASHBOARD_URL) (cluster id $${PROJECT_ID})"; \
	ids=$$(docker ps -aq --filter label=burla-cluster=$(BURLA_CLUSTER_NAME)); \
	if [ -n "$$ids" ]; then docker rm -f $$ids >/dev/null; fi; \
	for scratch in _shared_workspace _node_auth _local_dev_state; do \
		rm -rf ./$$scratch; mkdir -p ./$$scratch; chmod 777 ./$$scratch; \
	done; \
	docker network create $(BURLA_CLUSTER_NETWORK) 2>/dev/null || true; \
	BURLA_ENVIRONMENT=test \
	PROJECT_ID=$${PROJECT_ID} \
	IN_LOCAL_DEV_MODE=True \
	CLOUD_PROVIDER=$${BURLA_CLOUD:-aws} \
	CLUSTER_ID_TOKEN=$${CLUSTER_ID_TOKEN} \
	BURLA_CLUSTER_NAME=$(BURLA_CLUSTER_NAME) \
	LOCAL_DEV_NETWORK=$(BURLA_CLUSTER_NETWORK) \
	LOCAL_DEV_HEAD_HOST=host.docker.internal \
	LOCAL_DEV_NODE_PORT_BASE=$(BURLA_NODE_PORT_BASE) \
	LOCAL_DEV_NODE_QUANTITY=$${LOCAL_DEV_NODE_QUANTITY:-1} \
	BURLA_NODE_IMAGE=$${NODE_IMAGE} \
	BURLA_BACKEND_URL=$${BACKEND_URL} \
	REDIRECT_LOCALLY_ON_LOGIN=True \
	HOST_PWD=$(PWD) \
	PORT=$(BURLA_HEAD_PORT) \
	HISTORY_DB_PATH=$(PWD)/_local_dev_state/history.db \
	uv run --project $(PROJECT_ABS) --group dev python -m burla._local_dev

# `main_service` runs here as a local subprocess hot-reloading this checkout;
# nodes are real cloud VMs: EC2 in the Burla test AWS account by default, or
# Azure VMs with BURLA_CLOUD=azure (uses your active `az` subscription).
# Nodes reach this head through the relay, so many of these run at once on
# one machine. Node VMs cannot see this working tree: they run this
# checkout's branch, so push node_service changes before expecting them here.
remote-dev:
	set -e; \
	$(MAKE) -C main_service ensure-frontend; \
	BURLA_ENVIRONMENT=test \
	BURLA_CLOUD=$${BURLA_CLOUD:-aws} \
	BURLA_CLUSTER_NAME=$(BURLA_CLUSTER_NAME) \
	BURLA_HEAD_PORT=$(BURLA_HEAD_PORT) \
	uv run --project $(PROJECT_ABS) --group dev python -m burla._remote_dev

kill-kernels:
	pkill -f ipykernel
