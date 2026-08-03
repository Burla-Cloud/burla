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

define UV_ZSH_ENV
	set -e
	uv python install $(1) >/dev/null 2>&1
	uv python pin --project $(PROJECT_ABS) $(1) >/dev/null 2>&1
	uv sync --project $(PROJECT_ABS) --group $(2) >/dev/null 2>&1
	tmp_dir=$$(mktemp -d); \
	printf 'PROMPT="($(1)-$(2)) %%c %%%% "\nexport BURLA_CLUSTER_DASHBOARD_URL=$(BURLA_DASHBOARD_URL)\nexport BURLA_BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}\n' > $$tmp_dir/.zshrc; \
	trap 'rm -rf $$tmp_dir' EXIT; \
	ZDOTDIR=$$tmp_dir uv run --project $(PROJECT_ABS) --group $(2) zsh -i
endef

.PHONY: 3.11-dev 3.12-dev 3.13-dev 3.14-dev 3.11-jupyter 3.12-jupyter 3.13-jupyter 3.14-jupyter test-shell local-dev remote-dev stop stop-all cluster-info

test-shell:
	$(MAKE) -C main_service ensure-frontend
	uv run --project $(PROJECT_ABS) --group dev burla test-shell

3.11-dev:
	$(call UV_ZSH_ENV,3.11,dev)
3.12-dev:
	$(call UV_ZSH_ENV,3.12,dev)
3.13-dev:
	$(call UV_ZSH_ENV,3.13,dev)
3.14-dev:
	$(call UV_ZSH_ENV,3.14,dev)
	
kill-jupyter:
	pkill -f 'ipykernel|jupyter.*kernel'

# Every tier except `test-unit` needs a cluster running for THIS checkout:
# `make local-dev` (or `make remote-dev`) in another terminal. Tests reach it at
# BURLA_CLUSTER_DASHBOARD_URL, defaulted here to this checkout's head port so
# they never talk to another checkout's cluster.
test:
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "not chaos" -s --disable-warnings

# Pure unit tests — the only tier that needs no cluster at all.
test-unit:
	uv run --project ./client --group dev pytest -m unit -s --disable-warnings

# Service-level tests. Requires a cluster for this checkout.
test-service:
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "service and not chaos" -s --disable-warnings

# End-to-end tests. Requires a cluster for this checkout.
test-e2e:
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "e2e and not chaos" -s --disable-warnings

# Chaos (destructive) tests. Run tests one at a time with a cluster reset
# between; they shut down / restart / mutate the cluster.
test-chaos:
	BURLA_CLUSTER_DASHBOARD_URL=$${BURLA_CLUSTER_DASHBOARD_URL:-$(BURLA_DASHBOARD_URL)} \
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m chaos -s --disable-warnings

cluster-info:
	echo "cluster:        $(BURLA_CLUSTER_NAME)"; \
	echo "dashboard:      $(BURLA_DASHBOARD_URL)"; \
	echo "docker network: $(BURLA_CLUSTER_NETWORK)"; \
	echo "node ports:     $(BURLA_NODE_PORT_BASE)+"

# Remove this checkout's cluster containers. Filtered by label so other
# checkouts' clusters on the same docker daemon are left alone. Cluster state
# lives inside the main_service process, so there is nothing else to clean up.
stop:
	set -e; \
	ids=$$(docker ps -aq --filter label=burla-cluster=$(BURLA_CLUSTER_NAME)); \
	if [ -n "$$ids" ]; then docker rm -f $$ids >/dev/null; fi; \
	docker network rm $(BURLA_CLUSTER_NETWORK) >/dev/null 2>&1 || true; \
	echo "Removed cluster [$(BURLA_CLUSTER_NAME)]."

stop-all:
	set -e; \
	ids=$$(docker ps -aq --filter label=burla-cluster); \
	if [ -n "$$ids" ]; then docker rm -f $$ids >/dev/null; fi; \
	echo "Removed every burla dev cluster on this machine."


# The whole cluster (head, nodes, workers) runs locally as docker containers on
# this checkout's own network, all bind-mounted so every service hot-reloads on
# save. Uses `LOCAL_DEV_CONFIG` in `main_service.__init__.py` (1 node by
# default; raise with LOCAL_DEV_NODE_QUANTITY). Needs no cloud credentials.
local-dev:
	set -e; \
	IMAGE_PROJECT=$${BURLA_DEV_IMAGE_PROJECT:-$$(gcloud config get-value project 2>/dev/null || echo burla-test)}; \
	IMAGE_NAME=$${BURLA_MAIN_SERVICE_IMAGE:-us-docker.pkg.dev/$${IMAGE_PROJECT}/burla-main-service/burla-main-service:latest}; \
	NODE_IMAGE=$${BURLA_NODE_IMAGE:-us-docker.pkg.dev/$${IMAGE_PROJECT}/burla-node-service/burla-node-service:latest}; \
	BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}; \
	PROJECT_ID=$${BURLA_DEV_PROJECT:-aws-$$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo local)}; \
	TOKEN_FILE=$${XDG_DATA_HOME:-$$HOME/.local/share}/burla-test/clusters/$${PROJECT_ID}/cluster_token; \
	[ -f "$$TOKEN_FILE" ] || TOKEN_FILE=$$HOME/Library/Application\ Support/burla-test/clusters/$${PROJECT_ID}/cluster_token; \
	CLUSTER_ID_TOKEN=$${BURLA_CLUSTER_ID_TOKEN:-$$(cat "$$TOKEN_FILE" 2>/dev/null || echo local-dev-token)}; \
	echo "Starting cluster [$(BURLA_CLUSTER_NAME)] at $(BURLA_DASHBOARD_URL) (cluster id $${PROJECT_ID})"; \
	ids=$$(docker ps -aq --filter label=burla-cluster=$(BURLA_CLUSTER_NAME)); \
	if [ -n "$$ids" ]; then docker rm -f $$ids >/dev/null; fi; \
	for scratch in _worker_service_python_env _shared_workspace _node_auth; do \
		rm -rf ./$$scratch; mkdir -p ./$$scratch; chmod 777 ./$$scratch; \
	done; \
	docker network create $(BURLA_CLUSTER_NETWORK) 2>/dev/null || true; \
	tty_flag=$$( [ -t 0 ] && echo --tty || true ); \
	docker run --rm --interactive $$tty_flag \
		--name main_service-$(BURLA_CLUSTER_NAME) \
		--label burla-cluster=$(BURLA_CLUSTER_NAME) \
		--network $(BURLA_CLUSTER_NETWORK) \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e PROJECT_ID=$${PROJECT_ID} \
		-e IN_LOCAL_DEV_MODE=True \
		-e CLUSTER_ID_TOKEN=$${CLUSTER_ID_TOKEN} \
		-e BURLA_CLUSTER_NAME=$(BURLA_CLUSTER_NAME) \
		-e LOCAL_DEV_NETWORK=$(BURLA_CLUSTER_NETWORK) \
		-e LOCAL_DEV_HEAD_HOST=main_service-$(BURLA_CLUSTER_NAME) \
		-e LOCAL_DEV_NODE_PORT_BASE=$(BURLA_NODE_PORT_BASE) \
		-e LOCAL_DEV_NODE_QUANTITY=$${LOCAL_DEV_NODE_QUANTITY:-1} \
		-e BURLA_NODE_IMAGE=$${NODE_IMAGE} \
		-e BURLA_BACKEND_URL=$${BACKEND_URL} \
		-e REDIRECT_LOCALLY_ON_LOGIN=True \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 127.0.0.1:$(BURLA_HEAD_PORT):5001 \
		--entrypoint python \
		$${IMAGE_NAME} -m uvicorn main_service:app \
			--host 0.0.0.0 \
			--port 5001 \
			--reload --reload-exclude main_service/frontend/node_modules/ \
			--timeout-keep-alive 600 \
			--timeout-graceful-shutdown 0

# `main_service` runs here as a local subprocess hot-reloading this checkout;
# nodes are real EC2 instances in the Burla test AWS account. Nodes reach this
# head through the relay, so many of these run at once on one machine.
# Node VMs cannot see this working tree: they run this checkout's branch, so
# push node_service changes before expecting them here.
remote-dev:
	set -e; \
	$(MAKE) -C main_service ensure-frontend; \
	BURLA_ENVIRONMENT=test \
	BURLA_CLOUD=aws \
	BURLA_CLUSTER_NAME=$(BURLA_CLUSTER_NAME) \
	uv run --project $(PROJECT_ABS) --group dev burla remote-dev

dev-images:
	set -e; \
	$(MAKE) -C ./main_service dev-image; \
	$(MAKE) -C ./node_service dev-image

kill-kernels:
	pkill -f ipykernel
