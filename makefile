.ONESHELL:
.SILENT:

UV_PROJECT := ./client
PROJECT_ABS := $(abspath $(UV_PROJECT))

define UV_ZSH_ENV
	set -e
	uv python install $(1) >/dev/null 2>&1
	uv python pin --project $(PROJECT_ABS) $(1) >/dev/null 2>&1
	uv sync --project $(PROJECT_ABS) --group $(2) >/dev/null 2>&1
	tmp_dir=$$(mktemp -d); \
	printf 'PROMPT="($(1)-$(2)) %%c %%%% "\nexport BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001\nexport BURLA_BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}\n' > $$tmp_dir/.zshrc; \
	trap 'rm -rf $$tmp_dir' EXIT; \
	ZDOTDIR=$$tmp_dir uv run --project $(PROJECT_ABS) --group $(2) zsh -i
endef

.PHONY: 3.11-dev 3.12-dev 3.13-dev 3.14-dev 3.11-jupyter 3.12-jupyter 3.13-jupyter 3.14-jupyter test-shell

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

# DEV-VM ONLY: every target below except `test-unit` must run on an
# ephemeral dev VM (see scripts/dev_vm_create.sh and
# .cursor/skills/burla-ephemeral-dev-vm/). Running `make test*` on a
# laptop is unsupported — local-dev's Docker-in-Docker, Firestore
# access, and bind-mount layout only work reliably inside a dev VM.
# If you are an agent, read client/tests/README.md before invoking.
test:
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "not chaos" -s --disable-warnings

# Pure unit tests — the only tier safe to run outside a dev VM.
test-unit:
	uv run --project ./client --group dev pytest -m unit -s --disable-warnings

# Service-level tests. DEV VM ONLY. Requires `make local-dev` running on the VM.
test-service:
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "service and not chaos" -s --disable-warnings

# End-to-end tests. DEV VM ONLY. Requires `make local-dev` running on the VM.
test-e2e:
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m "e2e and not chaos" -s --disable-warnings

# Chaos (destructive) tests. DEV VM ONLY. Run tests one at a time with a
# cluster reset between; they shut down / restart / mutate the cluster.
test-chaos:
	BURLA_REQUIRE_CLUSTER=1 uv run --project ./client --group dev pytest -m chaos -s --disable-warnings

# kill all local-dev cluster containers. Cluster state lives inside the
# main_service process, so there is nothing else to clean up.
stop:
	set -e; \
	ids=$$(docker ps -a --format '{{.Names}} {{.ID}}' | awk '$$1 ~ /^(node_|worker_|OLD--)/ {print $$2}'); \
	if [ -n "$$ids" ]; then docker rm -f $$ids; fi; \
	echo "Removed all node_* / worker_* containers."


# start ONLY the main service, in local dev mode
# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, worker_svc) will restart when changes to code are made.
local-dev:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}; \
	TOKEN_SECRET=$${BURLA_CLUSTER_TOKEN_SECRET:-burla-cluster-id-token}; \
	IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/burla-main-service/burla-main-service:latest" \
	); \
	echo "Killing all node_* and worker_* containers"; \
	ids=$$(docker ps -a --format '{{.Names}} {{.ID}}' | awk '$$1 ~ /^(node_|worker_)/ {print $$2}'); \
	if [ -n "$$ids" ]; then docker rm -f $$ids; fi; \
	echo "Removing _worker_service_python_env"; \
	rm -rf ./_worker_service_python_env; \
	mkdir -p ./_worker_service_python_env; \
	chmod 777 ./_worker_service_python_env; \
	echo "Removing _shared_workspace"; \
	rm -rf ./_shared_workspace; \
	mkdir -p ./_shared_workspace; \
	chmod 777 ./_shared_workspace; \
	echo "Removing _node_auth"; \
	rm -rf ./_node_auth; \
	mkdir -p ./_node_auth; \
	chmod 777 ./_node_auth; \
	echo "Starting local dev"; \
	docker network create local-burla-cluster 2>/dev/null || true; \
	CLUSTER_ID_TOKEN=$$(gcloud secrets versions access latest --secret=$${TOKEN_SECRET} 2>/dev/null || echo local-dev-token); \
	docker run --rm -it \
		--name main_service \
		--network local-burla-cluster \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e IN_LOCAL_DEV_MODE=True \
		-e CLUSTER_ID_TOKEN=$${CLUSTER_ID_TOKEN} \
		-e BURLA_BACKEND_URL=$${BACKEND_URL} \
		-e REDIRECT_LOCALLY_ON_LOGIN=True \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 127.0.0.1:5001:5001 \
		--entrypoint python \
		$${IMAGE_NAME} -m uvicorn main_service:app \
			--host 0.0.0.0 \
			--port 5001 \
			--reload --reload-exclude main_service/frontend/node_modules/ \
			--timeout-keep-alive 600 \
			--timeout-graceful-shutdown 0

# Only the `main_service` is run locally, nodes are started as GCE VM's in the test cloud.
# Node VMs cannot see this working tree, so they run the `dev` branch: commit and
# push node_service changes before expecting them here.
# Uses the cluster config stored in the head's history db (a fresh one is
# seeded on first boot). CLUSTER_ID_TOKEN comes from Burla's local state dir
# where `burla deploy` saves it (Secret Manager is the pre-1.7 fallback).
remote-dev:
	set -e; \
	trap 'docker rm -f main_service burla-head-caddy >/dev/null 2>&1 || true' EXIT; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	BACKEND_URL=$${BURLA_BACKEND_URL:-https://test.backend.burla.dev}; \
	TOKEN_SECRET=$${BURLA_CLUSTER_TOKEN_SECRET:-burla-cluster-id-token}; \
	TOKEN_FILE=$${XDG_DATA_HOME:-$$HOME/.local/share}/burla/clusters/$${PROJECT_ID}/cluster_token; \
	[ -f "$$TOKEN_FILE" ] || TOKEN_FILE=$$HOME/Library/Application\ Support/burla/clusters/$${PROJECT_ID}/cluster_token; \
	CLUSTER_ID_TOKEN=$$(cat "$$TOKEN_FILE" 2>/dev/null || gcloud secrets versions access latest --secret=$${TOKEN_SECRET}); \
	IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/burla-main-service/burla-main-service:latest" \
	); \
	mkdir -p ./_history_db; \
	docker rm -f main_service burla-head-caddy >/dev/null 2>&1 || true; \
	docker run --rm -d \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v $(PWD)/_history_db:/var/lib/burla \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e CLUSTER_ID_TOKEN=$${CLUSTER_ID_TOKEN} \
		-e BURLA_BACKEND_URL=$${BACKEND_URL} \
		-e BURLA_RELAY_HOST=$${BURLA_RELAY_HOST:-relay.test-clusters.burla.dev} \
		-e BURLA_NODE_SOURCE_REF=$${BURLA_NODE_SOURCE_REF:-dev} \
		-e REDIRECT_LOCALLY_ON_LOGIN=True \
		-p 127.0.0.1:5001:5001 \
		--entrypoint python \
		$${IMAGE_NAME} -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/ --timeout-graceful-shutdown 0; \
	while [ ! -f ./_history_db/tls/head.pem ]; do sleep 1; done; \
	printf ':8443 {\n  tls /etc/burla/tls/head.pem /etc/burla/tls/head.key\n  reverse_proxy 127.0.0.1:5001\n}\n' \
		> ./_history_db/Caddyfile; \
	docker run -d --network=host --name=burla-head-caddy \
		-v $(PWD)/_history_db/Caddyfile:/etc/caddy/Caddyfile:ro \
		-v $(PWD)/_history_db/tls/head.pem:/etc/burla/tls/head.pem:ro \
		-v $(PWD)/_history_db/tls/head.key:/etc/burla/tls/head.key:ro \
		caddy:2.10.2-alpine caddy run --config /etc/caddy/Caddyfile --adapter caddyfile; \
	docker logs -f main_service

dev-images:
	set -e; \
	$(MAKE) -C ./main_service dev-image; \
	$(MAKE) -C ./node_service dev-image

kill-kernels:
	pkill -f ipykernel
