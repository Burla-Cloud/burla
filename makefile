.ONESHELL:
.SILENT:

UV_PROJECT := ./client
PROJECT_ABS := $(abspath $(UV_PROJECT))

define UV_ZSH_ENV
	set -e
	uv python install $(1) >/dev/null 2>&1
	uv python pin --project $(PROJECT_ABS) $(1) >/dev/null 2>&1
	rm -rf $(PROJECT_ABS)/.venv
	uv sync --project $(PROJECT_ABS) --group $(2) >/dev/null 2>&1
	tmp_dir=$$(mktemp -d); \
	printf 'PROMPT="($(1)-$(2)) %%c %%%% "\n' > $$tmp_dir/.zshrc; \
	ZDOTDIR=$$tmp_dir exec uv run --project $(PROJECT_ABS) --group $(2) zsh -i
endef

define UV_JUPYTER_ENV
	set -e
	uv python install $(1) >/dev/null 2>&1
	uv python pin --project $(PROJECT_ABS) $(1) >/dev/null 2>&1
	rm -rf $(PROJECT_ABS)/.venv
	uv sync --project $(PROJECT_ABS) --group dev >/dev/null 2>&1
	cd .. && exec uv run --project $(PROJECT_ABS) --group dev jupyter-lab --NotebookApp.disable_checkpoints=True
endef

.PHONY: 3.11-dev 3.12-dev 3.13-dev 3.14-dev 3.11-jupyter 3.12-jupyter 3.13-jupyter 3.14-jupyter

3.11-dev:
	$(call UV_ZSH_ENV,3.11,dev)
3.12-dev:
	$(call UV_ZSH_ENV,3.12,dev)
3.13-dev:
	$(call UV_ZSH_ENV,3.13,dev)
3.14-dev:
	$(call UV_ZSH_ENV,3.14,dev)

3.11-jupyter:
	$(call UV_JUPYTER_ENV,3.11)
3.12-jupyter:
	$(call UV_JUPYTER_ENV,3.12)
3.13-jupyter:
	$(call UV_JUPYTER_ENV,3.13)
3.14-jupyter:
	$(call UV_JUPYTER_ENV,3.14)


test:
	pytest client/tests/test.py -s -x --disable-warnings

# remove all booting nodes from DB (only run in local-dev mode)
stop:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	export PROJECT_ID=$${PROJECT_ID}; \
	printf '%s\n' \
		'import os' \
		'import json' \
		'from google.cloud import firestore' \
		'from google.cloud.firestore_v1 import FieldFilter' \
		'' \
		'db = firestore.Client(project=os.environ["PROJECT_ID"], database="burla")' \
		'booting_filter = FieldFilter("status", "==", "BOOTING")' \
		'booting_nodes = db.collection("nodes").where(filter=booting_filter).get()' \
		'if not booting_nodes:' \
		'    print("No booting nodes found")' \
		'else:' \
		'    for document in booting_nodes:' \
		'        document.reference.delete()' \
		'        print(f"Deleted node doc: {document.id}")' \
	| poetry -C ./client run python -


# start ONLY the main service, in local dev mode
# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, worker_svc) will restart when changes to code are made.
local-dev:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
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
	echo "Starting local dev"; \
	docker network create local-burla-cluster 2>/dev/null || true; \
	gcloud auth print-access-token > .temp_token.txt; \
	docker run --rm -it \
		--name main_service \
		--network local-burla-cluster \
		-v $$(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e IN_LOCAL_DEV_MODE=True \
		-e REDIRECT_LOCALLY_ON_LOGIN=True \
		-e HOST_PWD=$$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 5001:5001 \
		--entrypoint python \
		$${IMAGE_NAME} -m uvicorn main_service:app \
			--host 0.0.0.0 \
			--port 5001 \
			--reload --reload-exclude main_service/frontend/node_modules/ \
			--timeout-keep-alive 600 \
			--timeout-graceful-shutdown 0

# Only the `main_service` is run locally, nodes are started as GCE VM's in the test cloud.
# Uses cluster config from firestore doc: `/databases/burla/cluster_config/cluster_config`
remote-dev:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/burla-main-service/burla-main-service:latest" \
	); \
	$(MAKE) __check-node-service-up-to-date && echo "" || exit 1; \
	:; \
	docker run --rm -it \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e REDIRECT_LOCALLY_ON_LOGIN=True \
		-p 5001:5001 \
		--entrypoint python \
		$${IMAGE_NAME} -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/ --timeout-graceful-shutdown 0

# raise error if local node service is different from remote-dev version
# does the node service have a git diff?
__check-node-service-up-to-date:
	if [ "$${NODE_SVC_HAS_DIFF}" = "true" ]; then \
		echo "DEPLOYED NODE SERVICE NOT UP TO DATE!"; \
		echo "Your local node service is different from the cluster's node service."; \
		echo "To fix this, commit your node service code to the latest release branch."; \
		exit 1; \
	fi; \
	echo "deployed node service up to date with local version.";

deploy-test:
	set -e; \
	$(MAKE) __check-node-service-up-to-date && echo "" || exit 1; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) deploy-test; \

deploy-prod:
	set -e; \
	$(MAKE) __check-node-service-up-to-date && echo "" || exit 1; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) publish;
