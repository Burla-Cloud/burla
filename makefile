.ONESHELL:
.SILENT:

PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)
PROJECT_NUM := $(shell gcloud projects describe $(PROJECT_ID) --format="value(projectNumber)")
ACCESS_TOKEN := $(shell gcloud auth print-access-token)
MAIN_SVC_IMAGE_NAME := us-docker.pkg.dev/$(PROJECT_ID)/burla-main-service/burla-main-service:latest


demo:
	poetry -C ./client run python examples/basic.py

shell:
	poetry -C ./client shell

test:
	poetry -C ./client run pytest client/tests/test.py -s -x --disable-warnings

test-jupyter:
	poetry -C ./client run jupyter-lab

# remove all booting nodes from DB (only run in local-dev mode)
stop:
	printf '%s\n' \
		'import json' \
		'from google.cloud import firestore' \
		'from google.cloud.firestore_v1 import FieldFilter' \
		'from appdirs import user_config_dir' \
		'from pathlib import Path' \
		'' \
		'appdata_dir = Path(user_config_dir(appname="burla", appauthor="burla"))' \
		'config_path = appdata_dir / Path("burla_credentials.json")' \
		'project_id = json.loads(config_path.read_text())["project_id"]' \
		'db = firestore.Client(project=project_id, database="burla")' \
		'booting_filter = FieldFilter("status", "==", "BOOTING")' \
		'for document in db.collection("nodes").where(filter=booting_filter).get():' \
		'    document.reference.delete()' \
		'    print(f"Deleted node doc: {document.id}")' \
	| poetry -C ./client run python -

# start ONLY the main service, in local dev mode
# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, worker_svc) will restart when changes to code are made.
local-dev:
	set -e; \
	docker network create local-burla-cluster 2>/dev/null || true; \
	gcloud auth print-access-token > .temp_token.txt; \
	docker run --rm -it \
		--name main_service \
		--network local-burla-cluster \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e GOOGLE_CLOUD_PROJECT=$(PROJECT_ID) \
		-e IN_LOCAL_DEV_MODE=True \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 5001:5001 \
		--entrypoint python3.13 \
		$(MAIN_SVC_IMAGE_NAME) -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/ --timeout-keep-alive 600

# Only the `main_service` is run locally, nodes are started as GCE VM's in the test cloud.
# Uses cluster config from firestore doc: `/databases/burla/cluster_config/cluster_config`
remote-dev:
	set -e; \
	$(MAKE) __check-node-service-up-to-date && echo "" || exit 1; \
	:; \
	docker run --rm -it \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$(PROJECT_ID) \
		-p 5001:5001 \
		--entrypoint python3.13 \
		$(MAIN_SVC_IMAGE_NAME) -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/

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

deploy-prod:
	set -e; \
	$(MAKE) __check-node-service-up-to-date && echo "" || exit 1; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) publish;
