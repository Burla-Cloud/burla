.ONESHELL:
.SILENT:

PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)
PROJECT_NUM := $(shell gcloud projects describe $(PROJECT_ID) --format="value(projectNumber)")
ACCESS_TOKEN := $(shell gcloud auth print-access-token)
MAIN_SVC_IMAGE_NAME := us-docker.pkg.dev/$(PROJECT_ID)/burla-main-service/burla-main-service:latest


test-local:
	poetry -C ./client run pytest ./client/tests/test_in_local_dev_mode.py -s -x --disable-warnings

test-jupyter:
	poetry -C ./client run jupyter-lab

test-remote:
	poetry -C ./client run pytest ./client/tests/test_in_remote_dev_mode.py -s -x --disable-warnings

# start ONLY the main service, in local dev mode
# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, worker_svc) will restart when changes to code are made.
local-dev:
	set -e; \
	docker network create local-burla-cluster 2>/dev/null || true; \
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
		--entrypoint python3.11 \
		$(MAIN_SVC_IMAGE_NAME) -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/

# raise error if local node/worker services are different from remote-dev versions
# does the worker service have a git diff since AFTER the last image was pushed?
# does the node service have a git diff?
__check-local-services-up-to-date:
	set -e; \
	WORKER_SVC_TS=$$(cat ./worker_service/last_image_pushed_at.txt); \
	WORKER_SVC_DIR="./worker_service/src/worker_service"; \
	WORKER_SVC_DIFF=$$(git diff --stat "@{$${WORKER_SVC_TS}}" -- "$${WORKER_SVC_DIR}"); \
	WORKER_SVC_HAS_DIFF=$$(echo "$${WORKER_SVC_DIFF}" | grep -q . && echo "true" || echo "false"); \
	NODE_SVC_DIFF=$$(git diff -- "./node_service/src/node_service"); \
	NODE_SVC_HAS_DIFF=$$(echo "$${NODE_SVC_DIFF}" | grep -q . && echo "true" || echo "false"); \
	if [ "$${WORKER_SVC_HAS_DIFF}" = "true" ]; then \
		echo "DEPLOYED CONTAINER SERVICE NOT UP TO DATE!"; \
		echo "Your local worker service is different from the cluster's worker service."; \
		echo "To fix this, run 'make image_nogpu' from './worker_service'."; \
	fi; \
	if [ "$${NODE_SVC_HAS_DIFF}" = "true" ]; then \
		echo "DEPLOYED NODE SERVICE NOT UP TO DATE!"; \
		echo "Your local node service is different from the cluster's node service."; \
		echo "To fix this, commit your node service code to the latest release branch."; \
	fi; \
	if [ "$${WORKER_SVC_HAS_DIFF}" = "true" ] || [ "$${NODE_SVC_HAS_DIFF}" = "true" ]; then \
		exit 1; \
	fi; \
	echo "deployed worker service and node service are up to date with local versions.";


# Only the `main_service` is run locally, nodes are started as GCE VM's in the test cloud.
# Uses cluster config from firestore doc: `/databases/burla/cluster_config/cluster_config`
remote-dev:
	set -e; \
	$(MAKE) __check-local-services-up-to-date && echo "" || exit 1; \
	:; \
	docker run --rm -it \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$(PROJECT_ID) \
		-p 5001:5001 \
		--entrypoint python3.11 \
		$(MAIN_SVC_IMAGE_NAME) -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload \
			--reload-exclude main_service/frontend/node_modules/

# Moves latest worker service image to prod & 
# Builds new main-service image, moves to prod, then deploys prod main service
deploy-prod:
	set -e; \
	$(MAKE) __check-local-services-up-to-date && echo "" || exit 1; \
	:; \
	cd ./worker_service; \
	$(MAKE) image; \
	$(MAKE) publish-prod-image; \
	cd ..; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) deploy-prod

deploy-test:
	set -e; \
	$(MAKE) __check-local-services-up-to-date && echo "" || exit 1; \
	:; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) deploy-test

