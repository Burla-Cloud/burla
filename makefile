.ONESHELL:
.SILENT:

PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)
PROJECT_NUM := $(shell gcloud projects describe $(PROJECT_ID) --format="value(projectNumber)")
ACCESS_TOKEN := $(shell gcloud auth print-access-token)
MAIN_SVC_IMAGE_NAME := us-docker.pkg.dev/$(PROJECT_ID)/burla-main-service/burla-main-service:latest


test-local:
	poetry -C ./client run pytest ./client/tests/test_in_local_dev_mode.py -s --disable-warnings

test-remote:
	poetry -C ./client run pytest ./client/tests/test_in_remote_dev_mode.py -s --disable-warnings

# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, worker_svc) will restart when changes to code are made.
local-dev-cluster:
	docker network create local-burla-cluster; \
	docker run --rm -it \
		--name main_service \
		--network local-burla-cluster \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e ACCESS_TOKEN=$(ACCESS_TOKEN) \
		-e GOOGLE_CLOUD_PROJECT=$(PROJECT_ID) \
		-e PROJECT_ID=$(PROJECT_ID) \
		-e PROJECT_NUM=$(PROJECT_NUM) \
		-e IN_LOCAL_DEV_MODE=True \
		-e IN_PROD=False \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 5001:5001 \
		--entrypoint poetry \
		$(MAIN_SVC_IMAGE_NAME) run uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload

# private recipe,
# exits with error if deployed worker service or node service are not up to date with local
__check-local-services-up-to-date:
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
# Uses cluster config from firestore doc: `/databases/(default)/cluster_config/cluster_config`
remote-dev-cluster:
	$(MAKE) __check-local-services-up-to-date && echo "" || exit 1; \
	:; \
	docker run --rm -it \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$(PROJECT_ID) \
		-e PROJECT_ID=$(PROJECT_ID) \
		-e PROJECT_NUM=$(PROJECT_NUM) \
		-e IN_REMOTE_DEV_MODE=True \
		-e IN_PROD=False \
		-p 5001:5001 \
		--entrypoint poetry \
		$(MAIN_SVC_IMAGE_NAME) run uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload

# Moves latest worker service image to prod & 
# Builds new main-service image, moves to prod, then deploys prod main service
deploy-prod:
	$(MAKE) __check-local-services-up-to-date && echo "" || exit 1; \
	:; \
	cd ./worker_service; \
	$(MAKE) move-image-nogpu-to-prod; \
	cd ..; \
	cd ./main_service; \
	$(MAKE) image; \
	$(MAKE) move-test-image-to-prod; \
	$(MAKE) deploy-prod

