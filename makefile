.ONESHELL:
.SILENT:

# The cluster is run 100% locally using the config `LOCAL_DEV_CONFIG` in `main_service.__init__.py`
# All components (main_svc, node_svc, container_svc) will restart when changes to code are made.
local-dev-cluster:
	PROJECT_ID=$$(gcloud config get-value project) \
	PROJECT_NUM=$$(gcloud projects describe $${PROJECT_ID} --format="value(projectNumber)") \
	ACCESS_TOKEN=$$(gcloud auth print-access-token); \
	MAIN_SVC_IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/burla-main-service/burla-main-service:latest" \
	); \
	docker network create local-burla-cluster; \
	docker run --rm -it \
		--name main_service \
		--network local-burla-cluster \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e ACCESS_TOKEN=$${ACCESS_TOKEN} \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e PROJECT_ID=$${PROJECT_ID} \
		-e PROJECT_NUM=$${PROJECT_NUM} \
		-e IN_LOCAL_DEV_MODE=True \
		-e IN_PROD=False \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 5001:5001 \
		--entrypoint poetry $${MAIN_SVC_IMAGE_NAME} run \
			uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload

# Only the `main_service` is run locally, nodes are started as GCE VM's in the test cloud.
# Uses cluster config from firestore doc: `/databases/(default)/cluster_config/cluster_config`
remote-dev-cluster:
	PROJECT_ID=$$(gcloud config get-value project) \
	PROJECT_NUM=$$(gcloud projects describe $${PROJECT_ID} --format="value(projectNumber)") \
	MAIN_SVC_IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/burla-main-service/burla-main-service:latest" \
	); \
	docker run --rm -it \
		--name main_service \
		-v $(PWD)/main_service:/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e PROJECT_ID=$${PROJECT_ID} \
		-e PROJECT_NUM=$${PROJECT_NUM} \
		-e IN_REMOTE_DEV_MODE=True \
		-e IN_PROD=False \
		-p 5001:5001 \
		--entrypoint poetry $${MAIN_SVC_IMAGE_NAME} run \
			uvicorn main_service:app --host 0.0.0.0 --port 5001 --reload
