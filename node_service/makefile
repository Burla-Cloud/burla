.ONESHELL:
.SILENT:

WEBSERVICE_NAME = burla-node-service
PYTHON_MODULE_NAME = node_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)
PROD_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)


service:
	python3.11 -m uvicorn $(PYTHON_MODULE_NAME):app --host 0.0.0.0 --port 8080 --reload \
		--workers 1 --timeout-keep-alive 600 --reload

image:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	TEST_IMAGE_BASE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)" \
	); \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project $${PROJECT_ID} \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_TEST_IMAGE_TAG=$$(($${TEST_IMAGE_TAG} + 1)); \
	TEST_IMAGE_NAME_SEQUENTIAL=$$( echo $${TEST_IMAGE_BASE_NAME}:$${NEW_TEST_IMAGE_TAG} ); \
	TEST_IMAGE_NAME_LATEST=$$( echo $${TEST_IMAGE_BASE_NAME}:latest ); \
	gcloud builds submit --tag $${TEST_IMAGE_NAME_SEQUENTIAL}; \
	docker pull $${TEST_IMAGE_NAME_SEQUENTIAL}; \
	docker tag $${TEST_IMAGE_NAME_SEQUENTIAL} $${TEST_IMAGE_NAME_LATEST}; \
	docker push $${TEST_IMAGE_NAME_LATEST}; \
	echo "Successfully built Docker Image:"; \
	echo "$${TEST_IMAGE_NAME_SEQUENTIAL}"; \
	echo "$${TEST_IMAGE_NAME_LATEST}"; \
	echo "";

container:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	PROJECT_NUM=$$( \
		gcloud projects describe $${PROJECT_ID} --format="value(projectNumber)"  2>/dev/null \
	); \
	TEST_IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME):latest" \
	); \
	docker run --rm -it \
		-v $(PWD):/burla/node_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e IN_DEV=True \
		-e IN_PROD=False \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e PROJECT_ID=$${PROJECT_ID} \
		-e PROJECT_NUM=$${PROJECT_NUM} \
		-p 8080:8080 \
		--entrypoint bash $${TEST_IMAGE_NAME}
