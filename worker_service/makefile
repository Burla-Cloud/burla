.ONESHELL:
.SILENT:

WEBSERVICE_NAME = burla-worker-service
PYTHON_MODULE_NAME = worker_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)
PROD_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)


service:
	python3.11 -m gunicorn -t 60 -b 0.0.0.0:8080 worker_service:app


publish-prod-image:
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
	TEST_IMAGE_NAME=$${TEST_IMAGE_BASE_NAME}:$${TEST_IMAGE_TAG}; \
	DOCKERHUB_TOKEN=$$( \
		gcloud secrets versions access latest \
			--secret=jakes-docker-access-token \
			--project=burla-prod \
	); \
	echo $${DOCKERHUB_TOKEN} | docker login -u jakezuliani --password-stdin; \
	:; \
	docker tag $${TEST_IMAGE_NAME} jakezuliani/burla_worker_service:$${TEST_IMAGE_TAG}; \
	docker push jakezuliani/burla_worker_service:$${TEST_IMAGE_TAG}; \
	docker tag $${TEST_IMAGE_NAME} jakezuliani/burla_worker_service:latest; \
	docker push jakezuliani/burla_worker_service:latest


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
	docker build -t $${TEST_IMAGE_NAME_SEQUENTIAL} .; \
	docker push $${TEST_IMAGE_NAME_SEQUENTIAL}; \
	docker tag $${TEST_IMAGE_NAME_SEQUENTIAL} $${TEST_IMAGE_NAME_LATEST}; \
	docker push $${TEST_IMAGE_NAME_LATEST}; \
	echo "Successfully built Docker Image:"; \
	echo "$${TEST_IMAGE_NAME_SEQUENTIAL}"; \
	echo "$${TEST_IMAGE_NAME_LATEST}"; \
	echo "";

container:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	TEST_IMAGE_NAME=$$( echo \
		"us-docker.pkg.dev/$${PROJECT_ID}/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME):latest" \
	); \
	docker run --rm -it \
		--name worker_service \
		-v $(PWD):/burla/worker_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e IN_LOCAL_DEV_MODE=True \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 8080:8080 \
		$${TEST_IMAGE_NAME} bash
