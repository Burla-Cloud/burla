
.ONESHELL:
.SILENT:

ARTIFACT_REPO_NAME = burla-job-containers
PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)

base_image_nogpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="base-image-nogpu";

image_nogpu:
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="image-nogpu"

base_image_gpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="base-image-gpu";

image_gpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="image-gpu";

move-image-nogpu-to-prod:
	set -e; \
	ARTIFACT_PKG_NAME=$$( echo default/image-nogpu ); \
	TEST_IMAGE_BASE_NAME=$$( echo \
		us-docker.pkg.dev/$(PROJECT_ID)/$(ARTIFACT_REPO_NAME)/$${ARTIFACT_PKG_NAME} \
	); \
	PROD_IMAGE_BASE_NAME=$$( echo \
		us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$${ARTIFACT_PKG_NAME} \
	); \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$${ARTIFACT_PKG_NAME} \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=$(PROJECT_ID) \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $${TEST_IMAGE_BASE_NAME}:$${TEST_IMAGE_TAG} ); \
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$${ARTIFACT_PKG_NAME} \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_PROD_IMAGE_TAG=$$(($${PROD_IMAGE_TAG} + 1)); \
	PROD_IMAGE_NAME=$$( echo $${PROD_IMAGE_BASE_NAME}:$${NEW_PROD_IMAGE_TAG} ); \
	docker pull $${TEST_IMAGE_NAME}; \
	docker tag $${TEST_IMAGE_NAME} $${PROD_IMAGE_NAME}; \
	docker tag $${PROD_IMAGE_NAME} $${PROD_IMAGE_BASE_NAME}:latest; \
	docker push $${PROD_IMAGE_NAME}; \
	docker push $${PROD_IMAGE_BASE_NAME}:latest

move-image-gpu-to-prod:
	set -e; \
	ARTIFACT_PKG_NAME=$$( echo default/image-gpu ); \
	TEST_IMAGE_BASE_NAME=$$( echo \
		us-docker.pkg.dev/$(PROJECT_ID)/$(ARTIFACT_REPO_NAME)/$${ARTIFACT_PKG_NAME} \
	); \
	PROD_IMAGE_BASE_NAME=$$( echo \
		us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$${ARTIFACT_PKG_NAME} \
	); \
	TEST_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$${ARTIFACT_PKG_NAME} \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=$(PROJECT_ID) \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $${TEST_IMAGE_BASE_NAME}:$${TEST_IMAGE_TAG} ); \
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$${ARTIFACT_PKG_NAME} \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_PROD_IMAGE_TAG=$$(($${PROD_IMAGE_TAG} + 1)); \
	PROD_IMAGE_NAME=$$( echo $${PROD_IMAGE_BASE_NAME}:$${NEW_PROD_IMAGE_TAG} ); \
	docker pull $${TEST_IMAGE_NAME}; \
	docker tag $${TEST_IMAGE_NAME} $${PROD_IMAGE_NAME}; \
	docker tag $${PROD_IMAGE_NAME} $${PROD_IMAGE_BASE_NAME}:latest; \
	docker push $${PROD_IMAGE_NAME}; \
	docker push $${PROD_IMAGE_BASE_NAME}:latest