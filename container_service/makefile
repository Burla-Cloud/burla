
.ONESHELL:
.SILENT:

ARTIFACT_REPO_NAME = burla-job-containers
PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)

base_image_nogpu:
	set -e; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="base-image-nogpu";

# This adds a layer on top of `base_image_nogpu` that only updates the baked-in code with new code.
# This exists as a separate image so we can update the baked-in code quickly.
image_nogpu:
	echo -n $$(date "+%Y-%m-%d %H:%M:%S") > last_image_pushed_at.txt; \
	gcloud builds submit . \
		--config cloudbuild.yml \
		--substitutions _IMAGE_NAME="image-nogpu"

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
