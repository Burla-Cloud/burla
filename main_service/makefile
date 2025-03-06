.ONESHELL:
.SILENT:

WEBSERVICE_NAME = burla-main-service
PYTHON_MODULE_NAME = main_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)
PROD_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)

.PHONY: frontend
build-frontend:
	set -e; \
	cd ./frontend; \
	npm i; \
	npm run build; \
	rsync -a --delete dist/ ../src/main_service/static/; \
	cd ..; \
	printf "$$(date +%s)" > .frontend_last_built_at.txt

deploy-test:
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
			--project=$${PROJECT_ID} \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	TEST_IMAGE_NAME=$$( echo $${TEST_IMAGE_BASE_NAME}:$${TEST_IMAGE_TAG} ); \
	gcloud run deploy $(WEBSERVICE_NAME) \
		--image=$${TEST_IMAGE_NAME} \
		--project $${PROJECT_ID} \
		--region=us-central1 \
		--min-instances 0 \
		--max-instances 5 \
		--memory 2Gi \
		--cpu 1 \
		--timeout 360 \
		--concurrency 20 \
		--allow-unauthenticated; \
	echo $${PROJECT_ID}; \
	gcloud run services update-traffic $(WEBSERVICE_NAME) \
		--project $${PROJECT_ID} \
		--region=us-central1 \
		--to-latest

deploy-prod:
	gcloud run deploy $(WEBSERVICE_NAME) \
		--image=jakezuliani/burla_main_service:latest \
		--project burla-prod \
		--region=us-central1 \
		--min-instances 1 \
		--max-instances 20 \
		--memory 4Gi \
		--cpu 1 \
		--timeout 360 \
		--concurrency 20 \
		--allow-unauthenticated; \
	gcloud run services update-traffic $(WEBSERVICE_NAME) \
		--project burla-prod \
		--region=us-central1 \
		--to-latest

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
	docker tag $${TEST_IMAGE_NAME} jakezuliani/burla_main_service:$${TEST_IMAGE_TAG}; \
	docker tag $${TEST_IMAGE_NAME} jakezuliani/burla_main_service:latest; \
	docker push jakezuliani/burla_main_service:$${TEST_IMAGE_TAG} jakezuliani/burla_main_service:latest

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
	docker build --platform linux/amd64 -t $${TEST_IMAGE_NAME_SEQUENTIAL} .; \
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
		--name main_service \
		-v $(PWD):/burla/main_service \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e IN_LOCAL_DEV_MODE=True \
		-e HOST_PWD=$(PWD) \
		-e HOST_HOME_DIR=$${HOME} \
		-p 5001:5001 \
		--entrypoint poetry $${TEST_IMAGE_NAME} run bash
