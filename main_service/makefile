.ONESHELL:
.SILENT:

WEBSERVICE_NAME = burla-main-service
PYTHON_MODULE_NAME = main_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)
PROD_IMAGE_BASE_NAME := us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)

test:
	poetry run pytest -s --disable-warnings

service:
	poetry run uvicorn $(PYTHON_MODULE_NAME):application --host 0.0.0.0 --port 5001 --reload

restart_dev_cluster:
	curl -X POST http://127.0.0.1:5001/v1/cluster/restart

restart_prod_cluster:
	curl -X POST -H "Content-Length: 0" https://cluster.burla.dev/v1/cluster/restart

deploy-test:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	PROJECT_NUM=$$( \
		gcloud projects describe $${PROJECT_ID} --format="value(projectNumber)"  2>/dev/null \
	); \
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
	--set-env-vars PROJECT_ID=$${PROJECT_ID},PROJECT_NUM=$${PROJECT_NUM} \
	--min-instances 0 \
	--max-instances 5 \
	--memory 2Gi \
	--cpu 1 \
	--timeout 360 \
	--concurrency 20 \
	--allow-unauthenticated

move-test-image-to-prod:
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
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project=burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_PROD_IMAGE_TAG=$$(($${PROD_IMAGE_TAG} + 1)); \
	PROD_IMAGE_NAME=$$( echo $(PROD_IMAGE_BASE_NAME):$${NEW_PROD_IMAGE_TAG} ); \
	docker pull $${TEST_IMAGE_NAME}; \
	docker tag $${TEST_IMAGE_NAME} $${PROD_IMAGE_NAME}; \
	docker push $${PROD_IMAGE_NAME}

deploy-prod:
	set -e; \
	echo ; \
	echo HAVE YOU MOVED THE LATEST TEST-IMAGE TO PROD?; \
	while true; do \
		read -p "Do you want to continue? (yes/no): " yn; \
		case $$yn in \
			[Yy]* ) echo "Continuing..."; break;; \
			[Nn]* ) echo "Exiting..."; exit;; \
			* ) echo "Please answer yes or no.";; \
		esac; \
	done; \
	PROD_IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=$(ARTIFACT_PKG_NAME) \
			--location=us \
			--repository=$(ARTIFACT_REPO_NAME) \
			--project burla-prod \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	PROD_IMAGE_NAME=$$( echo $(PROD_IMAGE_BASE_NAME):$${PROD_IMAGE_TAG} ); \
	gcloud run deploy $(WEBSERVICE_NAME) \
	--image=$${PROD_IMAGE_NAME} \
	--project burla-prod \
	--region=us-central1 \
	--min-instances 1 \
	--max-instances 20 \
	--memory 4Gi \
	--cpu 1 \
	--timeout 360 \
	--concurrency 20 \
	--allow-unauthenticated

deploy-dev-to-prod:
	set -e; \
	echo ; \
	echo This will deploy the current local project DIRECTLY TO PROD!; \
	echo ARE YOU SURE YOU WANT TO DO THIS?; \
	echo ; \
	while true; do \
		read -p "Do you want to continue? (yes/no): " yn; \
		case $$yn in \
			[Yy]* ) echo "Continuing..."; break;; \
			[Nn]* ) echo "Exiting..."; exit;; \
			* ) echo "Please answer yes or no.";; \
		esac; \
	done; \
	$(MAKE) image; \
	$(MAKE) move-test-image-to-prod; \
	echo "yes" | $(MAKE) deploy-prod

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
	TEST_IMAGE_NAME=$$( echo $${TEST_IMAGE_BASE_NAME}:$${NEW_TEST_IMAGE_TAG} ); \
	gcloud builds submit --tag $${TEST_IMAGE_NAME}; \
	echo "Successfully built Docker Image:"; \
	echo "$${TEST_IMAGE_NAME}"; \
	echo "";

container:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	PROJECT_NUM=$$( \
		gcloud projects describe $${PROJECT_ID} --format="value(projectNumber)"  2>/dev/null \
	); \
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
	TEST_IMAGE_NAME=$$( echo $${TEST_IMAGE_BASE_NAME}:$${TEST_IMAGE_TAG} ); \
	docker run --rm -it \
		-v $(PWD):/home/pkg_dev/app \
		-v ~/.gitconfig:/home/pkg_dev/.gitconfig \
		-v ~/.ssh/id_rsa:/home/pkg_dev/.ssh/id_rsa \
		-v ~/.config/gcloud:/home/pkg_dev/.config/gcloud \
		-e IN_DEV=True \
		-e IN_PROD=False \
		-e GOOGLE_CLOUD_PROJECT=$${PROJECT_ID} \
		-e PROJECT_ID=$${PROJECT_ID} \
		-e PROJECT_NUM=$${PROJECT_NUM} \
		-p 5001:5001 \
		--entrypoint poetry $${TEST_IMAGE_NAME} run bash
