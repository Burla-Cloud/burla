.ONESHELL:
.SILENT:

PYTHON_VERSIONS = 3.10 3.11 3.12 3.13
REPO = cluster-default


service:
	python3.11 -m gunicorn -t 60 -b 0.0.0.0:8080 worker_service:app


publish:
	set -e; \
	VERSIONS="$(PYTHON_VERSIONS)"; \
	printf "%s\n" $$VERSIONS | parallel --line-buffer -j4 '\
		set -e; \
		VERSION={}; \
		PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
		BASE_NAME="us-docker.pkg.dev/$${PROJECT_ID}/$(REPO)/$${VERSION}"; \
		TAG=$$(gcloud artifacts tags list --package=$${VERSION} --location=us --repository=$(REPO) --project $${PROJECT_ID} 2>/dev/null | grep -Eo "^[0-9]+" | sort -n | tail -n1); \
		DOCKERHUB_TOKEN=$$(gcloud secrets versions access latest --secret=jakes-docker-access-token --project=burla-prod); \
		echo $${DOCKERHUB_TOKEN} | docker login -u jakezuliani --password-stdin; \
		docker tag $${BASE_NAME}:$${TAG} burlacloud/default-image-py$${VERSION}:$${TAG}; \
		docker push burlacloud/default-image-py$${VERSION}:$${TAG}; \
		docker tag $${BASE_NAME}:$${TAG} burlacloud/default-image-py$${VERSION}:latest; \
		docker push burlacloud/default-image-py$${VERSION}:latest'


images:
	set -e; \
	printf "$$(date "+%Y-%m-%d %H:%M:%S")" > last_image_pushed_at.txt; \
	VERSIONS="$(PYTHON_VERSIONS)"; \
	printf "%s\n" $$VERSIONS | parallel --line-buffer -j4 '\
		set -e; \
		VERSION={}; \
		PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
		BASE_NAME="us-docker.pkg.dev/$${PROJECT_ID}/$(REPO)/$${VERSION}"; \
		TAG=$$(gcloud artifacts tags list --package=$${VERSION} --location=us --repository=$(REPO) --project $${PROJECT_ID} 2>/dev/null | grep -Eo "^[0-9]+" | sort -n | tail -n1); \
		NAME="$${BASE_NAME}:$$(($${TAG} + 1))"; \
		docker build --platform linux/amd64 --build-arg PV=$${VERSION} -t "$${NAME}" -t "$${BASE_NAME}:latest" .; \
		docker push "$${NAME}"; \
		docker push "$${BASE_NAME}:latest"; \
		echo "Successfully built default image for $${VERSION}"'


images_same_env:
	set -e; \
	printf "$$(date "+%Y-%m-%d %H:%M:%S")" > last_image_pushed_at.txt; \
	VERSIONS="$(PYTHON_VERSIONS)"; \
	printf "%s\n" $$VERSIONS | parallel --line-buffer -j4 '\
		set -e; \
		VERSION={}; \
		PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
		BASE_NAME="us-docker.pkg.dev/$${PROJECT_ID}/$(REPO)/$${VERSION}"; \
		TAG=$$(gcloud artifacts tags list --package=$${VERSION} --location=us --repository=$(REPO) --project $${PROJECT_ID} 2>/dev/null | grep -Eo "^[0-9]+" | sort -n | tail -n1); \
		NAME="$${BASE_NAME}:$$(($${TAG} + 1))"; \
		docker build --platform linux/amd64 --build-arg BASE_IMAGE=$${BASE_NAME} -t "$${NAME}" -t "$${BASE_NAME}:latest" -f Dockerfile_same_env .; \
		docker push "$${NAME}"; \
		docker push "$${BASE_NAME}:latest"; \
		echo "Successfully built default image for $${VERSION}"'


# uses 3.10, I should develop in the oldest supported version.
container:
	set -e; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	IMAGE="us-docker.pkg.dev/$${PROJECT_ID}/$(REPO)/3.10:latest"; \
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
		$${IMAGE} bash
