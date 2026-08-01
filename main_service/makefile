.ONESHELL:
.SILENT:

BURLA_VERSION := $(shell python3 -c 'import tomllib; print(tomllib.load(open("../client/pyproject.toml", "rb"))["project"]["version"])')
WEBSERVICE_NAME = burla-main-service
PYTHON_MODULE_NAME = main_service

ARTIFACT_REPO_NAME := $(WEBSERVICE_NAME)
ARTIFACT_PKG_NAME := $(WEBSERVICE_NAME)

.PHONY: build-frontend ensure-frontend dev-image promote-tested-image
build-frontend:
	set -e; \
	cd ./frontend; \
	npm ci; \
	npm run build; \
	rsync -a --delete --exclude='.*' --exclude='login.html.j2' dist/ ../src/main_service/static/; \
	cd ..; \
	printf "$$(date +%s)" > .frontend_last_built_at.txt

ensure-frontend:
	set -e; \
	if ! python3 -c 'import re, sys; from pathlib import Path; static = Path("src/main_service/static"); html = (static / "index.html").read_text(); assets = re.findall(r"(?:src|href)=\"(/assets/[^\"]+)", html); sys.exit(not assets or any(not (static / path.lstrip("/")).exists() for path in assets))'; then \
		$(MAKE) build-frontend; \
	fi

promote-tested-image:
	set -e; \
	if [ -z "$${TEST_IMAGE_NAME}" ]; then \
		echo "TEST_IMAGE_NAME must name the verified image"; \
		exit 1; \
	fi; \
	case "$${TEST_IMAGE_NAME}" in \
		us-docker.pkg.dev/burla-test/*|us-docker.pkg.dev/burla-agent-[0-9][0-9]/*) ;; \
		*) echo "Refusing to promote non-test image $${TEST_IMAGE_NAME}"; exit 1 ;; \
	esac; \
	PROD_IMAGE_BASE_NAME=$$( echo \
		"us-docker.pkg.dev/burla-prod/$(ARTIFACT_REPO_NAME)/$(ARTIFACT_PKG_NAME)" \
	); \
	docker pull $${TEST_IMAGE_NAME}; \
	docker tag $${TEST_IMAGE_NAME} $${PROD_IMAGE_BASE_NAME}:$(BURLA_VERSION); \
	docker push $${PROD_IMAGE_BASE_NAME}:$(BURLA_VERSION)

dev-image: build-frontend
	set -e; \
	if ! git -C .. diff --quiet || [ -n "$$(git -C .. status --porcelain --untracked-files=normal)" ]; then \
		echo "Refusing image build from a dirty worktree"; \
		exit 1; \
	fi; \
	PROJECT_ID=$$(gcloud config get-value project 2>/dev/null); \
	case "$${PROJECT_ID}" in \
		burla-test|burla-agent-[0-9][0-9]) ;; \
		*) echo "Refusing dev image build in $${PROJECT_ID}"; exit 1 ;; \
	esac; \
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
	REVISION=$$(git -C .. rev-parse HEAD); \
	docker build --platform linux/amd64 \
		--label org.opencontainers.image.revision=$${REVISION} \
		-t $${TEST_IMAGE_NAME_SEQUENTIAL} .; \
	docker tag $${TEST_IMAGE_NAME_SEQUENTIAL} $${TEST_IMAGE_NAME_LATEST}; \
	docker push $${TEST_IMAGE_NAME_SEQUENTIAL}; \
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
		$${TEST_IMAGE_NAME}
