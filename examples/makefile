.ONESHELL:
.SILENT:

PROJECT_ID := $(shell gcloud config get-value project 2>/dev/null)
IMAGE_NAME := us-docker.pkg.dev/$(PROJECT_ID)/vector-embedding-demo/vector-embedding-demo:latest

vector_image:
	docker build --platform linux/amd64 -t $(IMAGE_NAME) ./vector_embeddings
	docker push $(IMAGE_NAME)
	echo "Successfully uploaded Docker Image: $(IMAGE_NAME)"

vectordb:
	poetry run python vector_embeddings/create_vector_db.py

demo:
	poetry run python vector_embeddings/demo.py

wiki:
	poetry run python vector_embeddings/format_wikipedia.py

hp_tuning:
	poetry run python hp_tuning.py

shell:
	poetry shell