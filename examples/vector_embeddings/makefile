.ONESHELL:
.SILENT:

IMAGE_NAME := us-docker.pkg.dev/imaginary-space-burla/vector-embedding-demo/vector-embedding-demo:latest

image:
	docker build --platform linux/amd64 -t $(IMAGE_NAME) .
	docker push $(IMAGE_NAME)
	echo "Successfully uploaded Docker Image: $(IMAGE_NAME)"

vectordb:
	poetry run python create_vector_db.py

demo:
	poetry run python demo.py

wiki:
	poetry run python format_wikipedia.py