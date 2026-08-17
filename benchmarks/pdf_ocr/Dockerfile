FROM jbarlow83/ocrmypdf@sha256:f3dbfb9827ecfc0ab9e26f6093605cb60a3cd0d6df7d3651a2f563e60797cd90

RUN apt-get update \
    && apt-get install -y --no-install-recommends poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Burla supplies the worker-server command; the upstream CLI entrypoint would
# otherwise receive that entire command as OCRmyPDF arguments.
USER root
ENTRYPOINT []
