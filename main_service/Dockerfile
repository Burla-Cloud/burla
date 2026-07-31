FROM python:3.13

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

ADD . /burla/main_service
WORKDIR /burla/main_service
ENV UV_PROJECT_ENVIRONMENT="/opt/burla-main-service-venv"
RUN uv sync --frozen --no-dev
ENV PATH="/opt/burla-main-service-venv/bin:${PATH}"

# PORT is 80 on head VMs (containers run with host networking there).
CMD ["sh", "-c", "python -m uvicorn main_service:app --host ${BIND_HOST:-0.0.0.0} --port ${PORT:-8080} --workers 1 --timeout-keep-alive 60"]
