FROM python:3.13

# install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

ADD . /burla/main_service
WORKDIR /burla/main_service
RUN uv pip install --system -e .

CMD ["python", "-m", "uvicorn", "main_service:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--timeout-keep-alive", "60"]
