FROM python:3.13-slim
RUN pip install --no-cache-dir fastapi uvicorn requests
COPY auth_plugin.py /app/auth_plugin.py
WORKDIR /app
CMD ["uvicorn", "auth_plugin:app", "--host", "0.0.0.0", "--port", "9000"]
