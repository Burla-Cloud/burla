FROM python:3.13

ADD . /burla/main_service
WORKDIR /burla/main_service
RUN python3.13 -m pip install -e .

ENTRYPOINT python3.13 -m uvicorn main_service:app --host 0.0.0.0 --port 8080
