FROM python:latest
WORKDIR /app/
COPY . .
RUN conda import environment.yaml
