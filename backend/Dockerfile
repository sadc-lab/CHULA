
FROM continuumio/miniconda3

WORKDIR /app


COPY environment.yaml /app/environment.yaml


RUN conda env create -f environment.yaml


SHELL ["conda", "run", "-n", "chula", "/bin/bash", "-c"]


COPY . /app

EXPOSE 8000


CMD ["conda", "run", "-n", "chula", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

