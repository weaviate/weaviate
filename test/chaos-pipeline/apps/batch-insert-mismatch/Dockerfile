FROM python:3.9-slim-buster

WORKDIR /workdir

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY wikipedia1k.json.zip ./
COPY batch-insert-mismatch.py ./
