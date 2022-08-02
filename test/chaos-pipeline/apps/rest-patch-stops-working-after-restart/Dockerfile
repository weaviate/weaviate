FROM python:3.9-slim-buster

WORKDIR /workdir

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY rest-patch-stops-working-after-restart.py ./
