FROM python:3.9-slim-buster

WORKDIR /workdir

COPY requirements.txt .
RUN pip3 install -r requirements.txt -f https://download.pytorch.org/whl/torch_stable.html

COPY generate.py import.py ./
