FROM python:3.10-slim

WORKDIR /app

RUN apt-get update
RUN pip install --upgrade pip setuptools

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["uvicorn app:app --log-level critical --host 0.0.0.0 --port 8080"]
##CMD ["uvicorn app:app  --host 0.0.0.0 --port 8080"]
