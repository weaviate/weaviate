FROM python:3.10-alpine

WORKDIR /chaos

RUN ["apk", "add", "curl", "bash"]

COPY run.sh .

COPY --from=docker:latest /usr/local/bin/docker /usr/local/bin/docker

CMD ["bash", "/chaos/run.sh"]
