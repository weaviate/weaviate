# Run on Ubuntu
FROM ubuntu:16.04

# Get arg
ARG DGRAPHIP

# Set the Workdir to weaviate
RUN mkdir -p /var/weaviate && cd /var/weaviate
WORKDIR /var/weaviate

# Install needed packages and scripts
RUN apt-get update -qq && apt-get install -qq -y jq curl zip wget && \
    wget -q -O /var/weaviate/weaviate.zip https://raw.githubusercontent.com/weaviate/weaviate/develop/dist/weaviate_nightly_linux_amd64.zip && \
    unzip -o -q -j weaviate.zip && \
    rm -rf weaviate_nightly_linux_amd64.zip && \
    wget -q -O /var/weaviate/weaviate.conf.json https://raw.githubusercontent.com/weaviate/weaviate/develop/weaviate.conf.json && \
    chmod +x /var/weaviate/weaviate && \
    echo "UPDATE WITH" ${DGRAPHIP} && \
    sed -i "s/x.x.x.x/${DGRAPHIP}/g" /var/weaviate/weaviate.conf.json

# Expose dgraph ports
EXPOSE 8080

# Run!
CMD /var/weaviate/weaviate --scheme=http --port=8080 --host=localhost --config=docker --config-file=/var/weaviate/weaviate.conf.json
