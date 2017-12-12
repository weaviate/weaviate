# Run on Ubuntu
FROM ubuntu:14.04

# Set the Workdir to weaviate
RUN mkdir -p /var/weaviate/config && cd /var/weaviate
WORKDIR /var/weaviate

# Install needed packages and scripts
RUN echo "BUILDING weaviate_nightly_linux_amd64.zip"

RUN apt-get -qq update && apt-get -qq install -y jq curl zip wget && \
    wget -q -O /var/weaviate/weaviate.zip https://raw.githubusercontent.com/weaviate/weaviate/develop/dist/weaviate_nightly_linux_amd64.zip && \
    unzip -o -q -j weaviate.zip && \
    rm /var/weaviate/weaviate.zip && \
    rm -rf weaviate_nightly_linux_amd64.zip && \
    chmod +x /var/weaviate/weaviate
    
# Expose dgraph ports
EXPOSE 80

# Run!
CMD /var/weaviate/weaviate --scheme=http --port=80 --host=0.0.0.0 --config=docker --config-file=/var/weaviate/config/weaviate.conf.json  > /var/weaviate/first_run.log 2>&1
