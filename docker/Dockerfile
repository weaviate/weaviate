# Run on Ubuntu
FROM ubuntu:16.04

# Set the Workdir to weaviate
RUN mkdir -p /var/weaviate && cd /var/weaviate
WORKDIR /var/weaviate

# Install needed packages and scripts
RUN apt-get update -qq && apt-get install -qq -y curl zip wget && \
    curl https://get.dgraph.io -sSf | bash && \
    wget -q -O /var/weaviate/weaviate.zip https://raw.githubusercontent.com/weaviate/weaviate/develop/dist/weaviate_nightly_linux_amd64.zip && \
    unzip -o -q -j weaviate.zip && \
    rm -rf weaviate_nightly_linux_amd64.zip && \
    wget -q -O /var/weaviate/weaviate.conf.json https://raw.githubusercontent.com/weaviate/weaviate/develop/weaviate.conf.json && \
    wget -q -O /var/weaviate/run-services.sh https://raw.githubusercontent.com/weaviate/weaviate/develop/docker/run-services.sh && \
    chmod +x /var/weaviate/run-services.sh 


# Expose dgraph ports
EXPOSE 8080

# Run!
CMD /var/weaviate/run-services.sh