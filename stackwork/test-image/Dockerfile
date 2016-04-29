# use node:5.10.0 as base image because this has curl and we know it is already downloaded in docker-server
FROM node:5.10.0
RUN npm install -g mocha
RUN npm install --save-dev chakram
COPY test /test
CMD ["mocha"]
