FROM node:5.10.0

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY build/src/package.json /usr/src/app/
RUN npm install
COPY build/src/controllers /usr/src/app/controllers
COPY build/src/libs /usr/src/app/libs
COPY build/src/weaviate.js /usr/src/app/
COPY server.js /usr/src/app/

EXPOSE 80
CMD [ "npm", "start" ]
