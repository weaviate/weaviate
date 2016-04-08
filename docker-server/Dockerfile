FROM node:5.10.0

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json /usr/src/app/
RUN npm install
COPY libs /usr/src/app/libs
COPY .eslintrc /usr/src/app/
COPY gulpfile.js /usr/src/app/
COPY weaviate.js /usr/src/app/
COPY docker-server/server.js /usr/src/app/

CMD [ "npm", "start" ]
