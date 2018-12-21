FROM node:11.4-alpine

COPY package.json package-lock.json ./
RUN npm i

COPY . .
CMD ["node", "."]
