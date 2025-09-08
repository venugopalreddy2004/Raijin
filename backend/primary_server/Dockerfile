FROM node:22.19.0-alpine

RUN mkdir -p /home/app

WORKDIR /home/app

COPY package*.json ./

RUN npm install

COPY ./src ./src

COPY tsconfig.json ./

EXPOSE 8080

RUN npm run build

CMD ["node","dist/index.js"]