FROM node:18-slim

WORKDIR /usr/src/app

COPY package*.json ./
COPY scripts/ ./scripts/

RUN npm install --omit-dev

COPY . .

CMD ["node","worker.js"]