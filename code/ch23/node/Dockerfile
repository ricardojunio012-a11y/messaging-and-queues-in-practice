# Debian (slim) image: the Kafka client uses the native librdkafka library
FROM node:24-slim
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
# Node 24 runs TypeScript directly. SERVICE chooses the program; the arguments of
# `docker compose run` go to it.
ENTRYPOINT ["sh", "-c", "exec node --disable-warning=ExperimentalWarning src/${SERVICE}.ts \"$@\"", "--"]
