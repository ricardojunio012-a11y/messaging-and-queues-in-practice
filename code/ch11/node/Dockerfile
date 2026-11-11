# Debian (slim) image: the Kafka client uses the native librdkafka library
FROM node:24-slim
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
COPY test ./test
# Node 24 runs TypeScript directly (it strips the type annotations on load).
CMD ["sh", "-c", "exec node --disable-warning=ExperimentalWarning src/${SERVICE}.ts"]
