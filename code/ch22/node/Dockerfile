# The NATS client is pure JavaScript: the alpine image is enough
FROM node:24-alpine
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
# Node 24 runs TypeScript directly (it strips the type annotations on load).
# SERVICE chooses the program; the arguments of `docker compose run` go to it.
ENTRYPOINT ["sh", "-c", "exec node --disable-warning=ExperimentalWarning src/${SERVICE}.ts \"$@\"", "--"]
