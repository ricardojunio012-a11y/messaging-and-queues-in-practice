# The decision matrix has no dependencies: only Node
FROM node:24-alpine
WORKDIR /app
COPY src ./src
# Node 24 runs TypeScript directly. The arguments of `docker compose run` go to the
# program.
ENTRYPOINT ["node", "--disable-warning=ExperimentalWarning", "src/matrix.ts"]
