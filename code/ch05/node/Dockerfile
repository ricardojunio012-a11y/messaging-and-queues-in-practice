FROM node:24-alpine
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY src ./src
# Node 24 runs TypeScript directly (it strips the type annotations on load).
CMD ["sh", "-c", "exec node --disable-warning=ExperimentalWarning src/${SERVICE}.ts"]
