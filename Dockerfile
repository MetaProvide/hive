FROM node:24-slim AS base
RUN apt-get update && \
    apt-get install -y --no-install-recommends libatomic1 && \
    rm -rf /var/lib/apt/lists/*


FROM base AS pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate


FROM pnpm AS deps
USER node
WORKDIR /app

COPY --chown=node:node package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile --shamefully-hoist


FROM deps AS development

WORKDIR /app/node_app
VOLUME [ "/app/node_app" ]

EXPOSE 4774
CMD ["pnpm", "run", "start:dev"]


FROM deps AS build

COPY --chown=node:node . .

RUN pnpm run build
RUN pnpm prune --prod


FROM base AS production
USER node
WORKDIR /app

RUN mkdir -p /app/storage

COPY --chown=node:node --from=build /app/node_modules ./node_modules
COPY --chown=node:node --from=build /app/dist ./

EXPOSE 4774
CMD ["node", "main.js"]
