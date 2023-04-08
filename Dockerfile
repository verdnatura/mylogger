FROM debian:bullseye-slim

ARG DEBIAN_FRONTEND=noninteractive

# NodeJs

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gnupg2 \
    && url -fsSL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Consumer

WORKDIR /mycdc
COPY package.json package-lock.json ./
RUN npm install --only=prod

ARG BUILD_ID=unknown
ARG VERSION
ENV VERSION $VERSION
RUN echo $VERSION

COPY \
    LICENSE \
    README.md \
    mylogger.js \
    index.js \
    config.yml \
    ./

CMD ["node", "mylogger.js"]
