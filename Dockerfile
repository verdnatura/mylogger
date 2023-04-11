FROM debian:bullseye-slim

ARG DEBIAN_FRONTEND=noninteractive

# NodeJs

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gnupg2 \
        git \
    && curl -fsSL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && npm i -g npm@9.6.4

# MyLogger

WORKDIR /mylogger
COPY package.json package-lock.json ./
RUN npm install --omit=dev \
    && git clone --depth 1 --branch fix-143 https://github.com/juan-ferrer-toribio/zongji.git \
    && (cd zongji && npm install --omit=dev)

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

CMD ["node", "index.js"]
