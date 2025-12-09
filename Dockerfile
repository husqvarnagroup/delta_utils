# Base image can be found at https://github.com/bulv1ne/pyspark-docker-base
# hadolint ignore=DL3006
FROM ghcr.io/bulv1ne/pyspark-docker-base

# hadolint ignore=DL3018
RUN apk add --no-cache libc6-compat

COPY . /var/project/
WORKDIR /var/project/
