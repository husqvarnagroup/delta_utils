# Base image can be found at https://github.com/bulv1ne/pyspark-docker-base
# hadolint ignore=DL3006
FROM ghcr.io/bulv1ne/pyspark-docker-base

COPY pyproject.toml /var/project/
WORKDIR /var/project/

RUN poetry config virtualenvs.create false && poetry install
