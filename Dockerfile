# Works for databricks instances running 9.1
FROM openjdk:8-jdk-slim

# hadolint ignore=DL3008,DL3003,DL3013
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libsqlite3-dev \
        libreadline-dev \
        libffi-dev \
        libbz2-dev  \
        curl && \
    curl -O https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz && \
    tar xf Python-3.8.12.tgz && \
    cd Python-3.8.12 && \
    ./configure --enable-optimizations --enable-loadable-sqlite-extensions && \
    make && \
    make altinstall && \
    rm -rf /var/lib/apt/lists/* && \
    ln /usr/local/bin/python3.8 /usr/local/bin/python && \
    ln /usr/local/bin/python3.8 /usr/local/bin/python3 && \
    python -m pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir poetry

COPY pyproject.toml /var/project/
WORKDIR /var/project/

RUN poetry config virtualenvs.create false && poetry install

COPY .databricks-connect /root/.databricks-connect

CMD ["databricks-connect", "test"]
