FROM amazoncorretto:21-alpine

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy
ENV UV_PYTHON_CACHE_DIR=/root/.cache/uv/python
ENV PATH="/root/.local/bin:$PATH"
# hadolint ignore=DL3018
RUN apk add --no-cache gcc bash libc6-compat && \
    uv python install 3.12

COPY . /var/project/
WORKDIR /var/project/
RUN --mount=type=cache,target=/root/.cache/uv uv sync
