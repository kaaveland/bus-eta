FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN useradd --create-home --home-dir /app --shell /usr/sbin/nologin webapp

RUN apt-get update && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

ADD leg_stats.parquet stop_stats.parquet stop_line.parquet pyproject.toml *.py .python-version /app/
WORKDIR /app
RUN uv sync --no-cache

USER webapp
EXPOSE 8000
ENTRYPOINT ["/app/.venv/bin/gunicorn"]
