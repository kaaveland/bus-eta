FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN useradd --create-home --home-dir /app --shell /usr/sbin/nologin webapp

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER webapp
WORKDIR /app
ADD leg_stats.parquet /app/
ADD stop_stats.parquet /app/
ADD stop_line.parquet /app/
ADD pyproject.toml /app/
ADD *.py .python-version /app/
RUN uv sync
EXPOSE 8000
ENTRYPOINT ["/app/.venv/bin/gunicorn"]
