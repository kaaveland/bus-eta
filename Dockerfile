FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN useradd --create-home --home-dir /app --shell /usr/sbin/nologin webapp

RUN apt-get update && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

ADD pyproject.toml /app/
ADD kollektivkart /app/kollektivkart/

WORKDIR /app
RUN uv sync --no-cache --extra=scripts

USER webapp
EXPOSE 8000
ENTRYPOINT ["/app/.venv/bin/gunicorn"]
