FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN apt-get update && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

ADD pyproject.toml uv.lock /app/
ADD kollektivkart /app/kollektivkart/

WORKDIR /app
RUN uv sync --no-cache --frozen --no-dev --group=scripts

EXPOSE 8000
ENTRYPOINT ["/app/.venv/bin/gunicorn"]
