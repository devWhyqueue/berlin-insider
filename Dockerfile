FROM python:3.13-slim

# Install uv binary from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Install project dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Install Chromium and system dependencies for Playwright
RUN playwright install --with-deps chromium

# Copy application source and install project
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz')" || exit 1

ENTRYPOINT ["berlin-insider"]
CMD ["worker"]
