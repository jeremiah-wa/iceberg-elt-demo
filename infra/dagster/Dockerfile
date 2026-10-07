# dagster dev for the uv workspace. The image holds the locked environment; docker-compose
# bind-mounts the project sources over /app/{extract_load,transform,orchestrate}.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/home

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY extract_load extract_load
COPY transform transform
COPY orchestrate orchestrate
RUN uv sync --frozen

RUN mkdir -p "$DAGSTER_HOME"
COPY infra/dagster/workspace.yaml /opt/dagster/workspace.yaml

# Not /app/orchestrate: dagster CLI commands load .env from their cwd, and orchestrate/.env may
# hold host-only values such as a Windows DAGSTER_HOME
WORKDIR /opt/dagster
EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/workspace.yaml"]
