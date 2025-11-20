# Multi-stage build
FROM python:3.14-slim AS builder

# Install Poetry
RUN pip install --no-cache-dir poetry==2.2.1

# Configure Poetry
RUN poetry config virtualenvs.create false

WORKDIR /app

# Copy project metadata
COPY pyproject.toml poetry.lock ./

# Install runtime deps including pyarrow (wheel only, no source build)
RUN poetry install --no-root --only main --no-interaction --no-ansi

# ---------------------------------------

FROM python:3.14-slim

# Create user
RUN useradd -m -u 1000 flashbots && mkdir -p /app && chown -R flashbots:flashbots /app

WORKDIR /app

# Copy Python deps
COPY --from=builder /usr/local/lib/python3.14/site-packages /usr/local/lib/python3.14/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy app
COPY --chown=flashbots:flashbots src/ ./src/
COPY --chown=flashbots:flashbots pyproject.toml poetry.lock ./

USER flashbots

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

CMD ["python", "src/live.py"]
