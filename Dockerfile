# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# Create non-root user early (before COPY)
RUN adduser --disabled-password --gecos "" --uid 1000 appuser

# Combine ALL apt-get operations into one layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements first (better layer caching)
# Note: pip.conf is mounted at runtime for local PyPI access
COPY --chown=appuser:appuser requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

# Copy application code with correct ownership
COPY --chown=appuser:appuser . .

USER appuser

ENV PYTHONUNBUFFERED=1 \
    DEFAULT_LAT="34.729847" \
    DEFAULT_LON="-86.5859011"

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
