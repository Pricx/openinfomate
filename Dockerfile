FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TRACKER_DB_URL=sqlite:////data/tracker.db \
    TRACKER_ENV_PATH=/data/.env \
    TRACKER_API_HOST=0.0.0.0 \
    TRACKER_API_PORT=8080

COPY pyproject.toml README.md LICENSE /app/
COPY src /app/src
COPY docker /app/docker

RUN python -m pip install -U pip && \
    python -m pip install . && \
    chmod +x /app/docker/entrypoint.sh

VOLUME ["/data"]
EXPOSE 8080

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["tracker", "api", "serve"]
