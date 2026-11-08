FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY logistics ./logistics
COPY etl ./etl
COPY sql ./sql
COPY scripts ./scripts
COPY tests ./tests
COPY pytest.ini .
COPY docker/pipeline-entrypoint.sh /app/docker/pipeline-entrypoint.sh

RUN useradd --create-home --uid 1000 app \
    && chmod 755 /app/docker/pipeline-entrypoint.sh \
    && chown -R app:app /app

USER app

ENTRYPOINT ["bash", "/app/docker/pipeline-entrypoint.sh"]
CMD ["python", "scripts/check_databases.py"]
