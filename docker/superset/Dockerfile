# Superset with the PostgreSQL driver (the official image ships without database drivers) and the
# Smart Logistics config and provisioning scripts.
FROM apache/superset:6.1.0

USER root

RUN . /app/.venv/bin/activate && uv pip install --no-cache psycopg2-binary==2.9.10

ENV SUPERSET_CONFIG_PATH=/app/smart_logistics/superset_config.py

COPY superset/superset_config.py superset/provision.py superset/validate.py /app/smart_logistics/
COPY docker/superset/init.sh /app/smart_logistics/init.sh
RUN chmod 755 /app/smart_logistics/init.sh

USER superset
