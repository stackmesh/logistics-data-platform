# Airflow with the platform in its own virtualenv: Airflow keeps its constraint-pinned packages, the DAG's
# tasks run the platform with the versions in constraints.txt (PIPELINE_PYTHON).
FROM apache/airflow:3.4.0-python3.12

USER root

ENV PIPELINE_HOME=/opt/pipeline/app \
    PIPELINE_PYTHON=/opt/pipeline/venv/bin/python \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt constraints.txt /opt/pipeline/
RUN python -m venv /opt/pipeline/venv \
    && /opt/pipeline/venv/bin/pip install -r /opt/pipeline/requirements.txt -c /opt/pipeline/constraints.txt

COPY logistics /opt/pipeline/app/logistics
COPY etl /opt/pipeline/app/etl
COPY ml /opt/pipeline/app/ml
COPY orchestration /opt/pipeline/app/orchestration
COPY sql /opt/pipeline/app/sql
COPY scripts /opt/pipeline/app/scripts
COPY dags /opt/airflow/dags
COPY docker/airflow/entrypoint.sh /opt/pipeline/entrypoint.sh

# uid 1000 matches the pipeline image, so both write the shared artifact volumes; gid 0 is Airflow's convention.
RUN mkdir -p /opt/pipeline/app/artifacts/features /opt/pipeline/app/artifacts/reports /opt/pipeline/app/ml/artifacts \
    && chown -R 1000:0 /opt/pipeline/app/artifacts /opt/pipeline/app/ml/artifacts \
    && chmod -R g+rwX /opt/pipeline/app/artifacts /opt/pipeline/app/ml/artifacts \
    && chmod 755 /opt/pipeline/entrypoint.sh

USER airflow

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/pipeline/entrypoint.sh"]
CMD ["--help"]
