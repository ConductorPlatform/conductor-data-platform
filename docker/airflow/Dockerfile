ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}

USER root

# ── System deps for dbt-core + git ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── FAB auth manager + dbt-core for project runtimes ──
RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab" \
    "dbt-core==2.0.0a4"

# ── Idempotent project service-account bootstrap ──
COPY --chown=airflow:root bootstrap_airflow_users.py /home/airflow/
