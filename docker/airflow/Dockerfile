ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}

USER root

# ── System deps for dbt-core + git ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ── FAB auth manager + accepted dbt-postgres runtime adapter ──
RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab" \
    "apache-airflow-providers-git==0.4.0" \
    "dbt-core==1.11.4" \
    "dbt-postgres==1.11.0"

# ── Idempotent project service-account bootstrap ──
COPY --chown=airflow:root bootstrap_airflow_users.py /home/airflow/
COPY --chown=airflow:root conductor_git_bundle.py /opt/airflow/plugins/
ENV PYTHONPATH="/opt/airflow/plugins"
