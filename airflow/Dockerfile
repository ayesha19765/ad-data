# First-time build can take up to 10 mins.

FROM apache/airflow:2.8.1-python3.9

ENV AIRFLOW_HOME=/opt/airflow

USER root

# # --- Install system dependencies
# RUN apt-get update && apt-get install -y \
#     git gcc g++ build-essential libpq-dev curl ca-certificates \
#     && rm -rf /var/lib/apt/lists/*

# # --- Copy requirements first (so Docker caching works properly)
# COPY requirements.txt .

# # --- Install Python dependencies as airflow user
# USER airflow
# RUN pip install --no-cache-dir -r requirements.txt
# USER root

# # --- Add Google Cloud SDK (for dbt/airflow integrations)
# SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# ARG CLOUD_SDK_VERSION=322.0.0
# ENV GCLOUD_HOME=/home/google-cloud-sdk
# ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

# RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
#     && TMP_DIR="$(mktemp -d)" \
#     && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
#     && mkdir -p "${GCLOUD_HOME}" \
#     && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
#     && "${GCLOUD_HOME}/install.sh" \
#     --bash-completion=false \
#     --path-update=false \
#     --usage-reporting=false \
#     --quiet \
#     && rm -rf "${TMP_DIR}" \
#     && gcloud --version

# WORKDIR $AIRFLOW_HOME
# USER $AIRFLOW_UID

# --- Install system dependencies
RUN apt-get update && apt-get install -y \
    git gcc g++ build-essential libpq-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --- Copy requirements first (so Docker caching works properly)
COPY requirements.txt .

# --- Install Python dependencies as airflow user
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# --- Default workdir
WORKDIR /opt/airflow
USER $AIRFLOW_UID
