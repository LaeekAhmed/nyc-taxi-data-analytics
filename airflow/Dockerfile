# First-time build can take upto 10 mins.

FROM apache/airflow:2.6.3

ENV AIRFLOW_HOME=/opt/airflow

# Compulsory to switch parameter
ENV PIP_USER=false

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

#python venv setup
RUN python3 -m venv /opt/airflow/venv1

COPY requirements.txt .
# RUN pip install --upgrade pip
RUN /opt/airflow/venv1/bin/pip install --no-cache-dir -r requirements.txt
RUN /opt/airflow/venv1/bin/pip install --no-cache-dir pandas sqlalchemy psycopg2-binary
ENV PIP_USER=true

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# update accordingly :
ARG CLOUD_SDK_VERSION=444.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

# also works without this :
COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID