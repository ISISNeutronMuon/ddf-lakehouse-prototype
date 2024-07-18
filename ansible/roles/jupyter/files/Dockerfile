# syntax=docker/dockerfile
FROM python:3.12-bookworm

ENV IPYTHON_STARTUP_DIR=/root/.ipython/profile_default/startup
ENV LOCAL_BIN=/usr/local/bin
ENV NOTEBOOKS_DIR=/var/lib/jupyter/notebooks

# Basic configuration
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  openjdk-17-jdk \
  build-essential && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Dirs
RUN mkdir -p \
  ${NOTEBOOKS_DIR} \
  ${IPYTHON_STARTUP_DIR}

# Python: Jupyter/Spark
RUN --mount=type=bind,source=./,target=/build pip install -r /build/requirements.txt
COPY ./notebook ${LOCAL_BIN}/
COPY ./ipython/startup/ ${IPYTHON_STARTUP_DIR}/

# Entrypoint: start notebook
COPY ./entrypoint.sh ${LOCAL_BIN}/

# File modes
RUN chmod u+x ${LOCAL_BIN}/*

CMD ["notebook"]
