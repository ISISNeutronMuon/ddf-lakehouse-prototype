# syntax=docker/dockerfile
######################################################################
# Maven stage to fetch the required jar extensions for Spark
# along with their dependencies.
# The main Spark distribution is currently handled directly by
# downloading it has part of the final image stage.
######################################################################
FROM maven:3.9.9-eclipse-temurin-17 AS spark-jars

RUN mkdir /source
COPY pom.xml /source/

WORKDIR /source
RUN mvn dependency:copy-dependencies

######################################################################
# Final image
######################################################################
FROM python:3.10-bookworm

# Arguments
ARG SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/
ARG SPARK_VERSION=3.5.2
ARG SPARK_MAJOR_VERSION=3.5
ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG SCALA_VERSION=2.12
ARG HADOOP_VERSION=3.4.0
ARG ICEBERG_VERSION=1.5.2
ARG POSTGRES_CONNECTOR_VERSION=42.7.4
ARG AWS_SDK_VERSION=2.27.21

# Environment
ENV SPARK_HOME=/opt/spark
ENV SCALA_VERSION=${SCALA_VERSION}
ENV LOCAL_BIN=/usr/local/bin

# Basic configuration
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:${PATH}"
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  sudo \
  curl \
  unzip \
  openjdk-17-jdk \
  build-essential \
  software-properties-common && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Dirs
RUN mkdir -p ${SPARK_HOME} \
  ${SPARK_HOME}/spark-events
WORKDIR ${SPARK_HOME}

# Spark
RUN curl ${SPARK_DOWNLOAD_URL}/spark-3.5.2/spark-3.5.2-bin-without-hadoop.tgz -o spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
  && tar xvzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz --directory ${SPARK_HOME} --strip-components 1 \
  && rm -rf spark-${SPARK_VERSION}-bin-without-hadoop.tgz

COPY --chown=root:root --from=spark-jars /source/target/dependency/ ${SPARK_HOME}/jars/

# Entrypoint: start spark connect
COPY ./entrypoint.sh ${LOCAL_BIN}/

# File modes
RUN chmod u+x ${SPARK_HOME}/sbin/* && \
  chmod u+x ${SPARK_HOME}/bin/* && \
  chmod u+x ${LOCAL_BIN}/*

ENTRYPOINT ["entrypoint.sh"]
