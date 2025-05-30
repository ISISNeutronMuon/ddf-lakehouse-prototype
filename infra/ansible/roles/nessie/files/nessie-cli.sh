#!/usr/bin/env bash
# Run the Nessie garbage collection tool via docker and the standalone JAR
#
set -euo pipefail

source .env

# General options
JAVA_IMAGE=openjdk:11-ea-jre
# NESSIE_GC_JDBC_ARGS="--jdbc --jdbc-url=${NESSIE_GC_JDBC_URL} --jdbc-user=${NESSIE_GC_JDBC_USER} --jdbc-password=${NESSIE_GC_JDBC_PASSWORD}"
# NESSIE_GC_ICEBERG_ARGS="--iceberg=s3.access-key-id=${NESSIE_GC_S3_ACCESS_KEY},s3.secret-access-key=${NESSIE_GC_S3_ACCESS_SECRET},s3.endpoint=${NESSIE_GC_S3_ENDPOINT},s3.path-style-access=${NESSIE_GC_S3_PATHSTYLE_ACCESS}"
NESSIE_CLI_VERSION="${NESSIE_CLI_VERSION:-0.103.0}"
NESSIE_CLI_JAR="nessie-gc-${NESSIE_CLI_VERSION}.jar"
NESSIE_CLI_DOWNLOAD_BASE_URL="https://github.com/projectnessie/nessie/releases/download/"
WORKING_DIR="${WORKING_DIR:-$HOME/.nessiecli}"

# # identify phase options
# DEFAULT_CUTOFF=${NESSIE_GC_CUTOFF_COMMITS:-1}
# CONTENT_SET_IDS_DIR="contentset_ids"

info() {
  echo $*
}

ensure_working_dirs_exists() {
  test -d ${WORKING_DIR} || mkdir -p ${WORKING_DIR}
}

download_nessie_cli_jar() {
  local destination=${WORKING_DIR}/${NESSIE_CLI_JAR}
  info "Downloading nessie-cli ${NESSIE_CLI_VERSION} to ${destination}"
  curl -L \
    -o ${destination} \
    "${NESSIE_CLI_DOWNLOAD_BASE_URL}/nessie-${NESSIE_CLI_VERSION}/nessie-cli-${NESSIE_CLI_VERSION}.jar"
}

ensure_nessiecli_jar_downloaded() {
  test -f ${WORKING_DIR}/${NESSIE_CLI_JAR} || download_nessie_cli_jar
}

preflight() {
  ensure_working_dirs_exists
  ensure_nessiecli_jar_downloaded
}

docker_run() {
  docker run \
      --rm \
      --network=host \
      -it \
      -v ${WORKING_DIR}:/work \
      ${JAVA_IMAGE} \
      java -jar /work/${NESSIE_CLI_JAR} $*
}

# identify() {
#   local live_content_set_id_file="${CONTENT_SET_IDS_DIR}/nessiegc_live_content_set_id_$(date '+%Y%m%dT%H%M%S')"
#   docker_run identify \
#       ${NESSIE_GC_JDBC_ARGS} \
#       --uri=${NESSIE_GC_URI} \
#       --default-cutoff=${DEFAULT_CUTOFF} \
#       --write-live-set-id-to="/work/${live_content_set_id_file}"
#   info "Live content set id written to '${live_content_set_id_file}'"
# }

# sweep () {
#   local live_content_set_id_file_host=$1
#   local live_content_set_id_file_container=${1/${WORKING_DIR}//work}
#   shift
#   info "Running sweep with additional options '$*' on live content set '$(cat ${live_content_set_id_file_host})'"
#   docker_run sweep \
#       ${NESSIE_GC_JDBC_ARGS} \
#       ${NESSIE_GC_ICEBERG_ARGS} \
#       -L=${live_content_set_id_file_container} \
#       $*
# }

#
preflight
docker_run $*
# Special case the commands that require a complicated combination of options
# to allow them to be specified as environment variables
# case "$1" in
#   identify)
#     identify
#     ;;
#   show)
#     shift
#     docker_run show ${NESSIE_GC_JDBC_ARGS} $*
#     ;;
#   sweep-no-delete)
#     # Second script argument is file containing content ID
#     sweep "$2" --defer-deletes
#     ;;
#   list-deferred)
#     shift
#     docker_run list-deferred ${NESSIE_GC_JDBC_ARGS} $*
#     ;;
#   deferred-deletes)
#     shift
#     docker_run deferred-deletes ${NESSIE_GC_JDBC_ARGS} ${NESSIE_GC_ICEBERG_ARGS} $*
#     ;;
#   *)
#     docker_run $*
#     ;;
# esac
