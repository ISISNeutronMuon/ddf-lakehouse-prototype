#!/bin/bash
# Destroy all resources created for this project
# Arguments:
#  - $1: Cloud project in clouds.yml
SERVER_NAMES="jupyter spark iceberg-catalog minio"
SECURITY_GROUPS=$SERVER_NAMES

fatal() {
  echo $1 1>&2
  exit 1
}

test -z "$(which openstack)" && fatal "openstack command not available. Is the Conda environment activated?"
test -z "$1" && fatal "Usage: $0 <cloud-project-name>"

read -r -p "Are you sure? [y/N] " response
case "$response" in
    [yY][eE][sS]|[yY])
        ;;
    *)
      exit 0
        ;;
esac

# Servers
openstack --os-cloud=$1 server stop $SERVER_NAMES
openstack --os-cloud=$1 server delete $SERVER_NAMES

# Security groups
openstack --os-cloud=$1 security group delete $SECURITY_GROUPS
