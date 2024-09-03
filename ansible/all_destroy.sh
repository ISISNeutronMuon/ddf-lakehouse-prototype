#!/bin/bash
# Destroy all resources created for this project
# Arguments:
#  - $1: Cloud project in clouds.yml

set -eo pipefail

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

echo "Removing all openstack resources."

# Servers
set +e
servers=$(openstack --os-cloud=da-proto server list -f json | jq --raw-output '.[].Name' | xargs)
echo "Stopping all servers"
openstack --os-cloud=$1 server stop $servers
active="$(openstack --os-cloud=$1 server list | grep ACTIVE)"
while [ -n "$active" ]; do
  sleep 1
  echo "Waiting for servers to be stopped"
  active="$(openstack --os-cloud=$1 server list | grep ACTIVE)"
done

echo "Deleting all servers"
openstack --os-cloud=$1 server delete $servers
instances="$(openstack --os-cloud=$1 server list)"
while [ -n "$instances" ]; do
  sleep 1
  echo "Waiting for servers to be deleted"
  instances="$(openstack --os-cloud=$1 server list)"
done

# Security groups
security_groups=${servers/jupyterhub/}
openstack --os-cloud=$1 security group delete $security_groups

# Post
echo "All openstack resources have been removed."
echo "Any modifications to ~/.ssh/known_hosts or /etc/hosts on localhost have not been removed and will need to be removed manually."
