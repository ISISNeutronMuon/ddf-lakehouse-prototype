#!/bin/bash
# Destroy all resources created for this project
# Arguments:
#  - $1: Cloud project in clouds.yml
SERVER_NAMES="jupyter spark iceberg_catalog minio trino"
SECURITY_GROUPS=$SERVER_NAMES

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

echo "Removing all openstack resources (requires sudo password to edit local /etc/hosts)"

# Remove local /etc/hosts and ~/.ssh/known_hosts mods
ansible-playbook -i staging -i localhost --limit localhost \
  -e etchosts_state=absent -e openstack_cloud_name=$1 -K \
  playbooks/system/etchosts.yml
ansible-playbook -i staging -i localhost --limit localhost \
  -e ssh_knownhosts_state=absent -e openstack_cloud_name=$1 \
  playbooks/system/ssh_knownhosts.yml

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
security_groups=$servers
openstack --os-cloud=$1 security group delete $security_groups
