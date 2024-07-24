#!/bin/bash
# Destroy all resources created for this project
# Arguments:
#  - $1: Cloud project in clouds.yml
SERVER_NAMES="jupyter spark iceberg-catalog minio trino"
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

# Remove local /etc/hosts mods
ansible-playbook -i staging --limit localhost -e etchosts_state=absent -K \
  playbooks/inventory/etchosts.yml

# Servers
echo "Stopping all servers"
openstack --os-cloud=$1 server stop $SERVER_NAMES
active="$(openstack --os-cloud=$1 server list | grep ACTIVE)"
while [ -n "$active" ]; do
  sleep 1
  echo "Waiting for servers to be stopped"
  active="$(openstack --os-cloud=$1 server list | grep ACTIVE)"
done

echo "Deleting all servers"
openstack --os-cloud=$1 server delete $SERVER_NAMES
instances="$(openstack --os-cloud=$1 server list)"
while [ -n "$instances" ]; do
  sleep 1
  echo "Waiting for servers to be deleted"
  instances="$(openstack --os-cloud=$1 server list)"
done

# Security groups
openstack --os-cloud=$1 security group delete $SECURITY_GROUPS
