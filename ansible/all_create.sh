#!/bin/bash
# Create all resources for this project. It's not a playbook so that the playbooks
# can run in parallel where possible
# Arguments:
#  - $1: Cloud project in clouds.yml
#  - $2: Key name used for connection to nodes
INVENTORY_FILENAME=staging
SITE_PB=site.yml

fatal() {
  echo $1 1>&2
  exit 1
}

test -z "$(which ansible-playbook)" && fatal "openstack command not available. Is the Conda environment activated?"
test -z "$2" && fatal "Usage: $0 <cloud-project-name> <ssh-key-name>"

export ANSIBLE_STDOUT_CALLBACK=ansible.posix.debug
export ANSIBLE_HOST_KEY_CHECKING=False
echo "Running ${SITE_PB} to create and setup all services"
ansible-playbook \
  --extra-vars openstack_cloud_name=$1 \
  --extra-vars openstack_key_name=$2 \
  --extra-vars inventory_filename=$PWD/${INVENTORY_FILENAME} \
  ${SITE_PB}
