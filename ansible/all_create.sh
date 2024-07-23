#!/bin/bash
# Create all resources for this project. It's not a playbook so that the playbooks
# can run in parallel where possible
# Arguments:
#  - $1: Cloud project in clouds.yml
#  - $2: Key name used for connection to nodes
INVENTORY_FILENAME=staging

fatal() {
  echo $1 1>&2
  exit 1
}

test -z "$(which ansible-playbook)" && fatal "openstack command not available. Is the Conda environment activated?"
test -z "$2" && fatal "Usage: $0 <cloud-project-name> <ssh-key-name>"

export ANSIBLE_STDOUT_CALLBACK=ansible.posix.debug
export ANSIBLE_HOST_KEY_CHECKING=False
ansible-playbook \
  -e openstack_cloud_name=$1 \
  -e openstack_key_name=$2 \
  -e inventory_filename=$PWD/${INVENTORY_FILENAME} \
  site.yml
