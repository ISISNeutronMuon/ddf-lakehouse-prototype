# Deployment

_The system is currently deployed on the [SCD cloud](https://cloud.stfc.ac.uk/)._
_In theory it can be deployed on any cloud infrastructure but it will require_
_changes before this will work._

There are several prerequisites required before deployment can being. Please
read [them here](./prerequisites.md).

## Ansible

Ansible playbooks in `<repo_root>/ansible/playbooks` control the deployment
of the prototype.

To create all VM resources run the amalgamated playbook:

```sh
ansible-playbook -i localhost -e inventory_filename=$PWD/dataplatform.ini \
  -e openstack_cloud_name=<cloud_name> \
  -e openstack_key_name=<ssh_key> \ playbooks/cloud/vms_create.yml
```

where _cloud\_name_ and _ssh\_key_ are described in the [prerequisites section](./prerequisites.md#openstack-api--vm-credentials).

```sh
ansible-playbook -i dataplatform.ini playbooks/cloud/services_create.yml
```

`ANSIBLE_HOST_KEY_CHECKING=False`
