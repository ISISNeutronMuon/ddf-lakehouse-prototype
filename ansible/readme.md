# Ansible Scripts

## Setup Ansible

An optional [Conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)
environment file, `condaenv.yml`, is provided to setup a working Ansible version within Conda.
For other Python installations a `ansible-install-requirements.txt` file is provided.

Once Ansible is installed, install the additional Ansible Galaxy roles with:

```sh
ansible-galaxy role install --roles-path galaxy_roles
```

The `galaxy_roles` path has been added to `.gitignore`.

## Run the playbooks

Create an inventory file, `nodes.txt`, containing the hostname and any additional
variables required, e.g.

```ini
[nodes]
hostname-or-ip-address ansible_user=ubuntu
```

and run the playbook:

```sh
ansible-playbook -i nodes.txt playbooks/prep_node.yml
```
