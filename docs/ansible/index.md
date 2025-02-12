# Ansible

[Ansible](../../ansible/) is currently used to deploy and maintain the system.
A [directory of playbooks](../../ansible/playbooks/) per service describe the
tasks required to create and configure the service.

## Dependencies

While the playbooks for each service are independent there is interdependencies
between elements that can require playbooks to be run in a certain order.
The main example of this is around Traefik where a port may change on a service
that requires updating the Traefik configuration once the service has been redeployed.

### Nessie

The following services depend on the Nessie configuration and may need to be redeployed
if the Spark configuration changes:

- JupyterHub
- Trino

### Spark

The following services depend on the Spark configuration and may need to be redeployed
if the Spark configuration changes:

- JupyterHub
