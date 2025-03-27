# Ansible

[Ansible](../../ansible/) is currently used to deploy and maintain the system.
A [directory of playbooks](../../ansible/playbooks/) per service describe the
tasks required to create and configure the service.

## Keycloak

Keycloak provides a central place for identity & access management. It uses `LDAP`
for authentication and defines realms & user groups for controlling access to
catalog resources.

Configuration is currently manually specified through the management console:
https://data-accelerator.isis.cclrc.ac.uk/auth/

### First-time setup

A clean Keycloak instance contains a temporary admin user along with a `master`
realm. On creation of a new instance:

* Create a new user:
  * assign them an email
  * assign them all roles (this makes them a "super admin")
  * once logged in as this user delete the temporary admin user account

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
