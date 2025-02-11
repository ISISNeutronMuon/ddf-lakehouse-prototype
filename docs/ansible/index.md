# Ansible

[Ansible](../../ansible/) is currently used to deploy and maintain the system.
A [directory of playbooks](../../ansible/playbooks/) per service describe the
tasks required to create and configure the service.

## Gotchas

While the playbooks for each service are independent there is interdependencies
between elements that can require playbooks to be run in a certain order.
The main example of this is around Traefik where a port may change on a service
that requires updating the Traefik configuration once the service has been redeployed.

### Spark

If the value of the `spark_controller_ui_https_port` is changed then Spark & JupyterHub
must be redeployed so that the controller spark configuration know the new location
of the UI proxy.
