# Kubernetes Setup

This directory holds k8s configuration for creation of the various clusters
deployed on the STFC cloud using
[Cluster API](https://stfc.atlassian.net/wiki/spaces/CLOUDKB/pages/285769806/Kubernetes).

## Notes

TODO: Clean these up

- `ansible-playbook -e cloud=da-proto -e server_name=k8s-management-vm -e key_name=martyngigg  management_vm_up.yml`
  creates a new management VM for the clusters as recommended by the SCD cloud team. You'll have to type yes to
  accept the host key.
  - Note the IP address and put the following in a `management_vm.txt` file, replacing `X.X.X.X` with the new ip:

```ini
[all]
X.X.X.X ansible_user=ubuntu
```

- `ansible-playbook -i management_vm.txt management_vm_prepare.yml` prepares the machine for use as a management
  VM. Amongst other tasks the playbook copies [files/capi/bootstrap](./files/capi/bootstrap.sh) to the machine.
  This script is intended to bootstrap a local microk8s cluster that can then be used to create other
  clusters.

- Run the `capi/bootstrap.sh` script to setup a local microk8s cluster that is ready to
  bootstrap the management cluster.

- ssh in the new management node, cd into the `capi/management-cluster` directory and deploy:

```sh
helm upgrade management capi/openstack-cluster --install -f values.yaml -f clouds.yaml -f user-values.yaml -f flavors.yaml -n clusters
```

- wait and watch `clusterctl describe cluster management -n clusters` until it says `Read: True`.
  - check `kubectl logs deploy/capo-controller-manager -n capo-system -f` for any errors if the cluster does not become ready

- Capture the new kubeconfig, `clusterctl get kubeconfig management -n clusters > management.kubeconfig`

- Move the control plane pods to the management cluster:

```sh
clusterctl init --infrastructure=openstack:v0.10.4 --kubeconfig=management.kubeconfig
clusterctl move --to-kubeconfig management.kubeconfig -n clusters
kubectl get kubeadmcontrolplane --kubeconfig=management.kubeconfig -n clusters
```

- Shutdown the local microk8s cluster: `microk8s stop`

- Swap the default kube config for the management cluster:

```sh
mv ~/.kube/config ~/.kube/microk8s.kubeconfig
cp management.kubeconfig ~/.kube/config
```

- Update the cluster to ensure it matches the helm charts

```sh
# Update the cluster to ensure everything lines up with your helm chart
helm upgrade cluster-api-addon-provider capi-addons/cluster-api-addon-provider --install --wait --version 0.5.6 -n clusters
helm upgrade management capi/openstack-cluster --install -f values.yaml -f clouds.yaml -f user-values.yaml -f flavors.yaml --wait -n clusters
```
