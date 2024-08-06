#!/usr/bin/env bash
set -euo pipefail

CAPO_ADDON_VERSION="0.5.6"
CLUSTER_CTL_VERSION="v1.7.4"
HELM_VERSION="3.7"
KUBECTL_VERSION="1.28"
CAPI_OPENSTACK_VERSION="0.10.4"

CLUSTERS_NAMESPACE=clusters

function fetch() {
  curl -LO $1
}

function install_exe_to_path() {
  chmod u+x $1
  sudo mv $1 /usr/local/bin/
}

echo "Installing snapd and openstack client"
sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y \
  snapd \
  python3-openstackclient
echo "Installing kubectl version $KUBECTL_VERSION"
sudo snap install kubectl --classic --channel=$KUBECTL_VERSION/stable
echo "Installing helm version $HELM_VERSION"
sudo snap install helm --classic --channel=$HELM_VERSION/stable
echo "Installing k9s latest version"
sudo snap install k9s --classic --stable

# Make snap package binaries accessible
export PATH=$PATH:/snap/bin:/snap/k9s/current/bin
echo -e "\nexport PATH=\$PATH:/snap/bin" >> ~/.bashrc

# Enable kubectl bash completion
echo 'source <(kubectl completion bash)' >> ~/.bashrc

echo "Installing clusterctl version ${CLUSTER_CTL_VERSION}"
fetch "https://github.com/kubernetes-sigs/cluster-api/releases/download/${CLUSTER_CTL_VERSION}/clusterctl-linux-amd64"
mv clusterctl-linux-amd64 clusterctl
install_exe_to_path clusterctl

echo "Installing and starting microk8s..."
sudo snap install microk8s --classic --channel=${KUBECTL_VERSION}/stable
sudo microk8s status --wait-ready

echo "Exporting the kubeconfig file from microk8s"
mkdir -p ~/.kube/
sudo microk8s.config  > ~/.kube/config
sudo chown $USER ~/.kube/config
sudo chmod 600 ~/.kube/config
sudo microk8s enable dns

echo "Initialising cluster-api OpenStack provider..."
echo "If this fails you may need a GITHUB_TOKEN, see https://stfc.atlassian.net/wiki/spaces/CLOUDKB/pages/211878034/Cluster+API+Setup for details"
clusterctl init --infrastructure=openstack:v${CAPI_OPENSTACK_VERSION}

echo "Importing required helm repos and packages"
helm repo add capi https://stackhpc.github.io/capi-helm-charts
helm repo add capi-addons https://stackhpc.github.io/cluster-api-addon-provider
helm repo update
helm upgrade \
  cluster-api-addon-provider \
  capi-addons/cluster-api-addon-provider \
  --create-namespace \
  --install \
  --wait \
  --namespace "${CLUSTERS_NAMESPACE}" \
  --version "${CAPO_ADDON_VERSION}"

echo "You are now ready to create a cluster following the remaining instructions..."
echo "https://stfc.atlassian.net/wiki/spaces/CLOUDKB/pages/211878034/Cluster+API+Setup"
