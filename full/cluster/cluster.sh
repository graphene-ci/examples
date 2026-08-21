#!/usr/bin/env bash
# The full example's REAL cluster: a kind cluster with Crossplane and
# the Yandex Cloud provider, wired into a graphene installation — the
# user's cluster the pipeline assumes (it declares resources INTO it,
# never declares it).
#
# Everything comes from ./.env (see .env.example). Idempotent: run it
# again after changing the env — existing pieces are kept or upgraded.
#
#   ./cluster.sh up      create/converge everything
#   ./cluster.sh down    delete the kind cluster (cloud resources are
#                        the pipeline's to tear down — do not force)
#   ./cluster.sh status  show what is up

set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

# --- configuration (env with defaults) --------------------------------
KIND_CLUSTER="${KIND_CLUSTER:-graphene-example}"
CROSSPLANE_VERSION="${CROSSPLANE_VERSION:-1.20.0}"
PROVIDER_YC_PACKAGE="${PROVIDER_YC_PACKAGE:-xpkg.upbound.io/yandexcloud/crossplane-provider-yc}"
PROVIDER_YC_VERSION="${PROVIDER_YC_VERSION:-v0.14.0}"
# The Yandex service-account authorized key, JSON (yc iam key create).
YC_SA_KEY_FILE="${YC_SA_KEY_FILE:-}"
YC_CLOUD_ID="${YC_CLOUD_ID:-}"
YC_FOLDER_ID="${YC_FOLDER_ID:-}"
# Optional: the bare machine's ssh key to store as the pipeline secret.
BARE_SSH_KEY_FILE="${BARE_SSH_KEY_FILE:-}"
# graphenectl context to store the pipeline secrets into; empty skips
# the graphene wiring (cluster-only mode).
GRAPHENE_CONTEXT="${GRAPHENE_CONTEXT:-}"

log() { echo "[cluster] $*" >&2; }
die() { echo "[cluster] ERROR: $*" >&2; exit 1; }

need() {
  command -v "$1" >/dev/null || die "$1 is required (install it and re-run)"
}

kubeconfig() { kind get kubeconfig --name "$KIND_CLUSTER"; }

up() {
  need kind; need kubectl; need helm
  [[ -n "$YC_SA_KEY_FILE" ]] || die "YC_SA_KEY_FILE is not set (see .env.example)"
  [[ -f "$YC_SA_KEY_FILE" ]] || die "YC_SA_KEY_FILE $YC_SA_KEY_FILE does not exist"
  [[ -n "$YC_CLOUD_ID" ]] || die "YC_CLOUD_ID is not set"
  [[ -n "$YC_FOLDER_ID" ]] || die "YC_FOLDER_ID is not set"

  # 1. The cluster.
  if kind get clusters | grep -qx "$KIND_CLUSTER"; then
    log "kind cluster $KIND_CLUSTER already exists"
  else
    log "creating kind cluster $KIND_CLUSTER"
    kind create cluster --name "$KIND_CLUSTER" --wait 120s
  fi
  export KUBECONFIG
  KUBECONFIG="$(mktemp)"
  kubeconfig > "$KUBECONFIG"

  # 2. Crossplane.
  helm repo add crossplane-stable https://charts.crossplane.io/stable >/dev/null
  helm repo update crossplane-stable >/dev/null
  log "installing crossplane $CROSSPLANE_VERSION"
  helm upgrade --install crossplane crossplane-stable/crossplane \
    --namespace crossplane-system --create-namespace \
    --version "$CROSSPLANE_VERSION" --wait

  # 3. The Yandex provider.
  log "installing provider $PROVIDER_YC_PACKAGE:$PROVIDER_YC_VERSION"
  kubectl apply -f - <<EOF
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-yc
spec:
  package: $PROVIDER_YC_PACKAGE:$PROVIDER_YC_VERSION
EOF
  log "waiting for the provider to become healthy"
  kubectl wait provider.pkg.crossplane.io/provider-yc \
    --for=condition=Healthy --timeout=300s

  # 4. Credentials + the default ProviderConfig the example's resources
  # resolve to (they carry no ProviderConfigRef).
  kubectl -n crossplane-system create secret generic yc-creds \
    --from-file=credentials="$YC_SA_KEY_FILE" \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl apply -f - <<EOF
apiVersion: yandex-cloud.jet.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
spec:
  credentials:
    cloudId: $YC_CLOUD_ID
    folderId: $YC_FOLDER_ID
    source: Secret
    secretRef:
      name: yc-creds
      namespace: crossplane-system
      key: credentials
EOF

  # 5. Wire the graphene installation: the pipeline reads the cluster
  # through the "kubeconfig" secret; the bare machine's key through
  # "bare-ssh-key". Both are NAMES in code — values live server-side.
  if [[ -n "$GRAPHENE_CONTEXT" ]]; then
    need graphenectl
    log "storing pipeline secrets into graphene (context $GRAPHENE_CONTEXT)"
    kubeconfig | graphenectl secret set kubeconfig --context "$GRAPHENE_CONTEXT"
    if [[ -n "$BARE_SSH_KEY_FILE" ]]; then
      graphenectl secret set bare-ssh-key --context "$GRAPHENE_CONTEXT" < "$BARE_SSH_KEY_FILE"
    fi
  else
    log "GRAPHENE_CONTEXT is empty — skipping the graphene wiring"
    log "store the kubeconfig yourself: kind get kubeconfig --name $KIND_CLUSTER | graphenectl secret set kubeconfig"
  fi

  log "cluster is up:"
  status
}

status() {
  if ! kind get clusters 2>/dev/null | grep -qx "$KIND_CLUSTER"; then
    log "kind cluster $KIND_CLUSTER: absent"
    return
  fi
  export KUBECONFIG
  KUBECONFIG="$(mktemp)"
  kubeconfig > "$KUBECONFIG"
  kubectl get provider.pkg.crossplane.io provider-yc 2>/dev/null || true
  kubectl get providerconfig.yandex-cloud.jet.crossplane.io default 2>/dev/null || true
  kubectl -n crossplane-system get pods 2>/dev/null | head -5 || true
}

down() {
  need kind
  log "deleting kind cluster $KIND_CLUSTER"
  log "NOTE: cloud resources created by pipelines are NOT touched —"
  log "      tear them down through the pipeline (stand TTL / delete)"
  kind delete cluster --name "$KIND_CLUSTER"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
