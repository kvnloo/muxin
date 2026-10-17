#!/usr/bin/env bash
# Live K3s inventory smoke test. Needs cargo, docker, k3d, kubectl, jq.
# Creates and deletes only a uniquely named k3d test cluster.
set -Eeuo pipefail

for command in cargo docker k3d kubectl jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf '%s is required for this test.\n' "$command" >&2
    exit 2
  fi
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running or accessible." >&2
  exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d)"
cluster="muxin-smoke-$(date +%s)-$$"
cleanup() {
  k3d cluster delete "$cluster" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT

cd "$root"
cargo build --no-default-features --locked
cargo test --workspace --locked

# Demo mode must not run kubectl or touch kubeconfig.
./target/debug/muxin k3s --demo --json | jq -e '
  .context == "demo"
  and (.nodes | length) == 2
  and (.pods | length) == 2
' >/dev/null

# Never merge this cluster into or switch the user's kubeconfig.
k3d cluster create "$cluster" \
  --agents 1 \
  --wait \
  --timeout 360s \
  --kubeconfig-update-default=false \
  --kubeconfig-switch-context=false

k3d kubeconfig get "$cluster" > "$scratch/kubeconfig.yaml"
chmod 600 "$scratch/kubeconfig.yaml"
export KUBECONFIG="$scratch/kubeconfig.yaml"
context="$(kubectl config current-context)"

kubectl --context "$context" create namespace agents
kubectl --context "$context" --namespace agents run muxin-smoke \
  --image=busybox:1.36 --restart=Never -- sleep 3600
kubectl --context "$context" --namespace agents wait \
  --for=condition=Ready pod/muxin-smoke --timeout=180s

./target/debug/muxin k8s --context "$context" --namespace agents --json > "$scratch/inventory.json"

jq -e --arg context "$context" '
  .context == $context
  and .namespace == "agents"
  and (.warnings | length) == 0
  and (.nodes | length) >= 2
  and ([.nodes[] | select(.ready == "Ready" and .schedulable == true)] | length) >= 2
  and ([.pods[] | select(.name == "muxin-smoke" and .phase == "Running" and .ready == 1)] | length) == 1
' "$scratch/inventory.json" >/dev/null

echo "PASS: demo isolation + live k3d/K3s node and pod inventory"
