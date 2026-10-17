# K3s / Kubernetes support (design proposal)

Origin: https://github.com/LumaryLabsLLC/muxin
First implementation: feature/k3s-readonly-inventory

Muxin already manages SSH machines, persistent agent sessions, Docker workspaces,
self-hosted runners, and an agent-facing CLI/MCP API. K3s should be a separate,
opt-in execution backend rather than pretending a Kubernetes pod is a normal SSH
machine. Kubernetes is an API, not a remote shell into the cluster's nodes.

## Stage 1: read-only cluster inventory

- Use local kubectl or the embedded `k3s kubectl` when standalone kubectl is unavailable; respect the user's kubeconfig. Never silently retry a failed standalone kubectl against another client.
- Allow explicit context and namespace selection; never store tokens or alter context.
- List node readiness and pods (namespace, phase, assigned node, ready containers,
  restarts). Expose through CLI --json and MCP.
- Handle RBAC restrictions without claiming a denied inventory is empty.
- No cluster mutations, privileged containers, or new dependencies.
- Use a dedicated low-privilege kubeconfig for MCP use; the K3s server's built-in admin kubeconfig is cluster-admin and should not be distributed to agents. Scope pod lists by namespace wherever possible.
- Reproducible live smoke test: `bash scripts/test-k3s-inventory.sh` starts an isolated k3d cluster and probes Muxin's CLI, with temporary kubeconfig and cleanup; run only when Docker is available.
- No assumption that kubectl or a cluster exists on every Muxin install.

## Stage 2: persistent agent workspaces (requires upstream design agreement)

- Opt-in cluster + namespace, bounded service account and RBAC.
- Create dedicated Pod/Deployment for each workspace with a persistent volume.
- Route workspace commands through Kubernetes exec, and logs through the Pod API.
- Preserve workspace identity and files across restarts. Never use ephemeral Pod
  filesystem for a persistent workspace.
- Only mount selected secrets and images. No hostPath or privileged Pod defaults.
- Honor user confirmation for create/destroy/exec and keep an audit trail.
- Handle rescheduling, Pending/CrashLoopBackOff, expired credentials, and
  namespace-level resource quotas.
- Define equivalent semantics for existing workspace_* CLI/MCP operations.

## Stage 3: fleet UX and placement

- Show selected clusters/nodes/workloads beside SSH machines, with clear
  distinctions between physical hosts and pods.
- Add resource-aware placement, per-job labels, GPUs, and per-cluster guardrails.
- UI lifecycle operations require explicit user approval.

## Evidence limits

Stage 1 unit tests cover parsing, status, restarts, ordering and invalid
namespace values. Without a real cluster/kubectl and Rust toolchain in this
execution environment, neither an end-to-end K3s verification nor local
cargo test can be claimed. Run cargo test --workspace --locked and a
namespace-scoped smoke test before promotion.

## Upstream permission

CONTRIBUTING.md asks contributors to open an issue and agree on major changes
before a PR. Origin issue creation was attempted and returned HTTP 403 from
the connected GitHub integration. This fork-only branch is not approval to
post a feature PR upstream. Preserve Adib Hanna/LumaryLabsLLC provenance.

### Fork prototype (not validated)

The stacked `feature/k3s-persistent-workspaces` branch adds a separate
`muxin k8s space` CLI/MCP surface with manifest planning, creation, listing,
status, command execution and deployment deletion. It uses a pre-existing PVC,
a one-replica Deployment with Recreate strategy, explicit user approval and a
namespace-scoped kubeconfig. Deleting never removes the PVC. This is still not
wired into the SSH machine workspace registry or Muxin's native GUI. Tests are
authored but not yet executed by this contributor environment; origin promotion
requires maintainer design agreement and reviewers' results.

