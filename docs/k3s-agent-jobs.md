# K3s-native bounded agent jobs

This feature is a separate, stacked slice following initial K3s inventory and
workload observability. Jobs are short-lived batch tasks, **not** durable agent
sessions or persistent workspaces.

## Safety contracts

- All mutations require an explicit Kubernetes context, namespace, and
  `confirm=true` / CLI `--yes` flag. Demo mode never calls kubectl.
- Require a consciously selected KUBECONFIG environment variable. K3s's
  default admin kubeconfig is unrestricted; do not make a mutation using it.
- Create uses an exact JSON manifest on kubectl stdin (not a host shell, never
  `kubectl apply` that overwrites a pre-existing workload). Delete only touches
  resources labeled as Muxin-managed agent Jobs.
- Pod does not mount host paths, Docker socket, Kubernetes API token, host
  network or privileged capabilities; defaults to unprivileged UID 1000 and
  seccomp RuntimeDefault. Use purpose-built non-root agent images.
- Each Job runs one pod, backoff zero, bounded CPU/memory, optional GPU, a
  finite deadline and one-hour cleanup TTL.
- `emptyDir` loses files with the Job Pod. Export artifacts before expiry;
  don't call it a persistent workspace.
- RBAC needs only namespace-scoped Jobs create/get/delete and pods/log list/get
  as needed. Do not grant cluster-admin.

## Design boundaries

Future persistent workspace support should use a Deployment and an explicitly
provisioned PVC, preserve Muxin workspace ids/command journals, and treat
namespace and claim ownership independently of transient pods. Do not simply
pretend a Kubernetes Pod is an SSH machine.

## Verification handoff

Reviewers: cargo fmt --all --check, cargo test --workspace --locked,
cargo build --no-default-features --locked, then a namespace-scoped K3s
`job plan -> create -> status -> logs -> delete` test with a dedicated service
account. The tests here have been authored but not executed. Preserve Muxin
upstream original author credit.
