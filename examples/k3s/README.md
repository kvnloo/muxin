# K3s permissions and storage examples

These manifests are **examples only**. Muxin does not apply them, generate
tokens, create namespaces or provision storage.

- `observer.yaml`: read-only Pod, controller, event and bounded log access
  to the `agents` namespace. It intentionally does not list cluster-scoped
  Nodes, so diagnostics may be `partial`.
- `operator.yaml`: adds namespaced Job and Deployment creation/deletion,
  PVC reads and Pod exec. It does **not** permit secret reads, privilege
  escalation or changes to namespaces, nodes, RoleBindings or cluster roles.
- Users must explicitly select `KUBECONFIG`, a kubeconfig context and a
  namespace. Use a scoped identity created and managed outside Muxin.

Check the effective identity:
```sh
kubectl --context homelab auth can-i list pods --namespace agents
kubectl --context homelab auth can-i create jobs --namespace agents
kubectl --context homelab auth can-i create pods/exec --namespace agents
kubectl --context homelab auth can-i list nodes
```

## PVC portability warning

K3s defaults to the `local-path` StorageClass. Its volumes carry node
affinity: a PVC may survive a restart or Pod recreation **on that node**, but
is not magically portable when the node is lost. Use Longhorn, NFS, another
CSI-backed solution, or other storage that is genuinely portable for cross-node
recovery. Verify access modes and node affinity in your cluster.

For a workspace, create the PVC deliberately before
`muxin k8s ... space create --claim <pvc>`. Muxin never creates or deletes
the PVC. Deleting the Deployment only removes the compute; data remains in
the claim. Storage backup and reclaim policy belong to the operator.

## Authorization boundary

RBAC controls what the selected client credential can do. Muxin's `--yes`
or `confirm=true` is *additional* user consent, not a substitute for RBAC.
Avoid default `/etc/rancher/k3s/k3s.yaml`: K3s documents it as an
unrestricted admin identity. Do not expose it to autonomous agents.

Sources:
- https://docs.k3s.io/cluster-access
- https://docs.k3s.io/add-ons/storage
- https://kubernetes.io/docs/reference/access-authn-authz/rbac/
