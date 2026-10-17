//! Read-only Kubernetes and K3s inventory using the user's kubectl context.
//! No cluster credentials are stored and no workload is changed.

use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use tokio::process::Command;

#[derive(Debug, Clone, Serialize)]
pub struct Node {
    pub name: String,
    pub ready: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Pod {
    pub namespace: String,
    pub name: String,
    pub node: Option<String>,
    pub phase: String,
    pub ready: usize,
    pub containers: usize,
    pub restarts: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterStatus {
    pub context: String,
    pub namespace: String,
    /// None means this list could not be read; Some([]) means it was empty.
    pub nodes: Option<Vec<Node>>,
    pub pods: Option<Vec<Pod>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

pub async fn inspect(context: Option<&str>, namespace: Option<&str>) -> Result<ClusterStatus, String> {
    inspect_with_tools("kubectl", "k3s", context, namespace).await
}

/// The program arguments stay separate (no shell) for both implementations.
/// With K3s alone installed, its embedded `k3s kubectl` is a compatible client.
async fn inspect_with_tools(
    kubectl: &str,
    k3s: &str,
    context: Option<&str>,
    namespace: Option<&str>,
) -> Result<ClusterStatus, String> {
    validate_options(context, namespace)?;

    // An explicit context is pinned for both calls. Without one, report the
    // current context instead of silently labelling it "default".
    let selected = match context {
        Some(name) => name.to_string(),
        None => {
            let output = run_kubectl_with(kubectl, k3s, None, &["config", "current-context"]).await?;
            String::from_utf8(output)
                .map_err(|_| "kubectl returned an invalid context name.".to_string())?
                .trim()
                .to_string()
        }
    };
    if selected.is_empty() {
        return Err("kubectl has no current context. Configure a cluster with kubeconfig first.".into());
    }

    // Use the same explicit context for every request so a concurrent
    // kubeconfig context switch cannot mix two clusters in one result.
    let mut pod_args = vec!["get", "pods", "-o", "json"];
    match namespace {
        Some(name) => pod_args.extend(["--namespace", name]),
        None => pod_args.push("--all-namespaces"),
    }
    let (node_result, pod_result) = tokio::join!(
        kubectl_json(kubectl, k3s, &selected, &["get", "nodes", "-o", "json"]),
        kubectl_json(kubectl, k3s, &selected, &pod_args),
    );

    build_status(selected, namespace, node_result, pod_result)
}

/// A denied or malformed resource list is unavailable, never an empty list.
/// Namespace-scoped readers may inspect pods without permission to list nodes.
fn build_status(
    context: String,
    namespace: Option<&str>,
    node_result: Result<Value, String>,
    pod_result: Result<Value, String>,
) -> Result<ClusterStatus, String> {
    let mut warnings = Vec::new();
    let nodes = match node_result.and_then(|value| parse_nodes(&value)) {
        Ok(nodes) => Some(nodes),
        Err(err) => {
            warnings.push(format!("Nodes: {err}"));
            None
        }
    };
    let pods = match pod_result.and_then(|value| parse_pods(&value)) {
        Ok(pods) => Some(pods),
        Err(err) => {
            warnings.push(format!("Pods: {err}"));
            None
        }
    };
    if nodes.is_none() && pods.is_none() {
        return Err(warnings.join("\n"));
    }

    Ok(ClusterStatus {
        context,
        namespace: namespace.unwrap_or("all").to_string(),
        nodes,
        pods,
        warnings,
    })
}

fn validate_options(context: Option<&str>, namespace: Option<&str>) -> Result<(), String> {
    if let Some(namespace) = namespace {
        if !valid_namespace(namespace) {
            return Err("Namespace must be a Kubernetes DNS label (up to 63 lowercase letters, digits or hyphens).".into());
        }
    }
    if context.is_some_and(|name| name.trim().is_empty()) {
        return Err("Context cannot be empty.".into());
    }
    Ok(())
}

/// Sample cluster for Muxin's --demo mode. Never contacts kubectl.
pub fn demo_status(context: Option<&str>, namespace: Option<&str>) -> Result<ClusterStatus, String> {
    validate_options(context, namespace)?;
    let pods = vec![
        Pod { namespace: "agents".into(), name: "agent-worker".into(), node: Some("worker-1".into()), phase: "Running".into(), ready: 1, containers: 1, restarts: 0 },
        Pod { namespace: "kube-system".into(), name: "coredns".into(), node: Some("worker-1".into()), phase: "Running".into(), ready: 1, containers: 1, restarts: 1 },
    ];
    Ok(ClusterStatus {
        context: "demo".into(),
        namespace: namespace.unwrap_or("all").into(),
        nodes: Some(vec![
            Node { name: "control-1".into(), ready: "Ready".into() },
            Node { name: "worker-1".into(), ready: "Ready".into() },
        ]),
        pods: Some(pods.into_iter().filter(|pod| namespace.is_none_or(|n| n == pod.namespace)).collect()),
        warnings: Vec::new(),
    })
}

fn valid_namespace(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && (bytes[bytes.len() - 1].is_ascii_lowercase() || bytes[bytes.len() - 1].is_ascii_digit())
        && bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

async fn kubectl_json(kubectl: &str, k3s: &str, context: &str, args: &[&str]) -> Result<Value, String> {
    let output = run_kubectl_with(kubectl, k3s, Some(context), args).await?;
    serde_json::from_slice(&output).map_err(|e| format!("kubectl returned invalid JSON: {e}"))
}

enum CommandError {
    Missing,
    Failed(String),
}

/// Prefer the standalone Kubernetes client; use K3s' embedded client when
/// the standalone executable is absent. A real kubectl failure does NOT fall
/// back to a different client, kubeconfig, or cluster.
async fn run_kubectl_with(kubectl: &str, k3s: &str, context: Option<&str>, args: &[&str]) -> Result<Vec<u8>, String> {
    match execute_kubectl(kubectl, false, context, args).await {
        Ok(output) => Ok(output),
        Err(CommandError::Missing) => match execute_kubectl(k3s, true, context, args).await {
            Ok(output) => Ok(output),
            Err(CommandError::Missing) => Err("Neither kubectl nor k3s was found. Install kubectl or use a machine with K3s and configure kubeconfig.".into()),
            Err(CommandError::Failed(err)) => Err(err),
        },
        Err(CommandError::Failed(err)) => Err(err),
    }
}

async fn execute_kubectl(
    binary: &str,
    embedded: bool,
    context: Option<&str>,
    args: &[&str],
) -> Result<Vec<u8>, CommandError> {
    let mut command = Command::new(binary);
    if embedded {
        command.arg("kubectl");
    }
    if let Some(context) = context {
        command.arg("--context").arg(context);
    }
    command.arg("--request-timeout=10s").args(args).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .map_err(|_| CommandError::Failed(format!("{binary} took too long (20s).")))?
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                CommandError::Missing
            } else {
                CommandError::Failed(format!("Couldn't run {binary}: {e}"))
            }
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail: String = stderr.trim().chars().take(300).collect();
        return Err(CommandError::Failed(if detail.is_empty() {
            format!("{binary} exited with {}.", output.status)
        } else {
            detail
        }));
    }
    if output.stdout.len() > 16 * 1024 * 1024 {
        return Err(CommandError::Failed("kubectl returned more than 16 MiB; select a namespace.".into()));
    }
    Ok(output.stdout)
}

fn items(data: &Value) -> Result<&Vec<Value>, String> {
    data.get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| "kubectl result is missing its items list.".into())
}

fn parse_nodes(data: &Value) -> Result<Vec<Node>, String> {
    let mut result = Vec::new();
    for entry in items(data)? {
        let name = entry["metadata"]["name"]
            .as_str()
            .ok_or("Kubernetes node is missing metadata.name.")?
            .to_string();
        let condition = entry["status"]["conditions"]
            .as_array()
            .and_then(|conditions| conditions.iter().find(|c| c["type"] == "Ready"))
            .and_then(|c| c["status"].as_str());
        let ready = match condition {
            Some("True") => "Ready",
            Some("False") => "NotReady",
            _ => "Unknown",
        };
        result.push(Node { name, ready: ready.into() });
    }
    result.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(result)
}

fn parse_pods(data: &Value) -> Result<Vec<Pod>, String> {
    let mut result = Vec::new();
    for entry in items(data)? {
        let name = entry["metadata"]["name"]
            .as_str()
            .ok_or("Kubernetes pod is missing metadata.name.")?
            .to_string();
        let namespace = entry["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
        let statuses = entry["status"]["containerStatuses"].as_array();
        let ready = statuses
            .map(|s| s.iter().filter(|c| c["ready"] == true).count())
            .unwrap_or(0);
        let containers = entry["spec"]["containers"]
            .as_array()
            .map(Vec::len)
            .unwrap_or_else(|| statuses.map(Vec::len).unwrap_or(0));
        let restarts = statuses
            .map(|s| s.iter().filter_map(|c| c["restartCount"].as_u64()).sum())
            .unwrap_or(0);
        let phase = if !entry["metadata"]["deletionTimestamp"].is_null() {
            "Terminating"
        } else {
            entry["status"]["phase"].as_str().unwrap_or("Unknown")
        };
        result.push(Pod {
            namespace,
            name,
            node: entry["spec"]["nodeName"].as_str().map(str::to_string),
            phase: phase.into(),
            ready,
            containers,
            restarts,
        });
    }
    result.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nodes_report_readiness_and_sort_by_name() {
        let nodes = parse_nodes(&json!({ "items": [
            {"metadata": {"name": "z"}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}},
            {"metadata": {"name": "a"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        ]})).unwrap();
        assert_eq!(nodes[0].name, "a");
        assert_eq!(nodes[0].ready, "Ready");
        assert_eq!(nodes[1].ready, "NotReady");
    }

    #[test]
    fn pods_report_restarts_and_namespace() {
        let pods = parse_pods(&json!({ "items": [{
            "metadata": {"name": "agent", "namespace": "agents"},
            "spec": {"nodeName": "worker-1", "containers": [{}, {}]},
            "status": {
                "phase": "Running",
                "containerStatuses": [
                    {"ready": true, "restartCount": 2},
                    {"ready": false, "restartCount": 1}
                ]
            }
        }]})).unwrap();
        assert_eq!(pods[0].namespace, "agents");
        assert_eq!(pods[0].node.as_deref(), Some("worker-1"));
        assert_eq!(pods[0].ready, 1);
        assert_eq!(pods[0].containers, 2);
        assert_eq!(pods[0].restarts, 3);
    }

    #[test]
    fn malformed_lists_are_not_reported_as_empty() {
        assert!(parse_nodes(&json!({})).is_err());
        assert!(parse_pods(&json!({"items": {}})).is_err());
    }

    #[test]
    fn demo_uses_only_sample_data_even_with_real_context_name() {
        let status = demo_status(Some("production"), Some("agents")).unwrap();
        assert_eq!(status.context, "demo");
        assert_eq!(status.nodes.as_ref().unwrap().len(), 2);
        assert_eq!(status.pods.as_ref().unwrap().len(), 1);
        assert_eq!(status.pods.as_ref().unwrap()[0].namespace, "agents");
        assert!(demo_status(None, Some("--all-namespaces")).is_err());
    }

    #[test]
    fn rbac_denial_is_not_an_empty_cluster() {
        let status = build_status(
            "lab".into(),
            Some("agents"),
            Err("nodes is forbidden".into()),
            Ok(json!({"items": [{
                "metadata": {"namespace": "agents", "name": "agent-1"},
                "spec": {"containers": [{}]},
                "status": {"phase": "Pending"}
            }]})),
        ).unwrap();
        assert!(status.nodes.is_none());
        assert_eq!(status.pods.as_ref().unwrap().len(), 1);
        assert_eq!(status.warnings, vec!["Nodes: nodes is forbidden"]);

        let serialized = serde_json::to_value(status).unwrap();
        assert!(serialized["nodes"].is_null());
        assert!(serialized["pods"].is_array());
    }

    #[test]
    fn empty_list_and_invalid_json_are_different() {
        let status = build_status(
            "lab".into(), None,
            Ok(json!({"items": []})),
            Err("pods forbidden".into())
        ).unwrap();
        assert_eq!(status.nodes.as_ref().unwrap().len(), 0);
        assert!(status.pods.is_none());
        assert!(build_status(
            "lab".into(), None,
            Err("nodes forbidden".into()),
            Err("pods forbidden".into()),
        ).is_err());
        let status = build_status(
            "lab".into(), None,
            Ok(json!({"items": []})),
            Ok(json!({"items": {}})),
        ).unwrap();
        assert!(status.pods.is_none(), "malformed pods must be unavailable");
        assert_eq!(status.warnings.len(), 1);
    }

    /// End-to-end fake K3s executable: verifies prefix, pinned context and
    /// namespace flags without requiring or touching a real cluster.
    #[cfg(unix)]
    #[tokio::test]
    async fn embedded_k3s_client_works_when_kubectl_is_missing() {
        use std::os::unix::fs::PermissionsExt;

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("muxin-k3s-test-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let binary = dir.join("k3s");
        let script = r#"#!/bin/sh
if [ "$1" != "kubectl" ]; then exit 17; fi
shift
case " $* " in
  *" config current-context "*) printf 'test-k3s\n' ;;
  *" get nodes -o json "*) printf '{"items":[{"metadata":{"name":"worker"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}' ;;
  *" get pods -o json --namespace agents "*) printf '{"items":[{"metadata":{"name":"agent","namespace":"agents"},"status":{"phase":"Running"}}]}' ;;
  *) exit 19 ;;
esac
"#;
        std::fs::write(&binary, script).unwrap();
        let mut mode = std::fs::metadata(&binary).unwrap().permissions();
        mode.set_mode(0o700);
        std::fs::set_permissions(&binary, mode).unwrap();

        let missing = dir.join("kubectl-not-installed");
        let status = inspect_with_tools(
            missing.to_str().unwrap(),
            binary.to_str().unwrap(),
            None,
            Some("agents"),
        ).await.unwrap();
        assert_eq!(status.context, "test-k3s");
        assert_eq!(status.nodes.as_ref().unwrap()[0].ready, "Ready");
        assert_eq!(status.pods.as_ref().unwrap()[0].name, "agent");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn namespace_validation_blocks_flag_injection() {
        assert!(valid_namespace("agent-workers"));
        assert!(!valid_namespace(""));
        assert!(!valid_namespace("--all-namespaces"));
        assert!(!valid_namespace("has spaces"));
        assert!(!valid_namespace("UPPERCASE"));
    }
}
