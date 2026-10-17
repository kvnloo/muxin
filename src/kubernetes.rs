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
    pub nodes: Vec<Node>,
    pub pods: Vec<Pod>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

pub async fn inspect(context: Option<&str>, namespace: Option<&str>) -> Result<ClusterStatus, String> {
    validate_options(context, namespace)?;

    // An explicit context is pinned for both calls. Without one, report the
    // current context instead of silently labelling it "default".
    let selected = match context {
        Some(name) => name.to_string(),
        None => {
            let output = run_kubectl(None, &["config", "current-context"]).await?;
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
        kubectl_json(&selected, &["get", "nodes", "-o", "json"]),
        kubectl_json(&selected, &pod_args),
    );

    // Namespace-limited RBAC commonly denies node listing. Keep the useful
    // results and explain what could not be read instead of claiming zero nodes.
    let mut warnings = Vec::new();
    let nodes = match node_result {
        Ok(data) => parse_nodes(&data)?,
        Err(err) => {
            warnings.push(format!("Nodes: {err}"));
            Vec::new()
        }
    };
    let pods = match pod_result {
        Ok(data) => parse_pods(&data)?,
        Err(err) => {
            warnings.push(format!("Pods: {err}"));
            Vec::new()
        }
    };
    if warnings.len() == 2 {
        return Err(warnings.join("\n"));
    }

    Ok(ClusterStatus {
        context: selected,
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
        nodes: vec![
            Node { name: "control-1".into(), ready: "Ready".into() },
            Node { name: "worker-1".into(), ready: "Ready".into() },
        ],
        pods: pods.into_iter().filter(|pod| namespace.is_none_or(|n| n == pod.namespace)).collect(),
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

async fn kubectl_json(context: &str, args: &[&str]) -> Result<Value, String> {
    let output = run_kubectl(Some(context), args).await?;
    serde_json::from_slice(&output).map_err(|e| format!("kubectl returned invalid JSON: {e}"))
}

async fn run_kubectl(context: Option<&str>, args: &[&str]) -> Result<Vec<u8>, String> {
    let mut command = Command::new("kubectl");
    if let Some(context) = context {
        command.arg("--context").arg(context);
    }
    command.arg("--request-timeout=10s").args(args).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .map_err(|_| "kubectl took too long (20s).".to_string())?
        .map_err(|e| format!("Couldn't run kubectl: {e}. Install kubectl and check kubeconfig."))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail: String = stderr.trim().chars().take(300).collect();
        return Err(if detail.is_empty() {
            format!("kubectl exited with {}.", output.status)
        } else {
            detail
        });
    }
    if output.stdout.len() > 16 * 1024 * 1024 {
        return Err("kubectl returned more than 16 MiB; select a namespace.".into());
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
        assert_eq!(status.nodes.len(), 2);
        assert_eq!(status.pods.len(), 1);
        assert_eq!(status.pods[0].namespace, "agents");
        assert!(demo_status(None, Some("--all-namespaces")).is_err());
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
