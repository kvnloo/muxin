//! Read-only Kubernetes workload, event and pod-log inspection.
//! All commands use argv (no shell), a fixed selected kubeconfig context, and
//! the same bounded kubectl runner as the node/pod overview.

use serde::Serialize;
use serde_json::Value;

use super::{kubectl_json, run_kubectl_with, selected_context, valid_namespace};

#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    pub namespace: String,
    pub kind: String,
    pub name: String,
    /// Requested replicas, number scheduled, or Job completions.
    pub desired: u32,
    /// Ready replicas, number ready, or succeeded Job completions.
    pub ready: u32,
    /// Available replicas (Deployments) or ready count when applicable.
    pub available: Option<u32>,
    /// Number of actively running Job pods.
    pub active: Option<u32>,
    /// Failed Job pods.
    pub failed: Option<u32>,
    /// Deployment rollout paused.
    pub paused: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Workloads {
    pub context: String,
    pub namespace: String,
    pub items: Vec<Workload>,
    /// Errors mean that the corresponding kind was NOT inspected, not empty.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub namespace: String,
    pub kind: String,
    pub object: String,
    pub reason: String,
    pub message: String,
    pub event_type: String,
    pub last_seen: Option<String>,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Events {
    pub context: String,
    pub namespace: String,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PodLogs {
    pub context: String,
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
    pub previous: bool,
    pub output: String,
    /// True when the log limit was reached; older lines may have been omitted.
    pub possibly_truncated: bool,
}

pub async fn inspect_workloads(context: Option<&str>, namespace: Option<&str>) -> Result<Workloads, String> {
    validate_scope(namespace)?;
    let context = selected_context("kubectl", "k3s", context).await?;
    let (deployments, statefulsets, daemonsets, jobs) = tokio::join!(
        get_list(&context, namespace, "deployments"),
        get_list(&context, namespace, "statefulsets"),
        get_list(&context, namespace, "daemonsets"),
        get_list(&context, namespace, "jobs"),
    );
    let mut items = Vec::new();
    let mut warnings = Vec::new();
    for (kind, outcome) in [
        ("Deployment", deployments),
        ("StatefulSet", statefulsets),
        ("DaemonSet", daemonsets),
        ("Job", jobs),
    ] {
        match outcome.and_then(|v| parse_workloads(&v, kind)) {
            Ok(mut rows) => items.append(&mut rows),
            Err(err) => warnings.push(format!("{kind}: {err}")),
        }
    }
    if warnings.len() == 4 {
        return Err(warnings.join("\n"));
    }
    items.sort_by(|a, b| (&a.namespace, &a.kind, &a.name).cmp(&(&b.namespace, &b.kind, &b.name)));
    Ok(Workloads {
        context,
        namespace: namespace.unwrap_or("all").into(),
        items,
        warnings,
    })
}

async fn get_list(context: &str, namespace: Option<&str>, kind: &str) -> Result<Value, String> {
    let mut argv = vec!["get", kind, "-o", "json"];
    if let Some(namespace) = namespace {
        argv.extend(["--namespace", namespace]);
    } else {
        argv.push("--all-namespaces");
    }
    kubectl_json("kubectl", "k3s", context, &argv).await
}

fn parse_workloads(raw: &Value, kind: &str) -> Result<Vec<Workload>, String> {
    let items = raw["items"].as_array().ok_or("Resource list is missing items array.")?;
    let mut workloads = Vec::with_capacity(items.len());
    for object in items {
        let name = object["metadata"]["name"].as_str().ok_or("Workload is missing metadata.name.")?;
        let namespace = object["metadata"]["namespace"].as_str().unwrap_or("default");
        let desired;
        let ready;
        let mut available = None;
        let mut active = None;
        let mut failed = None;
        let mut paused = false;
        match kind {
            "Deployment" => {
                desired = object["spec"]["replicas"].as_u64().unwrap_or(1) as u32;
                ready = object["status"]["readyReplicas"].as_u64().unwrap_or(0) as u32;
                available = Some(object["status"]["availableReplicas"].as_u64().unwrap_or(0) as u32);
                paused = object["spec"]["paused"] == true;
            }
            "StatefulSet" => {
                desired = object["spec"]["replicas"].as_u64().unwrap_or(1) as u32;
                ready = object["status"]["readyReplicas"].as_u64().unwrap_or(0) as u32;
                available = Some(object["status"]["availableReplicas"].as_u64().unwrap_or(ready as u64) as u32);
            }
            "DaemonSet" => {
                desired = object["status"]["desiredNumberScheduled"].as_u64().unwrap_or(0) as u32;
                ready = object["status"]["numberReady"].as_u64().unwrap_or(0) as u32;
                available = Some(object["status"]["numberAvailable"].as_u64().unwrap_or(0) as u32);
            }
            "Job" => {
                desired = object["spec"]["completions"].as_u64().unwrap_or(1) as u32;
                ready = object["status"]["succeeded"].as_u64().unwrap_or(0) as u32;
                active = Some(object["status"]["active"].as_u64().unwrap_or(0) as u32);
                failed = Some(object["status"]["failed"].as_u64().unwrap_or(0) as u32);
            }
            _ => return Err(format!("Unknown workload kind: {kind}")),
        }
        workloads.push(Workload {
            namespace: namespace.into(),
            kind: kind.into(),
            name: name.into(),
            desired,
            ready,
            available,
            active,
            failed,
            paused,
        });
    }
    Ok(workloads)
}

pub async fn inspect_events(context: Option<&str>, namespace: Option<&str>, limit: usize) -> Result<Events, String> {
    validate_scope(namespace)?;
    if limit == 0 || limit > 200 {
        return Err("Events limit must be between 1 and 200.".into());
    }
    let context = selected_context("kubectl", "k3s", context).await?;
    let raw = get_list(&context, namespace, "events").await?;
    Ok(Events {
        context,
        namespace: namespace.unwrap_or("all").into(),
        events: parse_events(&raw, limit)?,
    })
}

fn parse_events(raw: &Value, limit: usize) -> Result<Vec<Event>, String> {
    let items = raw["items"].as_array().ok_or("Event list is missing items array.")?;
    let mut events = Vec::with_capacity(items.len().min(limit));
    for value in items {
        events.push(Event {
            namespace: value["metadata"]["namespace"].as_str().unwrap_or("default").into(),
            kind: value["involvedObject"]["kind"].as_str().unwrap_or("Unknown").into(),
            object: value["involvedObject"]["name"].as_str().unwrap_or("unknown").into(),
            reason: value["reason"].as_str().unwrap_or("Unknown").into(),
            message: value["message"].as_str().unwrap_or("").chars().take(800).collect(),
            event_type: value["type"].as_str().unwrap_or("Normal").into(),
            last_seen: value["lastTimestamp"].as_str()
                .or_else(|| value["eventTime"].as_str())
                .or_else(|| value["metadata"]["creationTimestamp"].as_str())
                .map(str::to_string),
            count: value["count"].as_u64().unwrap_or(1).min(u32::MAX as u64) as u32,
        });
    }
    events.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
    events.truncate(limit);
    Ok(events)
}

pub async fn inspect_logs(
    context: Option<&str>,
    namespace: &str,
    pod: &str,
    container: Option<&str>,
    previous: bool,
    tail: u32,
    limit_bytes: u32,
) -> Result<PodLogs, String> {
    if !valid_namespace(namespace) {
        return Err("Pod logs require an explicit valid namespace.".into());
    }
    if !valid_resource_name(pod) {
        return Err("Pod must be a valid Kubernetes DNS subdomain.".into());
    }
    if container.is_some_and(|c| !valid_namespace(c)) {
        return Err("Container must be a Kubernetes DNS label.".into());
    }
    if tail == 0 || tail > 1000 || limit_bytes < 1024 || limit_bytes > 1024 * 1024 {
        return Err("Pod log limits: tail 1..1000 and bytes 1024..1048576.".into());
    }
    let context = selected_context("kubectl", "k3s", context).await?;
    let tail_arg = format!("--tail={tail}");
    let bytes_arg = format!("--limit-bytes={limit_bytes}");
    let mut argv = vec![
        "logs", pod, "--namespace", namespace, &tail_arg, &bytes_arg,
    ];
    if let Some(container) = container {
        argv.extend(["--container", container]);
    }
    if previous {
        argv.push("--previous");
    }
    let bytes = run_kubectl_with("kubectl", "k3s", Some(&context), &argv).await?;
    let truncated = bytes.len() >= limit_bytes as usize;
    Ok(PodLogs {
        context,
        namespace: namespace.into(),
        pod: pod.into(),
        container: container.map(str::to_string),
        previous,
        output: String::from_utf8_lossy(&bytes).to_string(),
        possibly_truncated: truncated,
    })
}

fn valid_resource_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 253 && name.split('.').all(valid_namespace)
}

/// Synthetic data for --demo. None of these call kubectl or access kubeconfig.
pub fn demo_workloads(namespace: Option<&str>) -> Result<Workloads, String> {
    validate_scope(namespace)?;
    let items = vec![
        Workload { namespace: "agents".into(), kind: "Deployment".into(), name: "coder".into(),
            desired: 3, ready: 2, available: Some(2), active: None, failed: None, paused: false },
        Workload { namespace: "agents".into(), kind: "Job".into(), name: "evaluation".into(),
            desired: 1, ready: 0, available: None, active: Some(1), failed: Some(0), paused: false },
        Workload { namespace: "kube-system".into(), kind: "DaemonSet".into(), name: "network".into(),
            desired: 2, ready: 2, available: Some(2), active: None, failed: None, paused: false },
    ];
    Ok(Workloads { context: "demo".into(), namespace: namespace.unwrap_or("all").into(),
        items: items.into_iter().filter(|w| namespace.is_none_or(|n| w.namespace == n)).collect(),
        warnings: Vec::new() })
}

pub fn demo_events(namespace: Option<&str>, limit: usize) -> Result<Events, String> {
    validate_scope(namespace)?;
    if limit == 0 || limit > 200 {
        return Err("Events limit must be between 1 and 200.".into());
    }
    let events = vec![Event { namespace: "agents".into(), kind: "Pod".into(),
        object: "evaluation".into(), reason: "Scheduled".into(), message: "Assigned to worker-1".into(),
        event_type: "Normal".into(), last_seen: Some("2026-10-17T00:00:00Z".into()), count: 1 }];
    Ok(Events { context: "demo".into(), namespace: namespace.unwrap_or("all").into(),
        events: events.into_iter().filter(|e| namespace.is_none_or(|n| e.namespace == n)).take(limit).collect() })
}

pub fn demo_logs(namespace: &str, pod: &str, container: Option<&str>, previous: bool, tail: u32, limit_bytes: u32) -> Result<PodLogs, String> {
    if !valid_namespace(namespace) || !valid_resource_name(pod) || container.is_some_and(|c| !valid_namespace(c)) {
        return Err("Invalid namespace, pod or container.".into());
    }
    if tail == 0 || tail > 1000 || limit_bytes < 1024 || limit_bytes > 1024 * 1024 {
        return Err("Pod log limits: tail 1..1000 and bytes 1024..1048576.".into());
    }
    Ok(PodLogs { context: "demo".into(), namespace: namespace.into(), pod: pod.into(),
        container: container.map(str::to_string), previous, output: "Example pod output (demo only).\n".into(),
        possibly_truncated: false })
}

fn validate_scope(namespace: Option<&str>) -> Result<(), String> {
    if namespace.is_some_and(|n| !valid_namespace(n)) {
        return Err("Namespace must be a valid Kubernetes DNS label.".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deployments_jobs_and_daemonsets_have_separate_ready_semantics() {
        let deployments = parse_workloads(&json!({"items": [{"metadata": {"namespace": "agents", "name": "coder"},
            "spec": {"replicas": 3}, "status": {"readyReplicas": 2, "availableReplicas": 2}}]}), "Deployment").unwrap();
        assert_eq!(deployments[0].desired, 3);
        assert_eq!(deployments[0].ready, 2);
        let jobs = parse_workloads(&json!({"items": [{"metadata": {"namespace": "agents", "name": "eval"},
            "spec": {"completions": 2}, "status": {"active": 1, "succeeded": 1, "failed": 3}}]}), "Job").unwrap();
        assert_eq!(jobs[0].ready, 1);
        assert_eq!(jobs[0].failed, Some(3));
        let daemonsets = parse_workloads(&json!({"items": [{"metadata": {"namespace": "agents", "name": "network"},
            "status": {"desiredNumberScheduled": 4, "numberReady": 3}}]}), "DaemonSet").unwrap();
        assert_eq!(daemonsets[0].desired, 4);
        assert_eq!(daemonsets[0].ready, 3);
    }

    #[test]
    fn event_order_and_namespace_validation() {
        let events = parse_events(&json!({"items":[
            {"metadata":{"namespace":"agents","creationTimestamp":"2026-10-17T01:00:00Z"},"reason":"Started"},
            {"metadata":{"namespace":"agents","creationTimestamp":"2026-10-17T02:00:00Z"},"reason":"Failed"}
        ]}), 1).unwrap();
        assert_eq!(events[0].reason, "Failed");
        assert!(!valid_resource_name("--namespace"));
        assert!(valid_resource_name("agent-worker-1"));
        assert!(valid_resource_name("worker.agent"));
    }
}
