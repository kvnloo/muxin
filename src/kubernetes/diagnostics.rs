//! K3s fleet diagnosis. A partial RBAC view is never called "healthy".
//! This is read-only: no mutable action is dispatched by a diagnostic alert.

use serde::Serialize;

use super::{
    ClusterStatus, Events, Workloads, inspect, inspect_events, inspect_workloads,
    selected_context, run_kubectl_with,
};

#[derive(Debug, Clone, Serialize)]
pub struct Contexts {
    pub current: Option<String>,
    pub names: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub severity: String,
    pub source: String,
    pub namespace: Option<String>,
    pub name: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Diagnostics {
    pub context: String,
    pub namespace: String,
    /// "ok", "warning", "error", or "partial" if at least one resource
    /// family was inaccessible. A partial result never claims clean health.
    pub state: String,
    pub summary: String,
    pub alerts: Vec<Alert>,
    pub warnings: Vec<String>,
}

/// List names only: never expose kubeconfig users, token or certificate fields.
pub async fn list_contexts() -> Result<Contexts, String> {
    let raw = run_kubectl_with("kubectl", "k3s", None,
        &["config", "get-contexts", "-o", "name"]).await?;
    let names: Vec<String> = String::from_utf8(raw)
        .map_err(|_| "kubectl returned invalid context names.".to_string())?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .take(1000)
        .map(str::to_string)
        .collect();
    let mut warnings = Vec::new();
    let current = match selected_context("kubectl", "k3s", None).await {
        Ok(name) => Some(name),
        Err(err) => {
            warnings.push(err);
            None
        }
    };
    Ok(Contexts { names, current, warnings })
}

pub fn demo_contexts() -> Contexts {
    Contexts { current: Some("demo".into()), names: vec!["demo".into()], warnings: Vec::new() }
}

/// Fetch each inventory family concurrently, but only after resolving the
/// selected context once: context changes cannot mix two clusters in one view.
pub async fn diagnose(context: Option<&str>, namespace: Option<&str>) -> Result<Diagnostics, String> {
    let chosen = selected_context("kubectl", "k3s", context).await?;
    let (status, workloads, events) = tokio::join!(
        inspect(Some(&chosen), namespace),
        inspect_workloads(Some(&chosen), namespace),
        inspect_events(Some(&chosen), namespace, 100),
    );
    let status = status?;
    Ok(assess(status, workloads, events))
}

pub fn demo_diagnostics(namespace: Option<&str>) -> Result<Diagnostics, String> {
    let status = super::demo_status(None, namespace)?;
    let workloads = super::demo_workloads(namespace);
    let events = super::demo_events(namespace, 100);
    Ok(assess(status, workloads, events))
}

fn assess(
    status: ClusterStatus,
    workload_results: Result<Workloads, String>,
    event_results: Result<Events, String>,
) -> Diagnostics {
    let mut alerts = Vec::new();
    let mut warnings = status.warnings.clone();
    let mut partial = status.nodes.is_none() || status.pods.is_none() || !warnings.is_empty();

    if let Some(nodes) = &status.nodes {
        for node in nodes {
            if node.ready != "Ready" {
                alerts.push(Alert { severity: "error".into(), source: "node".into(),
                    namespace: None, name: node.name.clone(),
                    detail: format!("Node readiness is {}.", node.ready) });
            }
            if !node.schedulable {
                alerts.push(Alert { severity: "info".into(), source: "node".into(),
                    namespace: None, name: node.name.clone(),
                    detail: "Node is cordoned; the scheduler will not place new Pods on it.".into() });
            }
            for pressure in &node.pressures {
                alerts.push(Alert { severity: "warning".into(), source: "node".into(),
                    namespace: None, name: node.name.clone(),
                    detail: format!("{pressure} is active.") });
            }
        }
    }

    if let Some(pods) = &status.pods {
        for pod in pods {
            let failure = matches!(pod.phase.as_str(), "Failed" | "Unknown")
                || matches!(pod.reason.as_deref(),
                    Some("CrashLoopBackOff" | "ImagePullBackOff" | "ErrImagePull" |
                         "CreateContainerConfigError" | "OOMKilled" | "Evicted"));
            let stalled = pod.phase == "Pending" || pod.reason.as_deref() == Some("Unschedulable");
            if failure || stalled {
                alerts.push(Alert { severity: if failure { "error" } else { "warning" }.into(),
                    source: "pod".into(), namespace: Some(pod.namespace.clone()),
                    name: pod.name.clone(),
                    detail: format!("{}{}", pod.phase,
                        pod.reason.as_ref().map(|r| format!(" ({r})")).unwrap_or_default()) });
            }
        }
    }

    match workload_results {
        Ok(workloads) => {
            if !workloads.warnings.is_empty() { partial = true; }
            warnings.extend(workloads.warnings);
            for item in workloads.items {
                if item.kind == "Job" {
                    if item.failed.unwrap_or(0) > 0 {
                        alerts.push(Alert { severity: "warning".into(), source: "Job".into(),
                            namespace: Some(item.namespace), name: item.name,
                            detail: format!("{} failed pod attempts.", item.failed.unwrap_or(0)) });
                    }
                } else if item.ready < item.desired && !item.paused {
                    alerts.push(Alert { severity: "warning".into(), source: item.kind,
                        namespace: Some(item.namespace), name: item.name,
                        detail: format!("{} of {} replicas ready.", item.ready, item.desired) });
                }
            }
        }
        Err(err) => {
            partial = true;
            warnings.push(format!("Workloads: {err}"));
        }
    }

    match event_results {
        Ok(events) => for event in events.events.into_iter().filter(|e| e.event_type == "Warning").take(30) {
            alerts.push(Alert { severity: "warning".into(), source: "event".into(),
                namespace: Some(event.namespace), name: event.object,
                detail: format!("{}: {}", event.reason, event.message) });
        },
        Err(err) => {
            partial = true;
            warnings.push(format!("Events: {err}"));
        }
    }
    // Stable severity-first ordering with a bounded result for tools/UI.
    let rank = |severity: &str| match severity { "error" => 0, "warning" => 1, _ => 2 };
    alerts.sort_by_key(|alert| rank(&alert.severity));
    alerts.truncate(100);
    let errors = alerts.iter().filter(|a| a.severity == "error").count();
    let warning_count = alerts.iter().filter(|a| a.severity == "warning").count();
    let state = if partial { "partial" } else if errors > 0 { "error" } else if warning_count > 0 { "warning" } else { "ok" };
    Diagnostics {
        context: status.context,
        namespace: status.namespace,
        state: state.into(),
        summary: format!("{} errors, {} warnings, {} coverage gaps", errors, warning_count, warnings.len()),
        alerts,
        warnings,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_rbac_cannot_be_healthy() {
        let mut status = super::super::demo_status(None, None).unwrap();
        status.nodes = None;
        status.warnings.push("nodes forbidden".into());
        let result = assess(status, super::super::demo_workloads(None), super::super::demo_events(None, 100));
        assert_eq!(result.state, "partial");
        assert!(result.warnings.iter().any(|s| s.contains("nodes")));
    }

    #[test]
    fn failing_node_and_pod_are_errors() {
        let mut status = super::super::demo_status(None, None).unwrap();
        status.nodes.as_mut().unwrap()[0].ready = "NotReady".into();
        status.pods.as_mut().unwrap()[0].reason = Some("CrashLoopBackOff".into());
        let result = assess(status, super::super::demo_workloads(None), super::super::demo_events(None, 100));
        assert_eq!(result.state, "error");
        assert!(result.alerts.iter().filter(|a| a.severity == "error").count() >= 2);
    }
}
