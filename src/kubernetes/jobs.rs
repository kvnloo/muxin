//! Guarded, namespace-scoped Kubernetes Jobs for bounded fleet tasks.
//! Creation uses a generated manifest on stdin, never a shell command.
//! This is not a persistent agent workspace: Jobs have finite deadlines.

use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::{kubectl_json, run_kubectl_with, selected_context, valid_namespace};

fn default_cpu() -> String { "500m".into() }
fn default_memory() -> String { "1Gi".into() }
fn default_deadline() -> u32 { 3600 }

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobRequest {
    /// Must be selected explicitly for every mutating operation.
    pub context: String,
    /// Required namespace, never implicitly "default" or all namespaces.
    pub namespace: String,
    pub name: String,
    pub image: String,
    /// OCI container argv; it is never interpolated into a host shell.
    pub command: Vec<String>,
    #[serde(default = "default_cpu")]
    pub cpu: String,
    #[serde(default = "default_memory")]
    pub memory: String,
    /// Optional NVIDIA GPU request; no host mounts or device privileges.
    pub gpu: u32,
    #[serde(default = "default_deadline")]
    pub deadline_seconds: u32,
    /// Only true when the user explicitly approves creation.
    pub confirm: bool,
}

impl Default for JobRequest {
    fn default() -> Self {
        Self { context: String::new(), namespace: String::new(), name: String::new(),
            image: String::new(), command: Vec::new(), cpu: default_cpu(),
            memory: default_memory(), gpu: 0, deadline_seconds: default_deadline(), confirm: false }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobReference {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub confirm: bool,
}

impl Default for JobReference {
    fn default() -> Self {
        Self { context: String::new(), namespace: String::new(), name: String::new(), confirm: false }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JobPlan {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub manifest: Value,
    /// The plan is not proof that the Job exists or can be scheduled.
    pub next: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobAction {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub action: String,
    pub details: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobStatus {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub active: u32,
    pub succeeded: u32,
    pub failed: u32,
    pub conditions: Vec<String>,
    pub managed_by_muxin: bool,
}

pub fn plan_job(request: &JobRequest) -> Result<JobPlan, String> {
    validate_ref(&request.context, &request.namespace, &request.name)?;
    if request.image.trim().is_empty() || request.image.len() > 256
        || !request.image.chars().all(|c| c.is_ascii_alphanumeric() || "-._/@:".contains(c))
        || request.image.starts_with('-')
    {
        return Err("Image must be a valid nonempty OCI reference (up to 256 characters).".into());
    }
    if request.command.is_empty() || request.command.len() > 32
        || request.command.iter().any(|s| s.is_empty() || s.len() > 4096 || s.contains('\0'))
        || request.command.iter().map(String::len).sum::<usize>() > 8192
    {
        return Err("Job command needs 1..32 nonempty arguments, at most 8192 bytes total.".into());
    }
    if !valid_cpu(&request.cpu) || !valid_memory(&request.memory) {
        return Err("CPU must be 1..128 cores or 1..32000m; memory 1..128Gi or 1..131072Mi.".into());
    }
    if request.gpu > 8 {
        return Err("GPU request must be between 0 and 8.".into());
    }
    if !(30..=86400).contains(&request.deadline_seconds) {
        return Err("Job deadline must be between 30 and 86400 seconds.".into());
    }
    let mut resources = json!({
        "requests": { "cpu": request.cpu, "memory": request.memory },
        "limits": { "cpu": request.cpu, "memory": request.memory }
    });
    if request.gpu > 0 {
        resources["requests"]["nvidia.com/gpu"] = json!(request.gpu);
        resources["limits"]["nvidia.com/gpu"] = json!(request.gpu);
    }
    let manifest = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": request.name,
            "namespace": request.namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "muxin",
                "app.kubernetes.io/component": "agent-job",
            }
        },
        "spec": {
            "parallelism": 1,
            "completions": 1,
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 3600,
            "activeDeadlineSeconds": request.deadline_seconds,
            "template": {
                "metadata": {
                    "labels": { "app.kubernetes.io/managed-by": "muxin",
                                "app.kubernetes.io/component": "agent-job" }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": false,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [{
                        "name": "agent",
                        "image": request.image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": request.command,
                        "workingDir": "/workspace",
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "capabilities": { "drop": ["ALL"] },
                        },
                        "resources": resources,
                        "volumeMounts": [{ "name": "workspace", "mountPath": "/workspace" }]
                    }],
                    "volumes": [{ "name": "workspace", "emptyDir": {} }]
                }
            }
        }
    });
    Ok(JobPlan {
        context: request.context.clone(),
        namespace: request.namespace.clone(),
        name: request.name.clone(),
        manifest,
        next: "Preview the manifest, then create with confirm=true (CLI --yes). This uses temporary storage; results must be exported before TTL cleanup.".into(),
    })
}

pub async fn create_job(request: &JobRequest, demo: bool) -> Result<JobAction, String> {
    let plan = plan_job(request)?;
    if !request.confirm {
        return Err("Creating a cluster Job requires explicit confirm=true (CLI --yes).".into());
    }
    if demo {
        return Ok(JobAction { context: "demo".into(), namespace: plan.namespace, name: plan.name,
            action: "demo:create".into(), details: "No real cluster or kubeconfig accessed.".into() });
    }
    require_explicit_kubeconfig()?;
    let context = selected_context("kubectl", "k3s", Some(&plan.context)).await?;
    let bytes = serde_json::to_vec(&plan.manifest).map_err(|e| e.to_string())?;
    let response = kubectl_with_input(&context, &["create", "-f", "-"], &bytes).await?;
    Ok(JobAction {
        context,
        namespace: plan.namespace,
        name: plan.name,
        action: "created".into(),
        details: String::from_utf8_lossy(&response).trim().into(),
    })
}

pub async fn job_status(reference: &JobReference, demo: bool) -> Result<JobStatus, String> {
    validate_ref(&reference.context, &reference.namespace, &reference.name)?;
    if demo {
        return Ok(JobStatus { context: "demo".into(), namespace: reference.namespace.clone(),
            name: reference.name.clone(), active: 1, succeeded: 0, failed: 0, conditions: vec![],
            managed_by_muxin: true });
    }
    let context = selected_context("kubectl", "k3s", Some(&reference.context)).await?;
    let raw = read_job(&context, &reference.namespace, &reference.name).await?;
    Ok(status_from(&context, &reference.namespace, &reference.name, &raw))
}

pub async fn delete_job(reference: &JobReference, demo: bool) -> Result<JobAction, String> {
    validate_ref(&reference.context, &reference.namespace, &reference.name)?;
    if !reference.confirm {
        return Err("Deleting a cluster Job requires explicit confirm=true (CLI --yes).".into());
    }
    if demo {
        return Ok(JobAction { context: "demo".into(), namespace: reference.namespace.clone(),
            name: reference.name.clone(), action: "demo:delete".into(),
            details: "No real cluster or kubeconfig accessed.".into() });
    }
    require_explicit_kubeconfig()?;
    let context = selected_context("kubectl", "k3s", Some(&reference.context)).await?;
    let raw = read_job(&context, &reference.namespace, &reference.name).await?;
    if !is_owned_job(&raw) {
        return Err("Refusing to delete a Job not labeled as managed by Muxin.".into());
    }
    let bytes = run_kubectl_with("kubectl", "k3s", Some(&context),
        &["delete", "job", &reference.name, "--namespace", &reference.namespace, "--wait=false"]).await?;
    Ok(JobAction { context, namespace: reference.namespace.clone(), name: reference.name.clone(),
        action: "deleted".into(), details: String::from_utf8_lossy(&bytes).trim().into() })
}

fn is_owned_job(raw: &Value) -> bool {
    raw["kind"] == "Job"
        && raw["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "muxin"
        && raw["metadata"]["labels"]["app.kubernetes.io/component"] == "agent-job"
}

async fn read_job(context: &str, namespace: &str, name: &str) -> Result<Value, String> {
    kubectl_json("kubectl", "k3s", context,
        &["get", "job", name, "-o", "json", "--namespace", namespace]).await
}

fn status_from(context: &str, namespace: &str, name: &str, raw: &Value) -> JobStatus {
    JobStatus {
        context: context.into(),
        namespace: namespace.into(),
        name: name.into(),
        active: raw["status"]["active"].as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        succeeded: raw["status"]["succeeded"].as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        failed: raw["status"]["failed"].as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        conditions: raw["status"]["conditions"].as_array().map(|items|
            items.iter().filter_map(|x| {
                if x["status"] == "True" { x["type"].as_str().map(str::to_string) } else { None }
            }).collect()
        ).unwrap_or_default(),
        managed_by_muxin: is_owned_job(raw),
    }
}

pub(super) fn validate_ref(context: &str, namespace: &str, name: &str) -> Result<(), String> {
    if context.is_empty() || context.len() > 253 || context.starts_with('-') || context.chars().any(char::is_control) {
        return Err("Explicit Kubernetes context is required (1..253 valid characters).".into());
    }
    if !valid_namespace(namespace) || !valid_namespace(name) {
        return Err("Explicit namespace and Job name must be valid Kubernetes DNS labels.".into());
    }
    Ok(())
}

pub(super) fn valid_cpu(v: &str) -> bool {
    match v.strip_suffix('m') {
        Some(m) => m.parse::<u32>().is_ok_and(|n| (1..=32000).contains(&n)),
        None => v.parse::<u32>().is_ok_and(|n| (1..=128).contains(&n)),
    }
}

pub(super) fn valid_memory(v: &str) -> bool {
    if let Some(n) = v.strip_suffix("Mi") {
        n.parse::<u32>().is_ok_and(|n| (1..=131072).contains(&n))
    } else if let Some(n) = v.strip_suffix("Gi") {
        n.parse::<u32>().is_ok_and(|n| (1..=128).contains(&n))
    } else {
        false
    }
}

/// A mutating command never discovers the K3s default cluster-admin config.
/// Explicit kubeconfig + context narrow accidental writes; Kubernetes RBAC is
/// still the real authorization boundary.
pub(super) fn require_explicit_kubeconfig() -> Result<(), String> {
    match std::env::var_os("KUBECONFIG") {
        Some(path) if !path.is_empty() => Ok(()),
        _ => Err("Set KUBECONFIG to a deliberately selected, least-privilege kubeconfig before changing cluster resources.".into()),
    }
}

enum ExecFailure {
    Missing,
    Failed(String),
}

pub(super) async fn kubectl_with_input(context: &str, argv: &[&str], manifest: &[u8]) -> Result<Vec<u8>, String> {
    if manifest.len() > 64 * 1024 {
        return Err("Refusing to submit a manifest larger than 64 KiB.".into());
    }
    match run_with_input("kubectl", false, context, argv, manifest).await {
        Ok(output) => Ok(output),
        Err(ExecFailure::Missing) => match run_with_input("k3s", true, context, argv, manifest).await {
            Ok(output) => Ok(output),
            Err(ExecFailure::Missing) => Err("Neither kubectl nor k3s is installed.".into()),
            Err(ExecFailure::Failed(err)) => Err(err),
        },
        Err(ExecFailure::Failed(err)) => Err(err),
    }
}

async fn run_with_input(binary: &str, embedded: bool, context: &str, argv: &[&str], body: &[u8]) -> Result<Vec<u8>, ExecFailure> {
    let mut process = Command::new(binary);
    if embedded {
        process.arg("kubectl");
    }
    process.arg("--context").arg(context).arg("--request-timeout=10s").args(argv)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let mut child = process.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound { ExecFailure::Missing }
        else { ExecFailure::Failed(format!("Couldn't start {binary}: {e}")) }
    })?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(body).await.map_err(|e| ExecFailure::Failed(format!("Couldn't send manifest to {binary}: {e}")))?;
    }
    let out = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .map_err(|_| ExecFailure::Failed(format!("{binary} did not finish in 30 seconds.")))?
        .map_err(|e| ExecFailure::Failed(format!("{binary} failed: {e}")))?;
    if !out.status.success() {
        let detail: String = String::from_utf8_lossy(&out.stderr).trim().chars().take(400).collect();
        return Err(ExecFailure::Failed(if detail.is_empty() {
            format!("{binary} returned {}", out.status)
        } else {
            detail
        }));
    }
    if out.stdout.len() > 1024 * 1024 {
        return Err(ExecFailure::Failed("kubectl output exceeded 1 MiB.".into()));
    }
    Ok(out.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_job_has_safe_defaults_and_gpu_limit() {
        let request = JobRequest {
            context: "home-k3s".into(), namespace: "agents".into(), name: "eval-1".into(),
            image: "python:3.13".into(), command: vec!["python".into(), "-V".into()],
            gpu: 1, ..Default::default()
        };
        let plan = plan_job(&request).unwrap();
        let pod = &plan.manifest["spec"]["template"]["spec"];
        assert_eq!(pod["automountServiceAccountToken"], false);
        assert_eq!(pod["containers"][0]["resources"]["limits"]["nvidia.com/gpu"], 1);
        assert_eq!(pod["securityContext"]["runAsNonRoot"], true);
        assert_eq!(plan.manifest["metadata"]["namespace"], "agents");
    }

    #[test]
    fn validation_rejects_unbounded_jobs_or_namespace_flags() {
        let valid = JobRequest { context: "lab".into(), namespace: "agents".into(),
            name: "task-1".into(), image: "alpine:3.20".into(), command: vec!["echo".into(), "ok".into()],
            ..Default::default() };
        assert!(plan_job(&valid).is_ok());
        let mut invalid = valid.clone();
        invalid.namespace = "--all-namespaces".into();
        assert!(plan_job(&invalid).is_err());
        invalid = valid.clone();
        invalid.gpu = 100;
        assert!(plan_job(&invalid).is_err());
        invalid = valid.clone();
        invalid.deadline_seconds = 0;
        assert!(plan_job(&invalid).is_err());
    }

    #[test]
    fn foreign_jobs_cannot_be_deleted() {
        let foreign = json!({ "kind": "Job", "metadata": { "labels": { "app": "unrelated" } } });
        assert!(!is_owned_job(&foreign));
        let ours = json!({ "kind": "Job", "metadata": { "labels": {
            "app.kubernetes.io/managed-by": "muxin",
            "app.kubernetes.io/component": "agent-job"
        }}});
        assert!(is_owned_job(&ours));
    }
}
