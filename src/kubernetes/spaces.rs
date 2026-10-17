//! PVC-backed, reschedulable Kubernetes workspaces.
//!
//! This is an opt-in cluster backend, deliberately separate from SSH machine
//! workspaces and the local workspaces.json registry. Cluster Deployments are
//! the source of truth. A pre-existing PVC owns all persistent files.

use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use super::{kubectl_json, run_kubectl_with, selected_context, valid_namespace};
use super::jobs::{kubectl_with_input, require_explicit_kubeconfig, valid_cpu, valid_memory, validate_ref};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpaceRequest {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub image: String,
    /// A PVC the user already created; never created or deleted by Muxin.
    pub claim: String,
    pub cpu: String,
    pub memory: String,
    pub gpu: u32,
    pub confirm: bool,
}

impl Default for SpaceRequest {
    fn default() -> Self {
        Self { context: String::new(), namespace: String::new(), name: String::new(),
            image: String::new(), claim: String::new(), cpu: "500m".into(),
            memory: "1Gi".into(), gpu: 0, confirm: false }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpaceRef {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub confirm: bool,
}

impl Default for SpaceRef {
    fn default() -> Self {
        Self { context: String::new(), namespace: String::new(), name: String::new(), confirm: false }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpaceExec {
    pub context: String,
    pub namespace: String,
    pub name: String,
    /// Container arguments after kubectl exec ... --
    pub command: Vec<String>,
    pub confirm: bool,
    /// Maximum wait, 1..300 seconds, default 60.
    pub timeout_seconds: u32,
}

impl Default for SpaceExec {
    fn default() -> Self {
        Self { context: String::new(), namespace: String::new(), name: String::new(),
            command: Vec::new(), confirm: false, timeout_seconds: 60 }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SpacePlan {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub claim: String,
    pub manifest: Value,
    pub next: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpaceInfo {
    pub name: String,
    pub namespace: String,
    pub claim: String,
    pub ready: u32,
    pub desired: u32,
    pub pod: Option<String>,
    pub node: Option<String>,
    pub phase: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpaceList {
    pub context: String,
    pub namespace: String,
    pub workspaces: Vec<SpaceInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpaceAction {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub action: String,
    pub details: String,
    /// Space deletion never deletes the PVC or its files.
    pub claim_preserved: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpaceOutput {
    pub context: String,
    pub namespace: String,
    pub name: String,
    pub pod: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

pub fn plan_space(req: &SpaceRequest) -> Result<SpacePlan, String> {
    validate_ref(&req.context, &req.namespace, &req.name)?;
    if !valid_namespace(&req.claim) {
        return Err("Workspace needs an existing PVC named by a valid DNS label.".into());
    }
    if req.image.is_empty() || req.image.len() > 256 || req.image.starts_with('-')
        || !req.image.chars().all(|c| c.is_ascii_alphanumeric() || "-._/@:".contains(c))
    {
        return Err("Workspace image must be a nonempty OCI reference without whitespace.".into());
    }
    if !valid_cpu(&req.cpu) || !valid_memory(&req.memory) || req.gpu > 8 {
        return Err("Invalid CPU, memory or GPU request (max 8 GPUs).".into());
    }
    let mut resources = json!({
        "requests": { "cpu": req.cpu, "memory": req.memory },
        "limits": { "cpu": req.cpu, "memory": req.memory }
    });
    if req.gpu > 0 {
        resources["requests"]["nvidia.com/gpu"] = json!(req.gpu);
        resources["limits"]["nvidia.com/gpu"] = json!(req.gpu);
    }
    let match_labels = json!({
        "app.kubernetes.io/name": "muxin-workspace",
        "app.kubernetes.io/instance": req.name,
    });
    let manifest = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "namespace": req.namespace,
            "name": req.name,
            "labels": {
                "app.kubernetes.io/managed-by": "muxin",
                "app.kubernetes.io/component": "agent-workspace",
                "app.kubernetes.io/instance": req.name,
            }
        },
        "spec": {
            "replicas": 1,
            "strategy": { "type": "Recreate" },
            "selector": { "matchLabels": match_labels },
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "muxin-workspace",
                        "app.kubernetes.io/instance": req.name,
                        "app.kubernetes.io/managed-by": "muxin",
                        "app.kubernetes.io/component": "agent-workspace",
                    }
                },
                "spec": {
                    "automountServiceAccountToken": false,
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [{
                        "name": "agent",
                        "image": req.image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["/bin/sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 3600; done"],
                        "workingDir": "/workspace",
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "capabilities": { "drop": ["ALL"] },
                        },
                        "resources": resources,
                        "volumeMounts": [{ "name": "workspace", "mountPath": "/workspace" }]
                    }],
                    "volumes": [{
                        "name": "workspace",
                        "persistentVolumeClaim": { "claimName": req.claim }
                    }]
                }
            }
        }
    });
    Ok(SpacePlan {
        context: req.context.clone(), namespace: req.namespace.clone(),
        name: req.name.clone(), claim: req.claim.clone(), manifest,
        next: "Provide an existing PVC; after review, use confirm=true (--yes). Muxin creates a Deployment, never the PVC. Image must include /bin/sh and sleep and run as UID 1000.".into()
    })
}

pub async fn create_space(req: &SpaceRequest, demo: bool) -> Result<SpaceAction, String> {
    let plan = plan_space(req)?;
    if !req.confirm {
        return Err("Creating a K3s workspace requires explicit confirm=true (--yes).".into());
    }
    if demo {
        return Ok(SpaceAction { context: "demo".into(), namespace: plan.namespace,
            name: plan.name, action: "demo:create".into(),
            details: "No cluster accessed.".into(), claim_preserved: true });
    }
    require_explicit_kubeconfig()?;
    let context = selected_context("kubectl", "k3s", Some(&plan.context)).await?;
    // Fail before deployment if the operator's PVC is absent or terminating.
    // A Pending claim can be valid with WaitForFirstConsumer storage classes.
    let pvc = kubectl_json("kubectl", "k3s", &context,
        &["get", "pvc", &req.claim, "-o", "json", "--namespace", &req.namespace]).await?;
    if pvc["kind"] != "PersistentVolumeClaim"
        || pvc["metadata"]["name"] != req.claim
        || !pvc["metadata"]["deletionTimestamp"].is_null()
    {
        return Err("The referenced PVC is missing, wrong, or being deleted.".into());
    }
    let bytes = serde_json::to_vec(&plan.manifest).map_err(|e| e.to_string())?;
    let stdout = kubectl_with_input(&context, &["create", "-f", "-"], &bytes).await?;
    Ok(SpaceAction { context, namespace: plan.namespace, name: plan.name,
        action: "created".into(), details: String::from_utf8_lossy(&stdout).trim().into(),
        claim_preserved: true })
}

pub async fn list_spaces(context: &str, namespace: &str, demo: bool) -> Result<SpaceList, String> {
    validate_scope(context, namespace)?;
    if demo {
        return Ok(SpaceList {
            context: "demo".into(), namespace: namespace.into(),
            workspaces: vec![SpaceInfo {
                name: "coder".into(), namespace: namespace.into(), claim: "coder-data".into(),
                ready: 1, desired: 1, pod: Some("coder-abc".into()),
                node: Some("worker-1".into()), phase: Some("Running".into())
            }]
        });
    }
    let context = selected_context("kubectl", "k3s", Some(context)).await?;
    let raw = kubectl_json("kubectl", "k3s", &context,
        &["get", "deployments", "--namespace", namespace,
          "-l", "app.kubernetes.io/managed-by=muxin,app.kubernetes.io/component=agent-workspace",
          "-o", "json"]).await?;
    let deployments = raw["items"].as_array().ok_or("Deployment list is missing items.")?;
    let mut out = Vec::with_capacity(deployments.len());
    for deployment in deployments {
        if is_owned_space(deployment) {
            out.push(info_from(deployment, None));
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(SpaceList { context, namespace: namespace.into(), workspaces: out })
}

pub async fn space_status(reference: &SpaceRef, demo: bool) -> Result<SpaceInfo, String> {
    validate_scope(&reference.context, &reference.namespace)?;
    if !valid_namespace(&reference.name) {
        return Err("Invalid workspace name.".into());
    }
    if demo {
        return Ok(SpaceInfo { name: reference.name.clone(), namespace: reference.namespace.clone(),
            claim: "coder-data".into(), ready: 1, desired: 1, pod: Some("coder-abc".into()),
            node: Some("worker-1".into()), phase: Some("Running".into()) });
    }
    let context = selected_context("kubectl", "k3s", Some(&reference.context)).await?;
    let deployment = read_space(&context, &reference.namespace, &reference.name).await?;
    let pod = find_ready_pod(&context, &reference.namespace, &reference.name).await.ok();
    Ok(info_from(&deployment, pod.as_ref()))
}

pub async fn delete_space(reference: &SpaceRef, demo: bool) -> Result<SpaceAction, String> {
    validate_ref(&reference.context, &reference.namespace, &reference.name)?;
    if !reference.confirm {
        return Err("Deleting a K3s workspace requires explicit confirm=true (--yes).".into());
    }
    if demo {
        return Ok(SpaceAction { context: "demo".into(), namespace: reference.namespace.clone(),
            name: reference.name.clone(), action: "demo:delete".into(),
            details: "No cluster accessed. PVC preserved.".into(), claim_preserved: true });
    }
    require_explicit_kubeconfig()?;
    let context = selected_context("kubectl", "k3s", Some(&reference.context)).await?;
    let _deployment = read_space(&context, &reference.namespace, &reference.name).await?;
    let stdout = run_kubectl_with("kubectl", "k3s", Some(&context),
        &["delete", "deployment", &reference.name, "--namespace", &reference.namespace,
          "--wait=false"]).await?;
    Ok(SpaceAction { context, namespace: reference.namespace.clone(),
        name: reference.name.clone(), action: "deleted".into(),
        details: format!("{} (PVC and its data not deleted)", String::from_utf8_lossy(&stdout).trim()),
        claim_preserved: true })
}

pub async fn execute_space(req: &SpaceExec, demo: bool) -> Result<SpaceOutput, String> {
    validate_ref(&req.context, &req.namespace, &req.name)?;
    if !req.confirm {
        return Err("Executing a command in a K3s workspace requires confirm=true (--yes).".into());
    }
    if req.command.is_empty() || req.command.len() > 32 ||
        req.command.iter().any(|part| part.is_empty() || part.len() > 8192 || part.contains('\0')) ||
        req.command.iter().map(String::len).sum::<usize>() > 16384
    {
        return Err("Provide 1..32 executable argv strings, at most 16384 bytes total.".into());
    }
    if req.timeout_seconds == 0 || req.timeout_seconds > 300 {
        return Err("Workspace execution timeout must be 1..300 seconds.".into());
    }
    if demo {
        return Ok(SpaceOutput { context: "demo".into(), namespace: req.namespace.clone(),
            name: req.name.clone(), pod: "coder-abc".into(), exit_code: Some(0),
            stdout: "Example output (demo only).\n".into(), stderr: String::new(), truncated: false });
    }
    require_explicit_kubeconfig()?;
    let context = selected_context("kubectl", "k3s", Some(&req.context)).await?;
    let deployment = read_space(&context, &req.namespace, &req.name).await?;
    if !is_owned_space(&deployment) {
        return Err("Only Muxin-managed deployments are accessible as workspaces.".into());
    }
    let pod = find_ready_pod(&context, &req.namespace, &req.name).await?;
    let mut args = vec!["exec".into(), pod.name.clone(), "--namespace".into(),
        req.namespace.clone(), "--container".into(), "agent".into(), "--".into()];
    args.extend(req.command.iter().cloned());
    let outcome = kubectl_exec(&context, &args, req.timeout_seconds).await?;
    Ok(SpaceOutput { context, namespace: req.namespace.clone(), name: req.name.clone(),
        pod: pod.name, exit_code: outcome.exit_code, stdout: outcome.stdout,
        stderr: outcome.stderr, truncated: outcome.truncated })
}

fn validate_scope(context: &str, namespace: &str) -> Result<(), String> {
    validate_ref(context, namespace, "valid-name")
}

fn is_owned_space(raw: &Value) -> bool {
    raw["kind"] == "Deployment"
        && raw["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "muxin"
        && raw["metadata"]["labels"]["app.kubernetes.io/component"] == "agent-workspace"
}

async fn read_space(context: &str, namespace: &str, name: &str) -> Result<Value, String> {
    let deployment = kubectl_json("kubectl", "k3s", context,
        &["get", "deployment", name, "--namespace", namespace, "-o", "json"]).await?;
    if !is_owned_space(&deployment) {
        return Err("This Deployment is not labeled as a Muxin workspace.".into());
    }
    Ok(deployment)
}

#[derive(Debug)]
struct PodTarget {
    name: String,
    node: Option<String>,
    phase: String,
}

async fn find_ready_pod(context: &str, namespace: &str, name: &str) -> Result<PodTarget, String> {
    let selector = format!("app.kubernetes.io/name=muxin-workspace,app.kubernetes.io/instance={name}");
    let raw = kubectl_json("kubectl", "k3s", context,
        &["get", "pods", "--namespace", namespace, "-l", &selector, "-o", "json"]).await?;
    let pods = raw["items"].as_array().ok_or("Pod list is missing items.")?;
    let pod = pods.iter().find(|item| {
        item["status"]["phase"] == "Running"
            && item["metadata"]["deletionTimestamp"].is_null()
            && item["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "muxin"
            && item["metadata"]["labels"]["app.kubernetes.io/component"] == "agent-workspace"
            && item["status"]["containerStatuses"].as_array().is_some_and(|rows|
                rows.iter().any(|row| row["name"] == "agent" && row["ready"] == true))
    }).ok_or("No Ready Pod for this Muxin workspace yet; inspect status and events.")?;
    Ok(PodTarget { name: pod["metadata"]["name"].as_str().ok_or("Pod missing name.")?.into(),
        node: pod["spec"]["nodeName"].as_str().map(str::to_string),
        phase: "Running".into() })
}

fn info_from(deployment: &Value, pod: Option<&PodTarget>) -> SpaceInfo {
    let container = &deployment["spec"]["template"]["spec"]["containers"][0];
    let claim = deployment["spec"]["template"]["spec"]["volumes"]
        .as_array().and_then(|v| v.iter().find_map(|v|
            v["persistentVolumeClaim"]["claimName"].as_str())).unwrap_or("unknown");
    let name = deployment["metadata"]["name"].as_str().unwrap_or("unknown");
    SpaceInfo {
        name: name.into(),
        namespace: deployment["metadata"]["namespace"].as_str().unwrap_or("default").into(),
        claim: claim.into(),
        ready: deployment["status"]["readyReplicas"].as_u64().unwrap_or(0).min(u32::MAX as u64) as u32,
        desired: deployment["spec"]["replicas"].as_u64().unwrap_or(1).min(u32::MAX as u64) as u32,
        pod: pod.map(|p| p.name.clone()),
        node: pod.and_then(|p| p.node.clone()),
        phase: pod.map(|p| p.phase.clone()),
    }
}

struct ExecReply {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    truncated: bool,
}

async fn kubectl_exec(context: &str, argv: &[String], seconds: u32) -> Result<ExecReply, String> {
    match exec_with("kubectl", false, context, argv, seconds).await {
        Err(ExecError::Missing) => match exec_with("k3s", true, context, argv, seconds).await {
            Err(ExecError::Missing) => Err("Neither kubectl nor k3s was found.".into()),
            Err(ExecError::Failed(err)) => Err(err),
            Ok(result) => Ok(result),
        },
        Err(ExecError::Failed(err)) => Err(err),
        Ok(result) => Ok(result),
    }
}

enum ExecError {
    Missing,
    Failed(String),
}

/// Drain both streams even after reaching the byte cap. Otherwise a verbose
/// process can deadlock on its stdout pipe while Muxin waits for exit.
async fn read_capped(mut stream: impl tokio::io::AsyncRead + Unpin, cap: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 { break; }
        let kept = n.min(cap.saturating_sub(output.len()));
        output.extend_from_slice(&buffer[..kept]);
        if kept < n { truncated = true; }
    }
    Ok((output, truncated))
}

async fn exec_with(binary: &str, embedded: bool, context: &str, argv: &[String], seconds: u32) -> Result<ExecReply, ExecError> {
    let mut command = Command::new(binary);
    if embedded { command.arg("kubectl"); }
    command.arg("--context").arg(context).arg("--request-timeout=10s").args(argv)
        .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    let mut child = command.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound { ExecError::Missing }
        else { ExecError::Failed(format!("Could not execute {binary}: {e}")) }
    })?;
    let stdout = child.stdout.take().ok_or_else(|| ExecError::Failed("Missing stdout.".into()))?;
    let stderr = child.stderr.take().ok_or_else(|| ExecError::Failed("Missing stderr.".into()))?;
    let (status, stdout, stderr) = tokio::time::timeout(Duration::from_secs(seconds as u64 + 5), async {
        tokio::try_join!(child.wait(), read_capped(stdout, 512 * 1024), read_capped(stderr, 128 * 1024))
    }).await.map_err(|_| ExecError::Failed("Workspace command timed out; the remote process may still run.".into()))?
        .map_err(|e| ExecError::Failed(format!("Could not read workspace command: {e}")))?;
    Ok(ExecReply {
        exit_code: status.code(),
        stdout: String::from_utf8_lossy(&stdout.0).into(),
        stderr: String::from_utf8_lossy(&stderr.0).into(),
        truncated: stdout.1 || stderr.1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_workspace_manifest_reuses_claim_and_recreates_pods() {
        let req = SpaceRequest { context: "lab".into(), namespace: "agents".into(),
            name: "coder".into(), claim: "coder-data".into(), image: "python:3.13".into(), ..Default::default() };
        let plan = plan_space(&req).unwrap();
        assert_eq!(plan.manifest["kind"], "Deployment");
        assert_eq!(plan.manifest["spec"]["strategy"]["type"], "Recreate");
        assert_eq!(plan.manifest["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"], "coder-data");
        assert_eq!(plan.manifest["spec"]["template"]["spec"]["automountServiceAccountToken"], false);
        assert_eq!(plan.manifest["spec"]["replicas"], 1);
    }

    #[test]
    fn rejects_invalid_claims_and_foreign_deployments() {
        let mut req = SpaceRequest { context: "lab".into(), namespace: "agents".into(),
            name: "coder".into(), claim: "bad/claim".into(), image: "python:3.13".into(), ..Default::default() };
        assert!(plan_space(&req).is_err());
        req.claim = "good-claim".into();
        assert!(plan_space(&req).is_ok());
        assert!(!is_owned_space(&json!({ "kind": "Deployment", "metadata": {"labels":{"app":"other"}}})));
    }

    #[test]
    fn bounded_reader_preserves_first_bytes_and_drains() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (bytes, truncated) = rt.block_on(read_capped(&b"abcdefghij"[..], 4)).unwrap();
        assert_eq!(bytes, b"abcd");
        assert!(truncated);
    }
}
