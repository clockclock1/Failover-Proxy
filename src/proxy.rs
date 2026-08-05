use crate::{
    auth,
    config::{
        endpoint_suffix, target_key, target_label, trim_slashes, ApiKeyMode, Config,
        FailoverStrategy, ModelConfig, TargetConfig,
    },
    stats::{dashmap_memory_overhead, now_ms, FailureInfo, LogEntry, LogModelError, RuntimeMemoryUsage, StatsStore},
    AppState,
};
use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
enum ProxyCallError {
    #[error("timeout")]
    Timeout,
    #[error(transparent)]
    Request(#[from] reqwest::Error),
}

const DEFAULT_OUTPUT_TOKEN_RESERVE: usize = 1024;
// A 1M-token request shrunk by 2/3 fits into a tiny context in under 32 rounds.
// The limit is only a loop-safety guard; compression stops earlier when it can no
// longer reduce the request.
const MAX_CONTEXT_COMPRESSION_ATTEMPTS: u32 = 32;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ProxyEndpoint {
    ChatCompletions,
    Responses,
    Completions,
}

impl ProxyEndpoint {
    fn from_path(path: &str) -> Self {
        match endpoint_suffix(path).as_str() {
            "responses" => Self::Responses,
            "completions" => Self::Completions,
            _ => Self::ChatCompletions,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::ChatCompletions => "chat/completions",
            Self::Responses => "responses",
            Self::Completions => "completions",
        }
    }

    fn candidates(self) -> [Self; 3] {
        match self {
            Self::ChatCompletions => [Self::ChatCompletions, Self::Responses, Self::Completions],
            Self::Responses => [Self::Responses, Self::ChatCompletions, Self::Completions],
            Self::Completions => [Self::Completions, Self::ChatCompletions, Self::Responses],
        }
    }
}

struct CompatibleUpstream {
    response: reqwest::Response,
    endpoint: ProxyEndpoint,
}

impl ProxyCallError {
    fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout => true,
            Self::Request(err) => err.is_timeout(),
        }
    }

    fn status(&self) -> u16 {
        match self {
            Self::Timeout => 504,
            Self::Request(_) => 0,
        }
    }
}

#[derive(Clone, Default)]
pub struct ProxyRuntime {
    round_robin: Arc<DashMap<String, AtomicU64>>,
    active_threads: Arc<DashMap<String, ActiveThread>>,
    thread_seq: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ActiveThread {
    pub id: String,
    pub slot: usize,
    pub chain_name: String,
    pub requested_model: String,
    pub target_name: String,
    pub target_model: String,
    pub target_base_url: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub compression_attempt: u32,
    pub max_compression_attempts: u32,
    pub phase: String,
    pub status: String,
    pub started_at: u64,
    pub updated_at: u64,
    pub release_at: u64,
    pub failed_models: Vec<String>,
    pub attempt_errors: Vec<AttemptError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct AttemptError {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

pub struct ProxySlot {
    runtime: ProxyRuntime,
    thread_id: Option<String>,
}

pub struct StreamSlotGuard {
    runtime: ProxyRuntime,
    thread_id: Option<String>,
}

impl ProxyRuntime {
    pub async fn acquire(&self, model: &ModelConfig, requested_model: &str) -> ProxySlot {
        let id = format!(
            "thread-{}",
            self.thread_seq.fetch_add(1, AtomicOrdering::Relaxed) + 1
        );
        let now = now_ms();
        self.active_threads.insert(
            id.clone(),
            ActiveThread {
                id: id.clone(),
                slot: self.active_threads.len() + 1,
                chain_name: model.public_name.clone(),
                requested_model: requested_model.to_string(),
                phase: "starting".to_string(),
                status: "线程已创建，等待尝试目标模型".to_string(),
                started_at: now,
                updated_at: now,
                failed_models: Vec::new(),
                attempt_errors: Vec::new(),
                ..ActiveThread::default()
            },
        );
        tracing::info!(thread_id = %id, chain = %model.public_name, "proxy thread created");
        ProxySlot {
            runtime: self.clone(),
            thread_id: Some(id),
        }
    }

    pub fn snapshot_threads(&self) -> Vec<ActiveThread> {
        let mut threads = self
            .active_threads
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        threads.sort_by_key(|thread| thread.started_at);
        threads
    }

    pub fn update_thread<F>(&self, thread_id: &str, f: F)
    where
        F: FnOnce(&mut ActiveThread),
    {
        if let Some(mut thread) = self.active_threads.get_mut(thread_id) {
            f(&mut thread);
            thread.updated_at = now_ms();
        }
    }

    pub fn append_error(&self, thread_id: &str, error: AttemptError) {
        self.update_thread(thread_id, |thread| {
            thread.attempt_errors.push(error);
            if thread.attempt_errors.len() > 32 {
                let drain_to = thread.attempt_errors.len() - 32;
                thread.attempt_errors.drain(0..drain_to);
            }
        });
    }

    fn select_target_api_key(&self, model: &ModelConfig, target: &TargetConfig) -> Option<String> {
        let keys = target_api_keys(target);
        if keys.is_empty() {
            return None;
        }
        if keys.len() == 1 {
            return keys.into_iter().next();
        }
        match target.api_key_mode {
            ApiKeyMode::RoundRobin => {
                let cursor_key = format!("api-key:{}", target_key(model, target));
                let cursor = self
                    .round_robin
                    .entry(cursor_key)
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, AtomicOrdering::Relaxed) as usize;
                Some(keys[cursor % keys.len()].clone())
            }
            ApiKeyMode::Random => {
                let idx = rand::random::<usize>() % keys.len();
                Some(keys[idx].clone())
            }
            ApiKeyMode::Single => Some(keys[0].clone()),
        }
    }

    pub fn round_robin_memory_usage(&self) -> RuntimeMemoryUsage {
        let mut usage = dashmap_memory_overhead(&self.round_robin);
        for entry in self.round_robin.iter() {
            usage.entries += 1;
            usage.content_bytes += entry.key().capacity();
        }
        usage.finish();
        usage
    }

    pub fn active_thread_memory_usage(&self) -> RuntimeMemoryUsage {
        let mut usage = dashmap_memory_overhead(&self.active_threads);
        for entry in self.active_threads.iter() {
            let thread = entry.value();
            usage.entries += 1;
            usage.content_bytes += entry.key().capacity()
                + thread.id.capacity()
                + thread.chain_name.capacity()
                + thread.requested_model.capacity()
                + thread.target_name.capacity()
                + thread.target_model.capacity()
                + thread.target_base_url.capacity()
                + thread.phase.capacity()
                + thread.status.capacity()
                + thread.failed_models.capacity() * std::mem::size_of::<String>()
                + thread.failed_models.iter().map(String::capacity).sum::<usize>()
                + thread.attempt_errors.capacity() * std::mem::size_of::<AttemptError>();
            for error in &thread.attempt_errors {
                usage.content_bytes += error.target.capacity()
                    + error.message.capacity()
                    + error.detail.as_ref().map(String::capacity).unwrap_or(0);
            }
        }
        usage.finish();
        usage
    }

    pub fn retain_round_robin_models(&self, models: &[ModelConfig]) {
        let valid = models
            .iter()
            .flat_map(|model| {
                std::iter::once(model.public_name.clone()).chain(
                    model
                        .targets
                        .iter()
                        .map(|target| format!("api-key:{}", target_key(model, target))),
                )
            })
            .collect::<HashSet<_>>();
        let stale = self
            .round_robin
            .iter()
            .filter_map(|entry| {
                if valid.contains(entry.key()) {
                    None
                } else {
                    Some(entry.key().clone())
                }
            })
            .collect::<Vec<_>>();
        for key in stale {
            self.round_robin.remove(&key);
        }
    }
}

impl Drop for ProxySlot {
    fn drop(&mut self) {
        if let Some(thread_id) = self.thread_id.take() {
            release_proxy_slot(self.runtime.clone(), thread_id);
        }
    }
}

impl ProxySlot {
    fn into_stream_guard(mut self) -> StreamSlotGuard {
        StreamSlotGuard {
            runtime: self.runtime.clone(),
            thread_id: self.thread_id.take(),
        }
    }
}

impl Drop for StreamSlotGuard {
    fn drop(&mut self) {
        if let Some(thread_id) = self.thread_id.take() {
            release_proxy_slot(self.runtime.clone(), thread_id);
        }
    }
}

fn release_proxy_slot(runtime: ProxyRuntime, thread_id: String) {
    if runtime.active_threads.remove(&thread_id).is_some() {
        tracing::info!(thread_id = %thread_id, "proxy thread removed");
    }
}

pub async fn list_models(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !auth::is_proxy_key(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid proxy API key", None);
    }
    let now = (now_ms() / 1000) as u64;
    let models = state.model_source.runtime_models(&cfg).await;
    Json(json!({
        "object": "list",
        "data": models.into_iter().map(|model| {
            let context_window = model.context_window_tokens;
            json!({
                "id": model.public_name,
                "object": "model",
                "created": now,
                "owned_by": "failover-proxy",
                "context_window": context_window,
                "context_length": context_window,
                "max_input_tokens": context_window
            })
        }).collect::<Vec<_>>()
    }))
    .into_response()
}

pub async fn proxy_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    proxy_completion(state, headers, uri.path().to_string(), body).await
}

async fn proxy_completion(
    state: AppState,
    headers: HeaderMap,
    pathname: String,
    body_bytes: Bytes,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !auth::is_proxy_key(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid proxy API key", None);
    }
    let mut body: Value = match serde_json::from_slice(&body_bytes) {
        Ok(value) => value,
        Err(err) => return send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    };
    if !body.is_object() {
        return send_error(StatusCode::BAD_REQUEST, "Invalid JSON body", None);
    }
    let requested_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(model) = state.model_source.find_model(&cfg, &requested_model).await else {
        return send_error(
            StatusCode::NOT_FOUND,
            &format!("Model '{}' is not configured", requested_model),
            None,
        );
    };
    let requested_context_tokens =
        estimate_json_tokens(&body).saturating_add(requested_output_token_reserve(&body));
    let proxy_context_window = model.context_window_tokens.max(1024);
    if requested_context_tokens > proxy_context_window as usize {
        return send_error(
            StatusCode::BAD_REQUEST,
            &format!(
                "Request needs about {} tokens, exceeding proxy model '{}' context window of {} tokens",
                requested_context_tokens, requested_model, proxy_context_window
            ),
            None,
        );
    }
    let targets = enabled_targets(&state, &cfg, &model).await;
    if targets.is_empty() {
        return send_error(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("Model '{}' has no enabled targets", requested_model),
            None,
        );
    }
    let slot = state.proxy_runtime.acquire(&model, &requested_model).await;
    let thread_id = slot.thread_id.as_deref().unwrap_or_default().to_string();
    let response = proxy_loop(
        &state,
        &headers,
        &pathname,
        &mut body,
        &cfg,
        &model,
        targets,
        &requested_model,
        &thread_id,
        slot,
    )
    .await;
    response
}

async fn proxy_loop(
    state: &AppState,
    headers: &HeaderMap,
    pathname: &str,
    body: &mut Value,
    cfg: &Config,
    model: &ModelConfig,
    targets: Vec<TargetConfig>,
    requested_model: &str,
    thread_id: &str,
    _slot: ProxySlot,
) -> Response {
    state.stats.chain_request(&model.public_name).await;
    let started_at = now_ms();
    let requested_endpoint = ProxyEndpoint::from_path(pathname);
    let is_stream = body.get("stream").and_then(Value::as_bool) == Some(true);
    let mut errors: Vec<AttemptError> = Vec::new();
    let mut failed_models: Vec<String> = Vec::new();

    for target in targets {
        if state
            .circuit_breakers
            .is_open_and_cleanup(model, &target, &state.stats)
            .await
        {
            let label = target_label(&target);
            let err = AttemptError {
                target: label.clone(),
                message: "circuit open".to_string(),
                ..AttemptError::default()
            };
            state.proxy_runtime.append_error(thread_id, err.clone());
            state.proxy_runtime.update_thread(thread_id, |thread| {
                thread.target_name = target.name.clone();
                thread.target_model = target.model_name.clone();
                thread.target_base_url = target.base_url.clone();
                thread.phase = "skipped".to_string();
                thread.status = "目标处于熔断冷却状态，已跳过".to_string();
                let mut next = failed_models.clone();
                next.push(label.clone());
                thread.failed_models = next;
            });
            errors.push(err);
            failed_models.push(label);
            continue;
        }

        let mut target_body = body.clone();
        let mut compression_attempts = 0;
        let configured_max_attempts = regular_attempt_limit(&target);
        let max_attempts = configured_max_attempts;
        let total_attempts = max_attempts.saturating_add(MAX_CONTEXT_COMPRESSION_ATTEMPTS);
        for attempt in 1..=total_attempts {
            if state.circuit_breakers.is_open(model, &target) {
                let label = target_label(&target);
                let err = AttemptError {
                    target: label.clone(),
                    attempt: Some(attempt),
                    message: "circuit open".to_string(),
                    ..AttemptError::default()
                };
                state.proxy_runtime.append_error(thread_id, err.clone());
                state.proxy_runtime.update_thread(thread_id, |thread| {
                    thread.target_name = target.name.clone();
                    thread.target_model = target.model_name.clone();
                    thread.target_base_url = target.base_url.clone();
                    thread.attempt = attempt;
                    thread.max_attempts = configured_max_attempts;
                    thread.compression_attempt = compression_attempts;
                    thread.max_compression_attempts = MAX_CONTEXT_COMPRESSION_ATTEMPTS;
                    thread.phase = "skipped".to_string();
                    thread.status = "目标在重试前进入熔断冷却状态，已跳过".to_string();
                    let mut next = failed_models.clone();
                    next.push(label.clone());
                    thread.failed_models = next;
                });
                errors.push(err);
                failed_models.push(label);
                break;
            }

            let label = target_label(&target);
            let target_started = now_ms();
            state.proxy_runtime.update_thread(thread_id, |thread| {
                thread.target_name = target.name.clone();
                thread.target_model = target.model_name.clone();
                thread.target_base_url = target.base_url.clone();
                thread.attempt = attempt;
                thread.max_attempts = configured_max_attempts;
                thread.compression_attempt = compression_attempts;
                thread.max_compression_attempts = MAX_CONTEXT_COMPRESSION_ATTEMPTS;
                thread.phase = if compression_attempts > 0 {
                    "context-retrying"
                } else {
                    "calling"
                }
                .to_string();
                thread.status = if compression_attempts > 0 {
                    format!(
                        "正在请求 {}（第 {} 次；上下文压缩重试 {}/{}）",
                        label, attempt, compression_attempts, MAX_CONTEXT_COMPRESSION_ATTEMPTS
                    )
                } else {
                    format!(
                        "正在请求 {}（第 {}/{} 次）",
                        label, attempt, configured_max_attempts
                    )
                };
                thread.failed_models = failed_models.clone();
            });

            let upstream = match call_target(
                state,
                headers,
                &target_body,
                model,
                &target,
                cfg,
                is_stream,
                requested_endpoint,
            )
            .await
            {
                Ok(upstream) => upstream,
                Err(err) => {
                    let failure = FailureInfo {
                        status: err.status(),
                        message: if err.is_timeout() {
                            "timeout".to_string()
                        } else {
                            err.to_string()
                        },
                        ..FailureInfo::default()
                    };
                    state
                        .circuit_breakers
                        .record_failure(
                            model,
                            &target,
                            cfg,
                            &state.stats,
                            failure.clone(),
                            now_ms().saturating_sub(target_started),
                        )
                        .await;
                    let err_item = AttemptError {
                        target: label.clone(),
                        attempt: Some(attempt),
                        message: failure.message.clone(),
                        ..AttemptError::default()
                    };
                    state
                        .proxy_runtime
                        .append_error(thread_id, err_item.clone());
                    state.proxy_runtime.update_thread(thread_id, |thread| {
                        thread.phase =
                            if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                                "retrying"
                            } else {
                                "failed-target"
                            }
                            .to_string();
                        thread.status = format!("{} 请求失败：{}", label, failure.message);
                    });
                    errors.push(err_item);
                    if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                        continue;
                    }
                    failed_models.push(label);
                    break;
                }
            };

            let CompatibleUpstream {
                response: upstream,
                endpoint: used_endpoint,
            } = upstream;
            let status = upstream.status();
            let response_type = upstream
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    if is_stream {
                        "text/event-stream"
                    } else {
                        "application/json"
                    }
                    .to_string()
                });

            if !status.is_success() {
                let text = upstream.text().await.unwrap_or_default();
                if is_context_length_error(status.as_u16(), &text)
                    && compression_attempts < MAX_CONTEXT_COMPRESSION_ATTEMPTS
                {
                    if let Some(compacted) =
                        compact_request_context(&target_body, requested_endpoint)
                    {
                        compression_attempts += 1;
                        let before_tokens = estimate_json_tokens(&target_body);
                        let after_tokens = estimate_json_tokens(&compacted);
                        target_body = compacted;
                        tracing::info!(
                            chain = %model.public_name,
                            target = %label,
                            compression_attempt = compression_attempts,
                            before_tokens,
                            after_tokens,
                            "upstream returned HTTP 422 context-length error; compacting and retrying same target"
                        );
                        state.proxy_runtime.update_thread(thread_id, |thread| {
                            thread.phase = "context-compressing".to_string();
                            thread.status = format!(
                                "{} 返回上下文超限，正在压缩后重试（{}/{}）",
                                label, compression_attempts, MAX_CONTEXT_COMPRESSION_ATTEMPTS
                            );
                        });
                        continue;
                    }
                }
                let failure = classify_upstream_failure(status.as_u16(), &text, false, false)
                    .unwrap_or_else(|| FailureInfo {
                        status: status.as_u16(),
                        message: format!("Upstream returned {}", status.as_u16()),
                        body: trim_error(&text),
                    });
                state
                    .circuit_breakers
                    .record_failure(
                        model,
                        &target,
                        cfg,
                        &state.stats,
                        failure.clone(),
                        now_ms().saturating_sub(target_started),
                    )
                    .await;
                let err_item = AttemptError {
                    target: label.clone(),
                    attempt: Some(attempt),
                    status: Some(failure.status),
                    message: if failure.message.is_empty() {
                        format!("Upstream returned {}", status.as_u16())
                    } else {
                        failure.message.clone()
                    },
                    detail: Some(if failure.body.is_empty() {
                        trim_error(&text)
                    } else {
                        failure.body.clone()
                    }),
                };
                state
                    .proxy_runtime
                    .append_error(thread_id, err_item.clone());
                state.proxy_runtime.update_thread(thread_id, |thread| {
                    thread.phase =
                        if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                            "retrying"
                        } else {
                            "failed-target"
                        }
                        .to_string();
                    thread.status = format!(
                        "{} 返回 HTTP {}",
                        label,
                        failure.status.max(status.as_u16())
                    );
                });
                errors.push(err_item);
                if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                    continue;
                }
                failed_models.push(label);
                if should_try_next(failure.status.max(status.as_u16()), cfg) {
                    break;
                }
                state.stats.chain_failure(&model.public_name).await;
                log_request(
                    &state.stats,
                    model,
                    requested_model,
                    &failed_models,
                    &errors,
                    "",
                    "failed",
                    started_at,
                    &failure.message,
                )
                .await;
                return raw_response(status, response_type, text, None);
            }

            if !is_stream {
                let text = upstream.text().await.unwrap_or_default();
                if let Some(failure) = classify_upstream_failure(status.as_u16(), &text, true, true)
                {
                    state
                        .circuit_breakers
                        .record_failure(
                            model,
                            &target,
                            cfg,
                            &state.stats,
                            failure.clone(),
                            now_ms().saturating_sub(target_started),
                        )
                        .await;
                    let err_item = AttemptError {
                        target: label.clone(),
                        attempt: Some(attempt),
                        status: Some(failure.status),
                        message: failure.message.clone(),
                        detail: Some(failure.body.clone()),
                    };
                    state
                        .proxy_runtime
                        .append_error(thread_id, err_item.clone());
                    state.proxy_runtime.update_thread(thread_id, |thread| {
                        thread.phase =
                            if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                                "retrying"
                            } else {
                                "failed-target"
                            }
                            .to_string();
                        thread.status = format!("{} 响应内容校验失败：{}", label, failure.message);
                    });
                    errors.push(err_item);
                    if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                        continue;
                    }
                    failed_models.push(label);
                    if should_try_next(failure.status, cfg) {
                        break;
                    }
                    state.stats.chain_failure(&model.public_name).await;
                    log_request(
                        &state.stats,
                        model,
                        requested_model,
                        &failed_models,
                        &errors,
                        "",
                        "failed",
                        started_at,
                        &failure.message,
                    )
                    .await;
                    return raw_response(
                        StatusCode::from_u16(failure.status).unwrap_or(StatusCode::BAD_GATEWAY),
                        response_type,
                        text,
                        None,
                    );
                }

                state
                    .circuit_breakers
                    .record_success(
                        model,
                        &target,
                        cfg,
                        &state.stats,
                        now_ms().saturating_sub(target_started),
                    )
                    .await;
                state
                    .stats
                    .chain_success(&model.public_name, !failed_models.is_empty())
                    .await;
                state.proxy_runtime.update_thread(thread_id, |thread| {
                    thread.phase = "completed".to_string();
                    thread.status = format!("已完成来自 {} 的非流式响应", label);
                    thread.failed_models = failed_models.clone();
                });
                log_request(
                    &state.stats,
                    model,
                    requested_model,
                    &failed_models,
                    &errors,
                    &label,
                    "success",
                    started_at,
                    &errors
                        .iter()
                        .map(format_attempt_error)
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .await;
                let text = transform_response_text(
                    requested_endpoint,
                    used_endpoint,
                    &text,
                    requested_model,
                    &target,
                )
                .unwrap_or(text);
                return raw_response(
                    status,
                    response_type,
                    text,
                    Some((&target.name, &target.model_name)),
                );
            }

            if used_endpoint != requested_endpoint
                && (!supports_stream_translation(requested_endpoint, used_endpoint)
                    || !is_event_stream_content_type(&response_type))
            {
                let text = upstream.text().await.unwrap_or_default();
                if let Some(failure) = classify_upstream_failure(status.as_u16(), &text, true, true)
                {
                    state
                        .circuit_breakers
                        .record_failure(
                            model,
                            &target,
                            cfg,
                            &state.stats,
                            failure.clone(),
                            now_ms().saturating_sub(target_started),
                        )
                        .await;
                    let err_item = AttemptError {
                        target: label.clone(),
                        attempt: Some(attempt),
                        status: Some(failure.status),
                        message: failure.message.clone(),
                        detail: Some(failure.body.clone()),
                    };
                    state
                        .proxy_runtime
                        .append_error(thread_id, err_item.clone());
                    errors.push(err_item);
                    if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                        continue;
                    }
                    failed_models.push(label);
                    if should_try_next(failure.status, cfg) {
                        break;
                    }
                    state.stats.chain_failure(&model.public_name).await;
                    log_request(
                        &state.stats,
                        model,
                        requested_model,
                        &failed_models,
                        &errors,
                        "",
                        "failed",
                        started_at,
                        &failure.message,
                    )
                    .await;
                    return send_error(
                        StatusCode::from_u16(failure.status).unwrap_or(StatusCode::BAD_GATEWAY),
                        &failure.message,
                        None,
                    );
                }
                state
                    .circuit_breakers
                    .record_success(
                        model,
                        &target,
                        cfg,
                        &state.stats,
                        now_ms().saturating_sub(target_started),
                    )
                    .await;
                state
                    .stats
                    .chain_success(&model.public_name, !failed_models.is_empty())
                    .await;
                state.proxy_runtime.update_thread(thread_id, |thread| {
                    thread.phase = "streaming".to_string();
                    thread.status = format!("正在以兼容路由返回 {}", label);
                    thread.failed_models = failed_models.clone();
                });
                log_request(
                    &state.stats,
                    model,
                    requested_model,
                    &failed_models,
                    &errors,
                    &label,
                    "success",
                    started_at,
                    &errors
                        .iter()
                        .map(format_attempt_error)
                        .collect::<Vec<_>>()
                        .join(", "),
                )
                .await;
                return synthetic_stream_response(
                    requested_endpoint,
                    used_endpoint,
                    &text,
                    requested_model,
                    &target,
                    _slot.into_stream_guard(),
                );
            }

            let inspected = match tokio::time::timeout(
                target_timeout(&target, cfg),
                inspect_initial_stream(upstream, status.as_u16()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("timeout")),
            };
            let inspected = match inspected {
                Ok(v) => v,
                Err(err) => {
                    let failure = FailureInfo {
                        message: err.to_string(),
                        ..FailureInfo::default()
                    };
                    state
                        .circuit_breakers
                        .record_failure(
                            model,
                            &target,
                            cfg,
                            &state.stats,
                            failure.clone(),
                            now_ms().saturating_sub(target_started),
                        )
                        .await;
                    let err_item = AttemptError {
                        target: label.clone(),
                        attempt: Some(attempt),
                        message: failure.message.clone(),
                        ..AttemptError::default()
                    };
                    state
                        .proxy_runtime
                        .append_error(thread_id, err_item.clone());
                    errors.push(err_item);
                    if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                        continue;
                    }
                    failed_models.push(label);
                    break;
                }
            };

            if let Some(failure) = inspected.failure {
                state
                    .circuit_breakers
                    .record_failure(
                        model,
                        &target,
                        cfg,
                        &state.stats,
                        failure.clone(),
                        now_ms().saturating_sub(target_started),
                    )
                    .await;
                let err_item = AttemptError {
                    target: label.clone(),
                    attempt: Some(attempt),
                    status: Some(failure.status),
                    message: failure.message.clone(),
                    detail: Some(failure.body.clone()),
                };
                state
                    .proxy_runtime
                    .append_error(thread_id, err_item.clone());
                state.proxy_runtime.update_thread(thread_id, |thread| {
                    thread.phase =
                        if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                            "retrying"
                        } else {
                            "failed-target"
                        }
                        .to_string();
                    thread.status = format!("{} 初始流检测失败：{}", label, failure.message);
                });
                errors.push(err_item);
                if should_retry_target(&failure, cfg, attempt, max_attempts, model) {
                    continue;
                }
                failed_models.push(label);
                if should_try_next(failure.status, cfg) {
                    break;
                }
                state.stats.chain_failure(&model.public_name).await;
                log_request(
                    &state.stats,
                    model,
                    requested_model,
                    &failed_models,
                    &errors,
                    "",
                    "failed",
                    started_at,
                    &failure.message,
                )
                .await;
                return send_error(
                    StatusCode::from_u16(failure.status).unwrap_or(StatusCode::BAD_GATEWAY),
                    &failure.message,
                    None,
                );
            }

            state
                .circuit_breakers
                .record_success(
                    model,
                    &target,
                    cfg,
                    &state.stats,
                    now_ms().saturating_sub(target_started),
                )
                .await;
            state.proxy_runtime.update_thread(thread_id, |thread| {
                thread.phase = "streaming".to_string();
                thread.status = format!("正在流式转发 {}", label);
                thread.failed_models = failed_models.clone();
            });
            tracing::info!(thread_id = %thread_id, target = %label, "proxy thread streaming");
            return stream_response(
                state.clone(),
                model.clone(),
                target.clone(),
                thread_id.to_string(),
                requested_model.to_string(),
                failed_models.clone(),
                errors.clone(),
                label,
                started_at,
                target_started,
                status,
                response_type,
                requested_endpoint,
                used_endpoint,
                inspected,
                !failed_models.is_empty(),
                _slot.into_stream_guard(),
            );
        }
    }

    state.stats.chain_failure(&model.public_name).await;
    state.proxy_runtime.update_thread(thread_id, |thread| {
        thread.phase = "failed".to_string();
        thread.status = "所有配置的目标模型均失败".to_string();
        thread.failed_models = failed_models.clone();
    });
    let detail = json!(errors);
    let error_text = errors
        .iter()
        .map(format_attempt_error)
        .collect::<Vec<_>>()
        .join(", ");
    log_request(
        &state.stats,
        model,
        requested_model,
        &failed_models,
        &errors,
        "",
        "failed",
        started_at,
        &error_text,
    )
    .await;
    send_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "All configured targets failed before a response could be returned",
        Some(detail),
    )
}

async fn enabled_targets(state: &AppState, cfg: &Config, model: &ModelConfig) -> Vec<TargetConfig> {
    let configured = model
        .targets
        .iter()
        .filter(|target| {
            target.enabled && !target.base_url.is_empty() && !target_api_keys(target).is_empty()
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut available = Vec::new();
    for target in &configured {
        if !state
            .circuit_breakers
            .is_open_and_cleanup(model, target, &state.stats)
            .await
        {
            available.push(target.clone());
        }
    }
    let selected = if configured.is_empty() || !available.is_empty() {
        available
    } else {
        state
            .circuit_breakers
            .reset_model(model, &state.stats)
            .await;
        configured
    };
    select_target_queue(state, cfg, model, selected).await
}

async fn select_target_queue(
    state: &AppState,
    _cfg: &Config,
    model: &ModelConfig,
    mut targets: Vec<TargetConfig>,
) -> Vec<TargetConfig> {
    targets.sort_by_key(|target| target.priority);
    if targets.len() <= 1 {
        return targets;
    }
    match model.strategy {
        FailoverStrategy::RoundRobin => {
            let cursor = state
                .proxy_runtime
                .round_robin
                .entry(model.public_name.clone())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, AtomicOrdering::Relaxed) as usize;
            rotate_targets(targets, cursor)
        }
        FailoverStrategy::Weighted => {
            let total: u32 = targets.iter().map(|target| target.weight.max(1)).sum();
            let mut ticket = rand::random::<f64>() * total as f64;
            let start = targets
                .iter()
                .position(|target| {
                    ticket -= target.weight.max(1) as f64;
                    ticket <= 0.0
                })
                .unwrap_or(0);
            rotate_targets(targets, start)
        }
        FailoverStrategy::LatencyBased => {
            let mut scored = Vec::with_capacity(targets.len());
            for target in targets {
                let latency = state.stats.avg_latency(model, &target).await;
                scored.push((target, latency));
            }
            scored.sort_by(|(a, la), (b, lb)| {
                let aa = if *la == 0 { u64::MAX } else { *la };
                let bb = if *lb == 0 { u64::MAX } else { *lb };
                aa.cmp(&bb).then_with(|| a.priority.cmp(&b.priority))
            });
            scored.into_iter().map(|(target, _)| target).collect()
        }
        FailoverStrategy::Priority => targets,
    }
}

fn rotate_targets(mut targets: Vec<TargetConfig>, start: usize) -> Vec<TargetConfig> {
    if targets.is_empty() {
        return targets;
    }
    let idx = start % targets.len();
    targets.rotate_left(idx);
    targets
}

async fn call_target(
    state: &AppState,
    inbound_headers: &HeaderMap,
    body: &Value,
    model: &ModelConfig,
    target: &TargetConfig,
    cfg: &Config,
    is_stream: bool,
    requested_endpoint: ProxyEndpoint,
) -> Result<CompatibleUpstream, ProxyCallError> {
    let timeout = target_timeout(target, cfg);
    let candidates = requested_endpoint.candidates();
    let api_key = state
        .proxy_runtime
        .select_target_api_key(model, target)
        .expect("enabled targets always have at least one API key");
    for (idx, endpoint) in candidates.into_iter().enumerate() {
        let upstream_stream = is_stream
            && (endpoint == requested_endpoint
                || supports_stream_translation(requested_endpoint, endpoint));
        let next_body =
            build_upstream_body(body, target, requested_endpoint, endpoint, upstream_stream);
        let mut req = state
            .client
            .post(upstream_endpoint_url(target, endpoint))
            .header(header::CONTENT_TYPE, "application/json")
            .bearer_auth(&api_key)
            .body(serde_json::to_vec(&next_body).unwrap_or_default());
        if let Some(value) = inbound_headers.get("openai-organization") {
            req = req.header("openai-organization", value);
        }
        if let Some(value) = inbound_headers.get("openai-project") {
            req = req.header("openai-project", value);
        }
        let response = if upstream_stream {
            match tokio::time::timeout(timeout, req.send()).await {
                Ok(result) => result?,
                Err(_) => return Err(ProxyCallError::Timeout),
            }
        } else {
            req.timeout(timeout).send().await?
        };
        if response.status().is_success() || idx == 2 {
            return Ok(CompatibleUpstream { response, endpoint });
        }
        if is_endpoint_unsupported_status(response.status()) {
            continue;
        }
        return Ok(CompatibleUpstream { response, endpoint });
    }
    unreachable!("endpoint candidate list is non-empty")
}

fn supports_stream_translation(requested: ProxyEndpoint, upstream: ProxyEndpoint) -> bool {
    matches!(
        (requested, upstream),
        (ProxyEndpoint::ChatCompletions, ProxyEndpoint::Responses)
            | (ProxyEndpoint::Responses, ProxyEndpoint::ChatCompletions)
    )
}

fn is_event_stream_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
}

fn compact_request_context(body: &Value, requested: ProxyEndpoint) -> Option<Value> {
    // Structured multimodal content cannot be safely reduced to text.  Keep the
    // original request intact so a 422 retries/fails over without losing images.
    if contains_image_input(body) {
        return None;
    }
    let before_tokens = estimate_json_tokens(body);
    let input_budget = (before_tokens.saturating_mul(2) / 3).max(256);
    let mut next = body.clone();
    let changed = match requested {
        ProxyEndpoint::ChatCompletions => {
            compact_message_history(&mut next, "messages", input_budget)
        }
        ProxyEndpoint::Responses => {
            if next.get("input").is_some_and(Value::is_array) {
                compact_message_history(&mut next, "input", input_budget)
            } else {
                compact_text_context(&mut next, "input", input_budget)
            }
        }
        ProxyEndpoint::Completions => compact_text_context(&mut next, "prompt", input_budget),
    };
    (changed && estimate_json_tokens(&next) < before_tokens).then_some(next)
}

fn contains_image_input(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(contains_image_input),
        Value::Object(fields) => {
            matches!(
                fields.get("type").and_then(Value::as_str),
                Some("image_url" | "input_image")
            ) || fields.values().any(contains_image_input)
        }
        _ => false,
    }
}

fn compact_message_history(body: &mut Value, field: &str, input_budget: usize) -> bool {
    let Some(messages) = body.get(field).and_then(Value::as_array).cloned() else {
        return false;
    };
    if messages.is_empty() {
        return false;
    }
    let mut per_message_chars = input_budget
        .saturating_mul(2)
        .saturating_div(messages.len().max(1));
    while per_message_chars >= 24 {
        let compacted = messages
            .iter()
            .map(|message| compact_message(message, per_message_chars))
            .collect::<Vec<_>>();
        let mut candidate = body.clone();
        if let Some(obj) = candidate.as_object_mut() {
            obj.insert(field.to_string(), Value::Array(compacted));
        }
        if estimate_json_tokens(&candidate) <= input_budget {
            *body = candidate;
            return true;
        }
        per_message_chars /= 2;
    }
    false
}

fn compact_message(message: &Value, max_chars: usize) -> Value {
    let mut compacted = message.clone();
    if let Some(obj) = compacted.as_object_mut() {
        let content = obj.get("content").map(value_to_text).unwrap_or_default();
        obj.insert(
            "content".to_string(),
            Value::String(compact_text_excerpt(&content, max_chars)),
        );
    }
    compacted
}

fn compact_text_context(body: &mut Value, field: &str, input_budget: usize) -> bool {
    let Some(original) = body.get(field).cloned() else {
        return false;
    };
    let source = value_to_text(&original);
    let mut max_chars = input_budget.saturating_mul(2);
    while max_chars >= 24 {
        let compacted = compact_text_excerpt(&source, max_chars);
        let mut candidate = body.clone();
        if let Some(obj) = candidate.as_object_mut() {
            obj.insert(field.to_string(), Value::String(compacted));
        }
        if estimate_json_tokens(&candidate) <= input_budget {
            *body = candidate;
            return true;
        }
        max_chars /= 2;
    }
    false
}

fn compact_text_excerpt(source: &str, max_chars: usize) -> String {
    let header = "Failover Proxy compacted earlier context:\n";
    let available = max_chars.saturating_sub(header.chars().count());
    if source.chars().count() <= available {
        return source.to_string();
    }
    let excerpt = {
        let head = available / 2;
        let tail = available.saturating_sub(head + 3);
        format!(
            "{}...{}",
            source.chars().take(head).collect::<String>(),
            source
                .chars()
                .rev()
                .take(tail)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
        )
    };
    format!("{}{}", header, excerpt)
}

#[cfg(test)]
fn message_role(message: &Value) -> &str {
    message
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user")
}

fn requested_output_token_reserve(body: &Value) -> usize {
    ["max_output_tokens", "max_completion_tokens", "max_tokens"]
        .iter()
        .find_map(|field| body.get(*field).and_then(Value::as_u64))
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(DEFAULT_OUTPUT_TOKEN_RESERVE)
}

// This intentionally overestimates instead of using a model-specific tokenizer:
// target families may have different tokenizers, and a conservative local budget
// is safer than sending a request that the fallback cannot accept.
const IMAGE_INPUT_TOKEN_RESERVE: usize = 4_096;

fn estimate_json_tokens(value: &Value) -> usize {
    let mut image_inputs = 0usize;
    let normalized = redact_image_payloads_for_estimation(value, &mut image_inputs);
    let text = serde_json::to_string(&normalized).unwrap_or_default();
    let mut ascii_chars = 0usize;
    let mut non_ascii_chars = 0usize;
    for ch in text.chars() {
        if ch.is_ascii() {
            ascii_chars += 1;
        } else {
            non_ascii_chars += 1;
        }
    }
    ascii_chars
        .div_ceil(3)
        .saturating_add(non_ascii_chars.saturating_mul(2))
        .saturating_add(image_inputs.saturating_mul(IMAGE_INPUT_TOKEN_RESERVE))
}

fn redact_image_payloads_for_estimation(value: &Value, image_inputs: &mut usize) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| redact_image_payloads_for_estimation(item, image_inputs))
                .collect(),
        ),
        Value::Object(fields) => {
            let is_image_input = matches!(
                fields.get("type").and_then(Value::as_str),
                Some("image_url" | "input_image")
            );
            if is_image_input {
                *image_inputs = image_inputs.saturating_add(1);
            }
            let mut redacted = serde_json::Map::new();
            for (key, item) in fields {
                if is_image_input && key == "image_url" {
                    redacted.insert(key.clone(), Value::String("[image input]".to_string()));
                } else {
                    redacted.insert(
                        key.clone(),
                        redact_image_payloads_for_estimation(item, image_inputs),
                    );
                }
            }
            Value::Object(redacted)
        }
        _ => value.clone(),
    }
}

fn is_endpoint_unsupported_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 404 | 405 | 501)
}

fn is_context_length_error(status: u16, body: &str) -> bool {
    if status != StatusCode::UNPROCESSABLE_ENTITY.as_u16() {
        return false;
    }
    let message = body.to_ascii_lowercase();
    [
        "context length",
        "context window",
        "maximum context",
        "too many tokens",
        "token limit",
        "input too long",
        "prompt is too long",
        "context_length_exceeded",
        "上下文",
        "输入过长",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

fn upstream_endpoint_url(target: &TargetConfig, endpoint: ProxyEndpoint) -> String {
    format!("{}/{}", trim_slashes(&target.base_url), endpoint.suffix())
}

fn target_api_keys(target: &TargetConfig) -> Vec<String> {
    let mut keys = target
        .api_keys
        .iter()
        .map(|key| key.trim())
        .filter(|key| !key.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if keys.is_empty() && !target.api_key.trim().is_empty() {
        keys.push(target.api_key.trim().to_string());
    }
    keys
}

fn build_upstream_body(
    body: &Value,
    target: &TargetConfig,
    requested: ProxyEndpoint,
    upstream: ProxyEndpoint,
    stream: bool,
) -> Value {
    if requested == upstream {
        let mut next = body.clone();
        set_request_model_and_stream(&mut next, target, Some(stream));
        return next;
    }

    let model = target_request_model(body, target);
    let mut out = serde_json::Map::new();
    out.insert("model".to_string(), Value::String(model));
    out.insert("stream".to_string(), Value::Bool(stream));
    match upstream {
        ProxyEndpoint::ChatCompletions => {
            out.insert(
                "messages".to_string(),
                Value::Array(request_to_chat_messages(body, requested)),
            );
            copy_request_fields(
                body,
                &mut out,
                &[
                    "temperature",
                    "top_p",
                    "presence_penalty",
                    "frequency_penalty",
                    "stop",
                    "response_format",
                    "seed",
                    "user",
                    "metadata",
                ],
            );
            if requested == ProxyEndpoint::Responses {
                if let Some(tools) = body.get("tools") {
                    out.insert("tools".to_string(), responses_tools_to_chat(tools));
                }
                if let Some(tool_choice) = body.get("tool_choice") {
                    out.insert(
                        "tool_choice".to_string(),
                        responses_tool_choice_to_chat(tool_choice),
                    );
                }
            } else {
                copy_request_fields(body, &mut out, &["tools", "tool_choice"]);
            }
            if let Some(value) = body
                .get("max_tokens")
                .or_else(|| body.get("max_output_tokens"))
            {
                out.insert("max_tokens".to_string(), value.clone());
            }
        }
        ProxyEndpoint::Responses => {
            if let Some(instructions) = request_instructions(body, requested) {
                out.insert("instructions".to_string(), Value::String(instructions));
            }
            out.insert(
                "input".to_string(),
                request_to_responses_input(body, requested),
            );
            copy_request_fields(
                body,
                &mut out,
                &[
                    "temperature",
                    "top_p",
                    "tools",
                    "tool_choice",
                    "response_format",
                    "reasoning",
                    "truncation",
                    "metadata",
                    "user",
                ],
            );
            if let Some(value) = body
                .get("max_output_tokens")
                .or_else(|| body.get("max_tokens"))
            {
                out.insert("max_output_tokens".to_string(), value.clone());
            }
        }
        ProxyEndpoint::Completions => {
            out.insert(
                "prompt".to_string(),
                Value::String(request_to_prompt(body, requested)),
            );
            copy_request_fields(
                body,
                &mut out,
                &[
                    "temperature",
                    "top_p",
                    "presence_penalty",
                    "frequency_penalty",
                    "stop",
                    "suffix",
                    "echo",
                    "logprobs",
                    "best_of",
                    "seed",
                    "user",
                ],
            );
            if let Some(value) = body
                .get("max_tokens")
                .or_else(|| body.get("max_output_tokens"))
            {
                out.insert("max_tokens".to_string(), value.clone());
            }
        }
    }
    Value::Object(out)
}

fn set_request_model_and_stream(body: &mut Value, target: &TargetConfig, stream: Option<bool>) {
    let model = target_request_model(body, target);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("model".to_string(), Value::String(model));
        if let Some(stream) = stream {
            obj.insert("stream".to_string(), Value::Bool(stream));
        }
    }
}

fn target_request_model(body: &Value, target: &TargetConfig) -> String {
    if target.model_name.is_empty() {
        body.get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    } else {
        target.model_name.clone()
    }
}

fn copy_request_fields(body: &Value, out: &mut serde_json::Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = body.get(*field) {
            out.insert((*field).to_string(), value.clone());
        }
    }
}

fn request_to_chat_messages(body: &Value, requested: ProxyEndpoint) -> Vec<Value> {
    if requested == ProxyEndpoint::ChatCompletions {
        return body
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| vec![chat_message("user", "")]);
    }
    let mut messages = Vec::new();
    if let Some(instructions) = request_instructions(body, requested) {
        messages.push(chat_message("system", instructions));
    }
    match requested {
        ProxyEndpoint::Responses => {
            messages.extend(responses_input_to_chat_messages(body.get("input")));
        }
        ProxyEndpoint::Completions => {
            messages.push(chat_message("user", request_to_prompt(body, requested)));
        }
        ProxyEndpoint::ChatCompletions => {}
    }
    if messages.is_empty() {
        messages.push(chat_message("user", ""));
    }
    messages
}

fn responses_input_to_chat_messages(input: Option<&Value>) -> Vec<Value> {
    match input {
        Some(Value::String(text)) => vec![chat_message("user", text)],
        Some(Value::Array(items)) => items
            .iter()
            .map(response_input_item_to_chat_message)
            .collect(),
        Some(value) => vec![chat_message("user", value_to_text(value))],
        None => Vec::new(),
    }
}

fn response_input_item_to_chat_message(item: &Value) -> Value {
    match item.get("type").and_then(Value::as_str) {
        Some("function_call") => {
            let call_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let name = item.get("name").and_then(Value::as_str).unwrap_or_default();
            let arguments = item.get("arguments").map(value_to_text).unwrap_or_default();
            json!({
                "role": "assistant",
                "content": Value::Null,
                "tool_calls": [{
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }]
            })
        }
        Some("function_call_output") => json!({
            "role": "tool",
            "tool_call_id": item.get("call_id").and_then(Value::as_str).unwrap_or_default(),
            "content": value_to_text(item.get("output").unwrap_or(&Value::Null))
        }),
        _ if item.get("role").is_some() || item.get("content").is_some() => {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
            json!({
                "role": role,
                "content": map_content_parts(item.get("content"), true)
            })
        }
        _ => chat_message("user", value_to_text(item)),
    }
}

fn responses_tools_to_chat(tools: &Value) -> Value {
    Value::Array(
        tools
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|tool| {
                        if tool.get("type").and_then(Value::as_str) == Some("function")
                            && tool.get("function").is_none()
                        {
                            json!({
                                "type": "function",
                                "function": {
                                    "name": tool.get("name").and_then(Value::as_str).unwrap_or_default(),
                                    "description": tool.get("description").cloned().unwrap_or(Value::Null),
                                    "parameters": tool.get("parameters").cloned().unwrap_or_else(|| json!({})),
                                    "strict": tool.get("strict").cloned().unwrap_or(Value::Null),
                                }
                            })
                        } else {
                            tool.clone()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default(),
    )
}

fn responses_tool_choice_to_chat(tool_choice: &Value) -> Value {
    if tool_choice.get("type").and_then(Value::as_str) == Some("function")
        && tool_choice.get("function").is_none()
    {
        json!({
            "type": "function",
            "function": {"name": tool_choice.get("name").and_then(Value::as_str).unwrap_or_default()}
        })
    } else {
        tool_choice.clone()
    }
}

fn request_to_responses_input(body: &Value, requested: ProxyEndpoint) -> Value {
    match requested {
        ProxyEndpoint::Responses => body.get("input").cloned().unwrap_or(Value::String(String::new())),
        ProxyEndpoint::ChatCompletions => Value::Array(
            body.get("messages")
                .and_then(Value::as_array)
                .map(|messages| {
                    messages
                        .iter()
                        .filter(|message| {
                            !message
                                .get("role")
                                .and_then(Value::as_str)
                                .is_some_and(|role| role == "system" || role == "developer")
                        })
                        .map(|message| {
                            json!({
                                "role": message.get("role").and_then(Value::as_str).unwrap_or("user"),
                                "content": map_content_parts(message.get("content"), false)
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        ),
        ProxyEndpoint::Completions => Value::String(request_to_prompt(body, requested)),
    }
}

fn request_instructions(body: &Value, requested: ProxyEndpoint) -> Option<String> {
    if let Some(value) = body
        .get("instructions")
        .or_else(|| body.get("system"))
        .and_then(Value::as_str)
    {
        return Some(value.to_string());
    }
    if requested == ProxyEndpoint::ChatCompletions {
        let instructions = body
            .get("messages")
            .and_then(Value::as_array)?
            .iter()
            .filter(|message| {
                message
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| role == "system" || role == "developer")
            })
            .map(|message| value_to_text(message.get("content").unwrap_or(&Value::Null)))
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if !instructions.is_empty() {
            return Some(instructions);
        }
    }
    None
}

fn request_to_prompt(body: &Value, requested: ProxyEndpoint) -> String {
    match requested {
        ProxyEndpoint::Completions => body.get("prompt").map(value_to_text).unwrap_or_default(),
        ProxyEndpoint::Responses => {
            let mut parts = Vec::new();
            if let Some(instructions) = request_instructions(body, requested) {
                parts.push(format!("system: {}", instructions));
            }
            parts.push(value_to_text(body.get("input").unwrap_or(&Value::Null)));
            parts.join("\n")
        }
        ProxyEndpoint::ChatCompletions => body
            .get("messages")
            .and_then(Value::as_array)
            .map(|messages| {
                messages
                    .iter()
                    .map(|message| {
                        let role = message
                            .get("role")
                            .and_then(Value::as_str)
                            .unwrap_or("user");
                        let content = value_to_text(message.get("content").unwrap_or(&Value::Null));
                        format!("{}: {}", role, content)
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default(),
    }
}

fn chat_message(role: &str, content: impl Into<String>) -> Value {
    json!({ "role": role, "content": content.into() })
}

fn map_content_parts(content: Option<&Value>, to_chat: bool) -> Value {
    match content {
        Some(Value::Array(parts)) => Value::Array(
            parts
                .iter()
                .map(|part| map_content_part(part, to_chat))
                .collect(),
        ),
        Some(value) => value.clone(),
        None => Value::String(String::new()),
    }
}

fn map_content_part(part: &Value, to_chat: bool) -> Value {
    let mut next = part.clone();
    let Some(obj) = next.as_object_mut() else {
        return next;
    };
    let Some(kind) = obj.get("type").and_then(Value::as_str).map(str::to_string) else {
        return next;
    };

    match (to_chat, kind.as_str()) {
        (true, "input_text") | (true, "output_text") => {
            obj.insert("type".to_string(), Value::String("text".to_string()));
        }
        (false, "text") => {
            obj.insert("type".to_string(), Value::String("input_text".to_string()));
        }
        // Responses stores the URL as a string and keeps `detail` next to it.
        // Chat Completions instead nests both fields in `image_url`.
        (true, "input_image") => {
            let image_url = obj.get("image_url").cloned();
            let detail = obj.remove("detail").or_else(|| {
                image_url
                    .as_ref()
                    .and_then(|value| value.get("detail"))
                    .cloned()
            });
            obj.insert("type".to_string(), Value::String("image_url".to_string()));
            match image_url {
                Some(Value::String(url)) => {
                    let mut image = serde_json::Map::new();
                    image.insert("url".to_string(), Value::String(url));
                    if let Some(detail) = detail {
                        image.insert("detail".to_string(), detail);
                    }
                    obj.insert("image_url".to_string(), Value::Object(image));
                }
                Some(Value::Object(mut image)) => {
                    if !image.contains_key("detail") {
                        if let Some(detail) = detail {
                            image.insert("detail".to_string(), detail);
                        }
                    }
                    obj.insert("image_url".to_string(), Value::Object(image));
                }
                Some(value) => {
                    obj.insert("image_url".to_string(), value);
                }
                None => {}
            }
        }
        (false, "image_url") => {
            let image_url = obj.get("image_url").cloned();
            let nested_detail = image_url
                .as_ref()
                .and_then(|value| value.get("detail"))
                .cloned();
            let url = image_url.as_ref().and_then(|value| match value {
                Value::Object(image) => image.get("url").cloned(),
                Value::String(url) => Some(Value::String(url.clone())),
                _ => None,
            });
            obj.insert("type".to_string(), Value::String("input_image".to_string()));
            if let Some(url) = url {
                obj.insert("image_url".to_string(), url);
            }
            if obj.get("detail").is_none() {
                if let Some(detail) = nested_detail {
                    obj.insert("detail".to_string(), detail);
                }
            }
        }
        _ => {}
    }
    next
}

fn transform_response_text(
    requested: ProxyEndpoint,
    upstream: ProxyEndpoint,
    text: &str,
    requested_model: &str,
    target: &TargetConfig,
) -> Option<String> {
    if requested == upstream {
        return None;
    }
    let payload = parse_json_safe(text)?;
    let transformed = response_payload_as(requested, upstream, &payload, requested_model, target);
    serde_json::to_string(&transformed).ok()
}

fn response_payload_as(
    requested: ProxyEndpoint,
    upstream: ProxyEndpoint,
    payload: &Value,
    requested_model: &str,
    target: &TargetConfig,
) -> Value {
    match requested {
        ProxyEndpoint::ChatCompletions => {
            response_as_chat(upstream, payload, requested_model, target)
        }
        ProxyEndpoint::Responses => {
            response_as_responses(upstream, payload, requested_model, target)
        }
        ProxyEndpoint::Completions => {
            response_as_completions(upstream, payload, requested_model, target)
        }
    }
}

fn response_as_chat(
    upstream: ProxyEndpoint,
    payload: &Value,
    requested_model: &str,
    target: &TargetConfig,
) -> Value {
    let text = response_text(upstream, payload);
    let finish_reason = response_finish_reason(payload);
    json!({
        "id": response_id(payload, "chatcmpl"),
        "object": "chat.completion",
        "created": response_created(payload),
        "model": requested_model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": text,
                "refusal": Value::Null,
            },
            "finish_reason": finish_reason,
        }],
        "usage": payload.get("usage").cloned().unwrap_or_else(|| json!({})),
        "failover_proxy_upstream": {
            "endpoint": upstream.suffix(),
            "target": target.name,
            "model": target.model_name,
        }
    })
}

fn response_as_responses(
    upstream: ProxyEndpoint,
    payload: &Value,
    requested_model: &str,
    target: &TargetConfig,
) -> Value {
    let text = response_text(upstream, payload);
    let output = responses_output_from_upstream(upstream, payload, &text);
    json!({
        "id": response_id(payload, "resp"),
        "object": "response",
        "created_at": response_created(payload),
        "status": "completed",
        "model": requested_model,
        "output": output,
        "output_text": text,
        "usage": payload.get("usage").cloned().unwrap_or_else(|| json!({})),
        "failover_proxy_upstream": {
            "endpoint": upstream.suffix(),
            "target": target.name,
            "model": target.model_name,
        }
    })
}

fn responses_output_from_upstream(
    upstream: ProxyEndpoint,
    payload: &Value,
    text: &str,
) -> Vec<Value> {
    if upstream == ProxyEndpoint::Responses {
        return payload
            .get("output")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
    }

    let mut output = Vec::new();
    if !text.is_empty() {
        output.push(json!({
            "type": "message",
            "id": format!("msg_{}", now_ms()),
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": []
            }]
        }));
    }
    for call in assistant_tool_calls(payload) {
        let function = call.get("function").unwrap_or(&call);
        let call_id = call
            .get("id")
            .or_else(|| call.get("call_id"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let name = function
            .get("name")
            .or_else(|| call.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let arguments = function
            .get("arguments")
            .or_else(|| call.get("arguments"))
            .map(value_to_text)
            .unwrap_or_default();
        if !name.is_empty() {
            output.push(json!({
                "type": "function_call",
                "id": format!("fc_{}", call_id),
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
                "status": "completed"
            }));
        }
    }
    if output.is_empty() {
        output.push(json!({
            "type": "message",
            "id": format!("msg_{}", now_ms()),
            "status": "completed",
            "role": "assistant",
            "content": []
        }));
    }
    output
}

fn response_as_completions(
    upstream: ProxyEndpoint,
    payload: &Value,
    requested_model: &str,
    target: &TargetConfig,
) -> Value {
    let text = response_text(upstream, payload);
    let finish_reason = response_finish_reason(payload);
    json!({
        "id": response_id(payload, "cmpl"),
        "object": "text_completion",
        "created": response_created(payload),
        "model": requested_model,
        "choices": [{
            "text": text,
            "index": 0,
            "logprobs": Value::Null,
            "finish_reason": finish_reason
        }],
        "usage": payload.get("usage").cloned().unwrap_or_else(|| json!({})),
        "failover_proxy_upstream": {
            "endpoint": upstream.suffix(),
            "target": target.name,
            "model": target.model_name,
        }
    })
}

fn response_text(upstream: ProxyEndpoint, payload: &Value) -> String {
    match upstream {
        ProxyEndpoint::ChatCompletions => assistant_text(payload),
        ProxyEndpoint::Responses => payload
            .get("output_text")
            .and_then(Value::as_str)
            .map(str::to_string)
            .filter(|text| !text.is_empty())
            .unwrap_or_else(|| responses_output_text(payload)),
        ProxyEndpoint::Completions => payload
            .pointer("/choices/0/text")
            .map(value_to_text)
            .unwrap_or_default(),
    }
}

fn responses_output_text(payload: &Value) -> String {
    payload
        .get("output")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .flat_map(|item| {
                    item.get("content")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default()
                })
                .map(|part| value_to_text(&part))
                .filter(|text| !text.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn response_finish_reason(payload: &Value) -> String {
    payload
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .or_else(|| payload.get("finish_reason").and_then(Value::as_str))
        .unwrap_or("stop")
        .to_string()
}

fn response_created(payload: &Value) -> u64 {
    payload
        .get("created")
        .or_else(|| payload.get("created_at"))
        .and_then(Value::as_u64)
        .unwrap_or_else(|| now_ms() / 1000)
}

fn response_id(payload: &Value, prefix: &str) -> String {
    payload
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("{}-{}", prefix, now_ms()))
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(value_to_text)
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(obj) => obj
            .get("text")
            .or_else(|| obj.get("content"))
            .or_else(|| obj.get("input_text"))
            .or_else(|| obj.get("output_text"))
            .map(value_to_text)
            .unwrap_or_else(|| value.to_string()),
        _ => value.to_string(),
    }
}

fn synthetic_stream_response(
    requested: ProxyEndpoint,
    upstream: ProxyEndpoint,
    text: &str,
    requested_model: &str,
    target: &TargetConfig,
    slot: StreamSlotGuard,
) -> Response {
    let payload = parse_json_safe(text).unwrap_or_else(|| json!({ "output_text": text }));
    let transformed = response_payload_as(requested, upstream, &payload, requested_model, target);
    let stream_text = synthetic_sse_text(requested, &transformed);
    let body_stream = async_stream::stream! {
        let slot_guard = slot;
        yield Ok::<Bytes, Infallible>(Bytes::from(stream_text));
        drop(slot_guard);
    };
    let mut response = Response::new(Body::from_stream(body_stream));
    *response.status_mut() = StatusCode::OK;
    insert_common_proxy_headers(
        response.headers_mut(),
        "text/event-stream",
        &target.name,
        &target.model_name,
        true,
    );
    response
}

fn synthetic_sse_text(endpoint: ProxyEndpoint, payload: &Value) -> String {
    let text = response_text(endpoint, payload);
    match endpoint {
        ProxyEndpoint::ChatCompletions => {
            let chunk_id = response_id(payload, "chatcmpl");
            let created = response_created(payload);
            let model = payload.get("model").and_then(Value::as_str).unwrap_or("");
            let first = json!({
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": text}, "finish_reason": Value::Null}]
            });
            let done = json!({
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": response_finish_reason(payload)}]
            });
            format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n", first, done)
        }
        ProxyEndpoint::Responses => synthetic_responses_sse(payload, &text),
        ProxyEndpoint::Completions => {
            let chunk = json!({
                "id": response_id(payload, "cmpl"),
                "object": "text_completion",
                "created": response_created(payload),
                "model": payload.get("model").and_then(Value::as_str).unwrap_or(""),
                "choices": [{"text": text, "index": 0, "logprobs": Value::Null, "finish_reason": response_finish_reason(payload)}]
            });
            format!("data: {}\n\ndata: [DONE]\n\n", chunk)
        }
    }
}

fn synthetic_responses_sse(payload: &Value, text: &str) -> String {
    let mut response = payload.clone();
    if let Some(obj) = response.as_object_mut() {
        obj.insert(
            "status".to_string(),
            Value::String("in_progress".to_string()),
        );
    }
    let created = json!({ "type": "response.created", "response": response });
    let mut events = vec![format!("event: response.created\ndata: {}\n\n", created)];

    for (output_index, item) in payload
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        events.push(format!(
            "event: response.output_item.added\ndata: {}\n\n",
            json!({ "type": "response.output_item.added", "output_index": output_index, "item": item })
        ));
        if item.get("type").and_then(Value::as_str) == Some("message") && !text.is_empty() {
            events.push(format!(
                "event: response.output_text.delta\ndata: {}\n\n",
                json!({ "type": "response.output_text.delta", "output_index": output_index, "content_index": 0, "delta": text })
            ));
        }
        events.push(format!(
            "event: response.output_item.done\ndata: {}\n\n",
            json!({ "type": "response.output_item.done", "output_index": output_index, "item": item })
        ));
    }

    events.push(format!(
        "event: response.completed\ndata: {}\n\n",
        json!({ "type": "response.completed", "response": payload })
    ));
    events.push("data: [DONE]\n\n".to_string());
    events.concat()
}

fn target_timeout(target: &TargetConfig, cfg: &Config) -> Duration {
    Duration::from_millis(if target.timeout_ms == 0 {
        cfg.request_timeout_ms
    } else {
        target.timeout_ms
    })
}

#[derive(Default)]
struct StreamInspection {
    chunks: Vec<Bytes>,
    stream: Option<Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>>,
    failure: Option<FailureInfo>,
}

async fn inspect_initial_stream(
    upstream: reqwest::Response,
    status: u16,
) -> anyhow::Result<StreamInspection> {
    let ok = upstream.status().is_success();
    let mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>> =
        Box::pin(upstream.bytes_stream());
    let mut chunks = Vec::new();
    let mut text = String::new();
    while text.len() < stream_probe_bytes() {
        match stream.next().await {
            Some(Ok(chunk)) => {
                text.push_str(&String::from_utf8_lossy(&chunk));
                chunks.push(chunk);
                if let Some(failure) = classify_upstream_failure(status, &text, ok, false) {
                    return Ok(StreamInspection {
                        chunks,
                        stream: Some(stream),
                        failure: Some(failure),
                    });
                }
                if stream_probe_complete(&text) {
                    return Ok(StreamInspection {
                        chunks,
                        stream: Some(stream),
                        failure: None,
                    });
                }
            }
            Some(Err(err)) => return Err(err.into()),
            None => {
                if let Some(failure) = classify_upstream_failure(status, &text, ok, false) {
                    return Ok(StreamInspection {
                        chunks,
                        stream: None,
                        failure: Some(failure),
                    });
                }
                return Ok(StreamInspection {
                    chunks,
                    stream: None,
                    failure: None,
                });
            }
        }
    }
    Ok(StreamInspection {
        chunks,
        stream: Some(stream),
        failure: None,
    })
}

struct StreamProtocolTransformer {
    requested: ProxyEndpoint,
    upstream: ProxyEndpoint,
    pending: String,
    response_id: Option<String>,
    created: Option<u64>,
    model: Option<String>,
    response_started: bool,
    output_item_started: bool,
    chat_role_sent: bool,
    text_emitted: bool,
    finished: bool,
}

impl StreamProtocolTransformer {
    fn new(requested: ProxyEndpoint, upstream: ProxyEndpoint) -> Self {
        Self {
            requested,
            upstream,
            pending: String::new(),
            response_id: None,
            created: None,
            model: None,
            response_started: false,
            output_item_started: false,
            chat_role_sent: false,
            text_emitted: false,
            finished: false,
        }
    }

    fn transform_chunk(&mut self, chunk: &Bytes) -> Bytes {
        if self.requested == self.upstream {
            return chunk.clone();
        }
        self.pending.push_str(&String::from_utf8_lossy(chunk));
        let mut transformed = String::new();
        while let Some(event) = take_complete_sse_event(&mut self.pending) {
            transformed.push_str(&self.transform_event(&event));
        }
        Bytes::from(transformed)
    }

    fn transform_event(&mut self, event: &str) -> String {
        match (self.requested, self.upstream) {
            (ProxyEndpoint::Responses, ProxyEndpoint::ChatCompletions) => {
                self.chat_event_as_responses(event)
            }
            (ProxyEndpoint::ChatCompletions, ProxyEndpoint::Responses) => {
                self.responses_event_as_chat(event)
            }
            _ => event.to_string(),
        }
    }

    fn chat_event_as_responses(&mut self, event: &str) -> String {
        if sse_event_is_done(event) {
            return self.finish_as_responses();
        }
        let mut output = String::new();
        for payload in parse_sse_data_payloads(event) {
            let Some(payload) = parse_json_safe(&payload) else {
                continue;
            };
            self.remember_chat_metadata(&payload);
            output.push_str(&self.start_responses_message());
            let content = payload
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !content.is_empty() {
                self.text_emitted = true;
                output.push_str(&sse_event(
                    "response.output_text.delta",
                    json!({
                        "type": "response.output_text.delta",
                        "output_index": 0,
                        "content_index": 0,
                        "delta": content,
                    }),
                ));
            }
            if payload
                .pointer("/choices/0/finish_reason")
                .is_some_and(|reason| !reason.is_null())
            {
                output.push_str(&self.finish_as_responses());
            }
        }
        output
    }

    fn responses_event_as_chat(&mut self, event: &str) -> String {
        if sse_event_is_done(event) {
            return self.finish_as_chat(None);
        }
        let mut output = String::new();
        for payload in parse_sse_data_payloads(event) {
            let Some(payload) = parse_json_safe(&payload) else {
                continue;
            };
            self.remember_responses_metadata(&payload);
            match payload.get("type").and_then(Value::as_str) {
                Some("response.output_text.delta") => {
                    let delta = payload.get("delta").and_then(Value::as_str).unwrap_or_default();
                    if !delta.is_empty() {
                        self.text_emitted = true;
                        output.push_str(&self.chat_delta(delta));
                    }
                }
                Some("response.output_text.done") if !self.text_emitted => {
                    let text = payload.get("text").and_then(Value::as_str).unwrap_or_default();
                    if !text.is_empty() {
                        self.text_emitted = true;
                        output.push_str(&self.chat_delta(text));
                    }
                }
                Some("response.completed") => {
                    let response = payload.get("response").unwrap_or(&payload);
                    if !self.text_emitted {
                        let text = response_text(ProxyEndpoint::Responses, response);
                        if !text.is_empty() {
                            self.text_emitted = true;
                            output.push_str(&self.chat_delta(&text));
                        }
                    }
                    output.push_str(&self.finish_as_chat(Some(response)));
                }
                _ => {}
            }
        }
        output
    }

    fn remember_chat_metadata(&mut self, payload: &Value) {
        if self.response_id.is_none() {
            self.response_id = payload.get("id").and_then(Value::as_str).map(str::to_string);
        }
        if self.created.is_none() {
            self.created = payload.get("created").and_then(Value::as_u64);
        }
        if self.model.is_none() {
            self.model = payload.get("model").and_then(Value::as_str).map(str::to_string);
        }
    }

    fn remember_responses_metadata(&mut self, payload: &Value) {
        let response = payload.get("response").unwrap_or(payload);
        if self.response_id.is_none() {
            self.response_id = response.get("id").and_then(Value::as_str).map(str::to_string);
        }
        if self.created.is_none() {
            self.created = response
                .get("created_at")
                .or_else(|| response.get("created"))
                .and_then(Value::as_u64);
        }
        if self.model.is_none() {
            self.model = response.get("model").and_then(Value::as_str).map(str::to_string);
        }
    }

    fn stream_id(&self, prefix: &str) -> String {
        self.response_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", prefix, now_ms()))
    }

    fn start_responses_message(&mut self) -> String {
        if self.response_started {
            return String::new();
        }
        self.response_started = true;
        self.output_item_started = true;
        let id = self.stream_id("resp");
        let created = self.created.unwrap_or_else(|| now_ms() / 1000);
        let model = self.model.clone().unwrap_or_default();
        format!(
            "{}{}",
            sse_event(
                "response.created",
                json!({
                    "type": "response.created",
                    "response": {
                        "id": id,
                        "object": "response",
                        "created_at": created,
                        "status": "in_progress",
                        "model": model,
                        "output": [],
                    }
                }),
            ),
            sse_event(
                "response.output_item.added",
                json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {
                        "id": format!("msg_{}", id),
                        "type": "message",
                        "status": "in_progress",
                        "role": "assistant",
                        "content": [],
                    }
                }),
            )
        )
    }

    fn finish_as_responses(&mut self) -> String {
        if self.finished {
            return String::new();
        }
        self.finished = true;
        let mut output = self.start_responses_message();
        let id = self.stream_id("resp");
        if self.output_item_started {
            output.push_str(&sse_event(
                "response.output_item.done",
                json!({
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": {
                        "id": format!("msg_{}", id),
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [],
                    }
                }),
            ));
        }
        output.push_str(&sse_event(
            "response.completed",
            json!({
                "type": "response.completed",
                "response": {
                    "id": id,
                    "object": "response",
                    "created_at": self.created.unwrap_or_else(|| now_ms() / 1000),
                    "status": "completed",
                    "model": self.model.clone().unwrap_or_default(),
                    "output": [],
                }
            }),
        ));
        output.push_str("data: [DONE]\n\n");
        output
    }

    fn chat_delta(&mut self, content: &str) -> String {
        let mut delta = serde_json::Map::new();
        if !self.chat_role_sent {
            self.chat_role_sent = true;
            delta.insert("role".to_string(), Value::String("assistant".to_string()));
        }
        delta.insert("content".to_string(), Value::String(content.to_string()));
        sse_data(json!({
            "id": self.stream_id("chatcmpl"),
            "object": "chat.completion.chunk",
            "created": self.created.unwrap_or_else(|| now_ms() / 1000),
            "model": self.model.clone().unwrap_or_default(),
            "choices": [{"index": 0, "delta": delta, "finish_reason": Value::Null}],
        }))
    }

    fn finish_as_chat(&mut self, response: Option<&Value>) -> String {
        if self.finished {
            return String::new();
        }
        self.finished = true;
        if let Some(response) = response {
            self.remember_responses_metadata(response);
        }
        format!(
            "{}data: [DONE]\n\n",
            sse_data(json!({
                "id": self.stream_id("chatcmpl"),
                "object": "chat.completion.chunk",
                "created": self.created.unwrap_or_else(|| now_ms() / 1000),
                "model": self.model.clone().unwrap_or_default(),
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }))
        )
    }
}

fn take_complete_sse_event(pending: &mut String) -> Option<String> {
    let lf = pending.find("\n\n");
    let crlf = pending.find("\r\n\r\n");
    let (index, separator_len) = match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => (lf, 2),
        (Some(_), Some(crlf)) => (crlf, 4),
        (Some(lf), None) => (lf, 2),
        (None, Some(crlf)) => (crlf, 4),
        (None, None) => return None,
    };
    Some(pending.drain(..index + separator_len).collect())
}

fn sse_event_is_done(event: &str) -> bool {
    event.lines().any(|line| {
        line.trim_start()
            .strip_prefix("data:")
            .is_some_and(|data| data.trim() == "[DONE]")
    })
}

fn sse_event(name: &str, payload: Value) -> String {
    format!("event: {name}\ndata: {payload}\n\n")
}

fn sse_data(payload: Value) -> String {
    format!("data: {payload}\n\n")
}

#[allow(clippy::too_many_arguments)]
fn stream_response(
    state: AppState,
    model: ModelConfig,
    target: TargetConfig,
    thread_id: String,
    requested_model: String,
    failed_models: Vec<String>,
    failed_errors: Vec<AttemptError>,
    label: String,
    started_at: u64,
    _target_started: u64,
    status: StatusCode,
    response_type: String,
    requested_endpoint: ProxyEndpoint,
    upstream_endpoint: ProxyEndpoint,
    mut inspected: StreamInspection,
    failover: bool,
    slot: StreamSlotGuard,
) -> Response {
    let response_target_name = target.name.clone();
    let response_model_name = target.model_name.clone();
    let body_stream = async_stream::stream! {
        let slot_guard = slot;
        let mut transformer = StreamProtocolTransformer::new(requested_endpoint, upstream_endpoint);
        let mut request_errors = failed_errors.clone();
        let mut completion = StreamCompletionDetector::default();
        let mut completed = false;
        let mut abort_guard = StreamAbortGuard::new(
            state.clone(),
            model.clone(),
            thread_id.clone(),
            requested_model.clone(),
            failed_models.clone(),
            request_errors.clone(),
            label.clone(),
            started_at,
        );
        let mut stream_failure = None;
        for chunk in inspected.chunks.drain(..) {
            if !completed && completion.observe(&chunk) {
                completed = true;
                abort_guard.disarm();
                complete_stream_success(
                    &state,
                    &model,
                    &thread_id,
                    &requested_model,
                    &failed_models,
                    &request_errors,
                    &label,
                    started_at,
                    failover,
                )
                .await;
            }
            let transformed = transformer.transform_chunk(&chunk);
            if !transformed.is_empty() {
                yield Ok::<Bytes, Infallible>(transformed);
            }
            if completed {
                break;
            }
        }
        if !completed {
            if let Some(mut stream) = inspected.stream {
                loop {
                    match stream.next().await {
                        Some(Ok(chunk)) => {
                            if completion.observe(&chunk) {
                                completed = true;
                                abort_guard.disarm();
                                complete_stream_success(
                                    &state,
                                    &model,
                                    &thread_id,
                                    &requested_model,
                                    &failed_models,
                                    &request_errors,
                                    &label,
                                    started_at,
                                    failover,
                                )
                                .await;
                            }
                            let transformed = transformer.transform_chunk(&chunk);
                            if !transformed.is_empty() {
                                yield Ok::<Bytes, Infallible>(transformed);
                            }
                            if completed {
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            stream_failure = Some(FailureInfo {
                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                message: err.to_string(),
                                ..FailureInfo::default()
                            });
                            break;
                        }
                        None => {
                            stream_failure = Some(FailureInfo {
                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                message: "Upstream stream ended without a completion event".to_string(),
                                ..FailureInfo::default()
                            });
                            break;
                        }
                    }
                }
            } else {
                stream_failure = Some(FailureInfo {
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                    message: "Upstream stream ended without a completion event".to_string(),
                    ..FailureInfo::default()
                });
            }
        }
        if !completed {
            if let Some(failure) = stream_failure {
                let mut stream_failed_models = failed_models.clone();
                stream_failed_models.push(label.clone());
                let err_item = AttemptError {
                    target: label.clone(),
                    status: Some(failure.status),
                    message: failure.message.clone(),
                    detail: Some(failure.body.clone()),
                    ..AttemptError::default()
                };
                state.proxy_runtime.append_error(&thread_id, err_item.clone());
                request_errors.push(err_item);
                state.proxy_runtime.update_thread(&thread_id, |thread| {
                    thread.phase = "failed".to_string();
                    thread.status = format!("流式响应中检测到失败：{}", failure.message);
                    thread.failed_models = stream_failed_models.clone();
                });
                state.stats.chain_failure(&model.public_name).await;
                abort_guard.disarm();
                log_request(
                    &state.stats,
                    &model,
                    &requested_model,
                    &stream_failed_models,
                    &request_errors,
                    "",
                    "failed",
                    started_at,
                    &failure.message,
                )
                .await;
            } else {
                abort_guard.disarm();
                complete_stream_success(
                    &state,
                    &model,
                    &thread_id,
                    &requested_model,
                    &failed_models,
                    &request_errors,
                    &label,
                    started_at,
                    failover,
                )
                .await;
            }
        }
        drop(slot_guard);
    };
    let mut response = Response::new(Body::from_stream(body_stream));
    *response.status_mut() = status;
    insert_common_proxy_headers(
        response.headers_mut(),
        if requested_endpoint == upstream_endpoint {
            &response_type
        } else {
            "text/event-stream"
        },
        &response_target_name,
        &response_model_name,
        true,
    );
    response
}

#[allow(clippy::too_many_arguments)]
async fn complete_stream_success(
    state: &AppState,
    model: &ModelConfig,
    thread_id: &str,
    requested_model: &str,
    failed_models: &[String],
    request_errors: &[AttemptError],
    label: &str,
    started_at: u64,
    failover: bool,
) {
    state
        .stats
        .chain_success(&model.public_name, failover)
        .await;
    state.proxy_runtime.update_thread(thread_id, |thread| {
        thread.phase = "completed".to_string();
        thread.status = format!("已完成来自 {} 的流式响应", label);
        thread.failed_models = failed_models.to_vec();
    });
    log_request(
        &state.stats,
        model,
        requested_model,
        failed_models,
        request_errors,
        label,
        "success",
        started_at,
        "",
    )
    .await;
}

#[derive(Default)]
struct StreamCompletionDetector {
    tail: String,
}

impl StreamCompletionDetector {
    fn observe(&mut self, chunk: &Bytes) -> bool {
        self.tail.push_str(&String::from_utf8_lossy(chunk));
        if self.tail.len() > 4096 {
            let keep_from = self
                .tail
                .char_indices()
                .rev()
                .nth(4095)
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            self.tail = self.tail[keep_from..].to_string();
        }
        stream_text_has_done_marker(&self.tail)
    }
}

fn stream_text_has_done_marker(text: &str) -> bool {
    text.lines().any(|line| {
        line.trim_start()
            .strip_prefix("data:")
            .map(|data| data.trim() == "[DONE]")
            .unwrap_or(false)
    }) || parse_sse_data_payloads(text).iter().any(|data| {
        parse_json_safe(data).is_some_and(|payload| {
            payload.get("type").and_then(Value::as_str) == Some("response.completed")
                || payload
                    .pointer("/choices/0/finish_reason")
                    .is_some_and(|reason| !reason.is_null())
        })
    })
}

struct StreamAbortGuard {
    state: AppState,
    model: ModelConfig,
    thread_id: String,
    requested_model: String,
    failed_models: Vec<String>,
    request_errors: Vec<AttemptError>,
    label: String,
    started_at: u64,
    disarmed: bool,
}

impl StreamAbortGuard {
    fn new(
        state: AppState,
        model: ModelConfig,
        thread_id: String,
        requested_model: String,
        failed_models: Vec<String>,
        request_errors: Vec<AttemptError>,
        label: String,
        started_at: u64,
    ) -> Self {
        Self {
            state,
            model,
            thread_id,
            requested_model,
            failed_models,
            request_errors,
            label,
            started_at,
            disarmed: false,
        }
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for StreamAbortGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let state = self.state.clone();
        let model = self.model.clone();
        let thread_id = self.thread_id.clone();
        let requested_model = self.requested_model.clone();
        let failed_models = self.failed_models.clone();
        let request_errors = self.request_errors.clone();
        let label = self.label.clone();
        let started_at = self.started_at;
        release_proxy_slot(state.proxy_runtime.clone(), thread_id.clone());
        tokio::spawn(async move {
            let message = "客户端在流式响应完成前断开连接".to_string();
            state.proxy_runtime.update_thread(&thread_id, |thread| {
                thread.phase = "completed".to_string();
                thread.status = message.clone();
                thread.failed_models = failed_models.clone();
            });
            state
                .stats
                .chain_success(&model.public_name, !failed_models.is_empty())
                .await;
            log_request(
                &state.stats,
                &model,
                &requested_model,
                &failed_models,
                &request_errors,
                &label,
                "success",
                started_at,
                &message,
            )
            .await;
        });
    }
}

pub fn classify_upstream_failure(
    status: u16,
    text: &str,
    response_ok: bool,
    validate_empty_output: bool,
) -> Option<FailureInfo> {
    let embedded = embedded_error(parse_json_safe(text).as_ref(), text)
        .or_else(|| embedded_stream_error(text));
    if !response_ok {
        return Some(FailureInfo {
            status,
            message: embedded
                .as_ref()
                .map(|e| e.message.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("Upstream returned {}", status)),
            body: trim_error(text),
        });
    }
    if let Some(err) = embedded {
        return Some(err);
    }
    if validate_empty_output {
        return validate_assistant_output(text);
    }
    None
}

fn parse_json_safe(text: &str) -> Option<Value> {
    if text.is_empty() {
        None
    } else {
        serde_json::from_str(text).ok()
    }
}

fn embedded_error(payload: Option<&Value>, text: &str) -> Option<FailureInfo> {
    let payload = payload?;
    let provider_code = payload
        .get("code")
        .or_else(|| payload.pointer("/error/code"))
        .or_else(|| payload.pointer("/detail/code"));
    let provider_code_is_error = provider_code.is_some_and(|value| {
        value.as_u64().is_some_and(|code| code >= 400)
            || value.as_str().is_some_and(|code| {
                code.parse::<u16>().is_ok_and(|status| status >= 400)
                    || [
                        "error",
                        "invalid",
                        "limit",
                        "quota",
                        "unauthorized",
                        "forbidden",
                    ]
                    .iter()
                    .any(|marker| code.to_ascii_lowercase().contains(marker))
            })
    });
    let has_explicit_error = payload.get("error").is_some()
        || payload.get("error_message").is_some()
        || payload.get("status_code").is_some()
        || provider_code_is_error
        || payload.get("success").and_then(Value::as_bool) == Some(false);
    let error = payload
        .get("error")
        .or_else(|| payload.pointer("/detail/error"))
        .or_else(|| payload.pointer("/details/error"));
    let mut candidates = Vec::new();
    if let Some(err) = error {
        if let Some(s) = err.as_str() {
            candidates.push(s.to_string());
        }
        if let Some(s) = err.get("message").and_then(Value::as_str) {
            candidates.push(s.to_string());
        }
        if let Some(s) = err.get("details").and_then(Value::as_str) {
            candidates.push(s.to_string());
        }
    }
    if has_explicit_error {
        if let Some(s) = payload.get("message").and_then(Value::as_str) {
            candidates.push(s.to_string());
        }
        if let Some(s) = payload.get("msg").and_then(Value::as_str) {
            candidates.push(s.to_string());
        }
        if let Some(s) = payload.get("error_description").and_then(Value::as_str) {
            candidates.push(s.to_string());
        }
    }
    if let Some(s) = payload.get("detail").and_then(Value::as_str) {
        candidates.push(s.to_string());
    }
    if let Some(s) = payload.get("error_message").and_then(Value::as_str) {
        candidates.push(s.to_string());
    }
    let message = candidates
        .into_iter()
        .find(|item| !item.trim().is_empty())
        .unwrap_or_default();
    let raw_status = error
        .and_then(|err| {
            err.get("status")
                .or_else(|| err.get("status_code"))
                .or_else(|| err.get("code"))
        })
        .or_else(|| payload.get("status"))
        .or_else(|| payload.get("status_code"))
        .or(provider_code);
    let numeric = raw_status
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str()?.parse::<u64>().ok())
        })
        .unwrap_or(0) as u16;
    let regex_status = regex::Regex::new(r"(?i)\b(?:returned|status|code|http)\s*:?\s*(\d{3})\b")
        .ok()
        .and_then(|re| {
            re.captures(text)
                .and_then(|cap| cap.get(1))
                .and_then(|m| m.as_str().parse::<u16>().ok())
        })
        .unwrap_or(0);
    let inferred_status = infer_error_status(
        provider_code.or(error.and_then(|err| err.get("code"))),
        &message,
    );
    let status = if numeric >= 400 {
        numeric
    } else if regex_status >= 400 {
        regex_status
    } else {
        inferred_status
    };
    if !message.is_empty() || status >= 400 {
        Some(FailureInfo {
            status: if status >= 400 { status } else { 0 },
            message: if !message.is_empty() {
                message
            } else {
                format!("Upstream returned {}", status)
            },
            body: trim_error(text),
        })
    } else {
        None
    }
}

fn infer_error_status(code_value: Option<&Value>, message: &str) -> u16 {
    let code = code_value
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let description = format!("{} {}", code, message.to_ascii_lowercase());
    if [
        "rate_limit",
        "rate limit",
        "too many requests",
        "resource_exhausted",
        "insufficient_quota",
        "quota exceeded",
    ]
    .iter()
    .any(|marker| description.contains(marker))
    {
        StatusCode::TOO_MANY_REQUESTS.as_u16()
    } else if [
        "invalid_api_key",
        "invalid api key",
        "authentication",
        "unauthorized",
        "api key",
    ]
    .iter()
    .any(|marker| description.contains(marker))
    {
        StatusCode::UNAUTHORIZED.as_u16()
    } else if ["forbidden", "permission denied", "access denied"]
        .iter()
        .any(|marker| description.contains(marker))
    {
        StatusCode::FORBIDDEN.as_u16()
    } else {
        0
    }
}

fn embedded_stream_error(text: &str) -> Option<FailureInfo> {
    for event in parse_sse_data_payloads(text) {
        if let Some(payload) = parse_json_safe(&event) {
            if let Some(err) = embedded_error(Some(&payload), &event) {
                return Some(err);
            }
        }
    }
    None
}

pub fn parse_sse_data_payloads(text: &str) -> Vec<String> {
    let mut events = Vec::new();
    let mut current = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                events.push(current.join("\n"));
                current.clear();
            }
        } else if let Some(data) = line.strip_prefix("data:") {
            let data = data.trim_start();
            if !data.is_empty() && data != "[DONE]" {
                current.push(data.to_string());
            }
        }
    }
    if !current.is_empty() {
        events.push(current.join("\n"));
    }
    events
}

fn validate_assistant_output(text: &str) -> Option<FailureInfo> {
    let payload = parse_json_safe(text)?;
    let assistant = assistant_text(&payload);
    let tool_calls = assistant_tool_calls(&payload);
    let reasoning = assistant_reasoning_text(&payload);
    if assistant.trim().is_empty()
        && tool_calls.is_empty()
        && reasoning.trim().is_empty()
        && !has_meaningful_responses_output(&payload)
    {
        return Some(FailureInfo {
            status: 502,
            message: "Upstream returned empty assistant output".to_string(),
            body: trim_error(text),
        });
    }
    None
}

fn has_meaningful_responses_output(payload: &Value) -> bool {
    payload
        .get("output")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(is_meaningful_responses_output_item))
}

fn is_meaningful_responses_output_item(item: &Value) -> bool {
    let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "function_call"
        | "computer_call"
        | "image_generation_call"
        | "file_search_call"
        | "web_search_call"
        | "code_interpreter_call" => true,
        "message" | "reasoning" => item
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| content.iter().any(is_meaningful_content_item)),
        _ => false,
    }
}

fn is_meaningful_content_item(item: &Value) -> bool {
    if item
        .get("text")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.trim().is_empty())
    {
        return true;
    }
    if item
        .get("refusal")
        .and_then(Value::as_str)
        .is_some_and(|text| !text.trim().is_empty())
    {
        return true;
    }
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("input_image" | "output_image" | "computer_screenshot")
    )
}

fn assistant_text(payload: &Value) -> String {
    if let Some(content) = payload.pointer("/choices/0/message/content") {
        if let Some(s) = content.as_str() {
            return s.to_string();
        }
        if let Some(arr) = content.as_array() {
            return arr
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .map(str::to_string)
                        .or_else(|| item.get("text").and_then(Value::as_str).map(str::to_string))
                        .or_else(|| {
                            item.get("content")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                })
                .collect::<Vec<_>>()
                .join("\n");
        }
    }
    payload
        .pointer("/choices/0/text")
        .and_then(Value::as_str)
        .or_else(|| payload.get("output_text").and_then(Value::as_str))
        .unwrap_or("")
        .to_string()
}

fn assistant_tool_calls(payload: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(arr) = payload
        .pointer("/choices/0/message/tool_calls")
        .and_then(Value::as_array)
    {
        out.extend(arr.clone());
    }
    if let Some(arr) = payload.get("tool_calls").and_then(Value::as_array) {
        out.extend(arr.clone());
    }
    if let Some(arr) = payload.get("output").and_then(Value::as_array) {
        out.extend(
            arr.iter()
                .filter(|item| {
                    item.get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|kind| kind.contains("tool") || kind.contains("function_call"))
                })
                .cloned(),
        );
    }
    out
}

fn assistant_reasoning_text(payload: &Value) -> String {
    let reasoning = payload
        .pointer("/choices/0/message/reasoning_content")
        .or_else(|| payload.pointer("/choices/0/message/reasoning"));
    if let Some(value) = reasoning {
        if let Some(s) = value.as_str() {
            return s.to_string();
        }
        if let Some(arr) = value.as_array() {
            return arr
                .iter()
                .filter_map(|item| {
                    item.as_str()
                        .map(str::to_string)
                        .or_else(|| item.get("text").and_then(Value::as_str).map(str::to_string))
                        .or_else(|| {
                            item.get("content")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        })
                })
                .collect::<Vec<_>>()
                .join("\n");
        }
    }
    String::new()
}

fn should_try_next(status_code: u16, cfg: &Config) -> bool {
    status_code >= 400 || cfg.failover_status_codes.contains(&status_code)
}

fn regular_attempt_limit(target: &TargetConfig) -> u32 {
    target.max_retries.saturating_add(1)
}

fn should_retry_target(
    failure: &FailureInfo,
    _cfg: &Config,
    attempt: u32,
    max_attempts: u32,
    _model: &ModelConfig,
) -> bool {
    if attempt >= max_attempts {
        return false;
    }
    if failure.status == 0 {
        return true;
    }
    failure.status >= 500 || failure.status == 408 || failure.status == 409
}

pub fn trim_error(text: &str) -> String {
    text.chars().take(1500).collect()
}

fn stream_probe_complete(text: &str) -> bool {
    text.contains("\n\n") || text.contains("\r\n\r\n") || text.len() >= stream_probe_bytes()
}

fn stream_probe_bytes() -> usize {
    std::env::var("STREAM_FAILURE_PROBE_KB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64)
        * 1024
}

fn send_error(status: StatusCode, message: &str, details: Option<Value>) -> Response {
    let body = match details {
        Some(details) => {
            json!({ "error": { "message": message, "type": "proxy_error", "details": details } })
        }
        None => json!({ "error": { "message": message, "type": "proxy_error" } }),
    };
    (status, Json(body)).into_response()
}

fn raw_response(
    status: StatusCode,
    response_type: String,
    text: String,
    proxy_headers: Option<(&str, &str)>,
) -> Response {
    let mut response = Response::new(Body::from(text));
    *response.status_mut() = status;
    let (target, model) = proxy_headers.unwrap_or(("", ""));
    insert_common_proxy_headers(response.headers_mut(), &response_type, target, model, false);
    response
}

fn insert_common_proxy_headers(
    headers: &mut HeaderMap,
    response_type: &str,
    target: &str,
    model: &str,
    stream: bool,
) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(response_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/json")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if stream {
            "no-cache, no-transform"
        } else {
            "no-cache"
        }),
    );
    headers.insert(
        "x-proxy-target",
        HeaderValue::from_str(target).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    headers.insert(
        "x-proxy-model",
        HeaderValue::from_str(model).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
}

fn format_attempt_error(item: &AttemptError) -> String {
    let suffix = item.attempt.map(|n| format!("#{}", n)).unwrap_or_default();
    let mut value = String::new();
    if let Some(status) = item.status {
        value.push_str(&format!("HTTP {}", status));
    }
    if !item.message.is_empty() {
        if !value.is_empty() {
            value.push_str(" - ");
        }
        value.push_str(&item.message);
    }
    if let Some(detail) = item.detail.as_deref().filter(|detail| !detail.is_empty()) {
        if !value.is_empty() {
            value.push_str("\n");
        }
        value.push_str(detail);
    }
    if value.is_empty() {
        value.push_str("unknown error");
    }
    format!(
        "{}{}: {}",
        if item.target.is_empty() {
            "target"
        } else {
            &item.target
        },
        suffix,
        value
    )
}

async fn log_request(
    stats: &StatsStore,
    model: &ModelConfig,
    requested_model: &str,
    failed_models: &[String],
    attempt_errors: &[AttemptError],
    final_model: &str,
    status: &str,
    started_at: u64,
    error: &str,
) {
    stats
        .add_log(LogEntry {
            chain_name: model.public_name.clone(),
            original_model: requested_model.to_string(),
            failed_models: failed_models.to_vec(),
            failed_model_errors: failed_model_error_entries(failed_models, attempt_errors, error),
            final_model: final_model.to_string(),
            status: status.to_string(),
            latency: now_ms().saturating_sub(started_at),
            error: error.to_string(),
            ..LogEntry::default()
        })
        .await;
}

fn failed_model_error_entries(
    failed_models: &[String],
    attempt_errors: &[AttemptError],
    fallback: &str,
) -> Vec<LogModelError> {
    let mut seen = HashSet::new();
    failed_models
        .iter()
        .filter(|model| seen.insert((*model).clone()))
        .map(|model| {
            let detail = attempt_errors
                .iter()
                .filter(|item| item.target == *model)
                .map(format_attempt_error)
                .collect::<Vec<_>>()
                .join("\n");
            LogModelError {
                model: model.clone(),
                error: if detail.is_empty() {
                    if fallback.is_empty() {
                        "unknown error".to_string()
                    } else {
                        fallback.to_string()
                    }
                } else {
                    detail
                },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use reqwest::Client;
    use std::{path::PathBuf, sync::Arc};
    use tokio::sync::RwLock;
    use uuid::Uuid;

    fn target() -> TargetConfig {
        TargetConfig {
            name: "upstream".to_string(),
            base_url: "http://example.com/v1".to_string(),
            api_key: "sk-test".to_string(),
            model_name: "real-model".to_string(),
            ..TargetConfig::default()
        }
    }

    async fn test_state() -> AppState {
        let dir =
            std::env::temp_dir().join(format!("failover-proxy-proxy-test-{}", Uuid::new_v4()));
        let stats_path = dir.join("stats.json");
        let logs_path = dir.join("request-logs.csv");
        let model_stats_path = dir.join("model-stats.csv");
        let runtime_stats_path = dir.join("runtime-stats.csv");
        let cfg = Config::default();
        let client = Client::new();
        AppState {
            config: Arc::new(RwLock::new(cfg.clone())),
            config_path: Arc::new(PathBuf::from("config.json")),
            runtime_stats_path: Arc::new(runtime_stats_path.clone()),
            stats: StatsStore::load(
                stats_path,
                logs_path,
                model_stats_path,
                runtime_stats_path,
                cfg.log_settings,
            )
            .await
            .expect("stats store"),
            circuit_breakers: crate::circuit::CircuitBreakers::default(),
            model_source: crate::model_source::ModelSourceService::new(client.clone()),
            provider_health: crate::model_source::ProviderHealthService::new(client.clone()),
            proxy_runtime: ProxyRuntime::default(),
            auth: crate::auth::AuthState::default(),
            client,
        }
    }

    fn model() -> ModelConfig {
        ModelConfig {
            public_name: "public-model".to_string(),
            targets: vec![target()],
            ..ModelConfig::default()
        }
    }

    async fn wait_for_abort_success_log(state: &AppState) -> crate::stats::Stats {
        for _ in 0..50 {
            let snapshot = state.stats.snapshot().await;
            if snapshot.successes == 1 && !snapshot.logs.is_empty() {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        state.stats.snapshot().await
    }

    #[test]
    fn chat_request_can_be_routed_to_responses_upstream() {
        let body = json!({
            "model": "public-model",
            "messages": [
                {"role": "system", "content": "be concise"},
                {"role": "user", "content": "hello"}
            ],
            "max_tokens": 12,
            "stream": true
        });
        let converted = build_upstream_body(
            &body,
            &target(),
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::Responses,
            false,
        );
        assert_eq!(converted["model"], "real-model");
        assert_eq!(converted["stream"], false);
        assert_eq!(converted["instructions"], "be concise");
        assert_eq!(converted["max_output_tokens"], 12);
        assert_eq!(converted["input"][0]["role"], "user");
        assert_eq!(converted["input"][0]["content"], "hello");
    }

    #[test]
    fn chat_image_request_uses_responses_image_shape() {
        let body = json!({
            "model": "public-model",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this image"},
                    {"type": "image_url", "image_url": {
                        "url": "data:image/png;base64,AAAA",
                        "detail": "high"
                    }}
                ]
            }]
        });

        let converted = build_upstream_body(
            &body,
            &target(),
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::Responses,
            false,
        );
        let image = &converted["input"][0]["content"][1];

        assert_eq!(image["type"], "input_image");
        assert_eq!(image["image_url"], "data:image/png;base64,AAAA");
        assert_eq!(image["detail"], "high");
    }

    #[test]
    fn context_length_error_requires_422_and_context_marker() {
        assert!(is_context_length_error(
            422,
            r#"{"error":{"message":"maximum context length exceeded"}}"#
        ));
        assert!(!is_context_length_error(
            400,
            "maximum context length exceeded"
        ));
        assert!(!is_context_length_error(422, "field validation failed"));
    }

    #[test]
    fn embedded_rate_limit_code_is_classified_as_429() {
        let failure = classify_upstream_failure(
            200,
            r#"{"error":{"code":"rate_limit_exceeded","message":"Please retry later"}}"#,
            true,
            true,
        )
        .expect("embedded error must fail the attempt");

        assert_eq!(failure.status, 429);
        assert_eq!(failure.message, "Please retry later");
    }

    #[test]
    fn provider_style_code_and_msg_errors_are_classified() {
        let failure = classify_upstream_failure(
            200,
            r#"{"code":429,"msg":"rate limit reached"}"#,
            true,
            true,
        )
        .expect("provider error must fail the attempt");

        assert_eq!(failure.status, 429);
        assert_eq!(failure.message, "rate limit reached");

        let failure = classify_upstream_failure(
            200,
            r#"{"success":false,"code":"invalid_api_key","message":"key rejected"}"#,
            true,
            true,
        )
        .expect("provider error must fail the attempt");

        assert_eq!(failure.status, 401);
        assert_eq!(failure.message, "key rejected");
    }

    #[test]
    fn responses_message_content_is_not_mistaken_for_empty_output() {
        let response = r#"{
            "object":"response",
            "output":[{
                "type":"message",
                "content":[{"type":"output_text","text":"正常结果"}]
            }]
        }"#;

        assert!(classify_upstream_failure(200, response, true, true).is_none());
    }

    #[test]
    fn genuinely_empty_success_response_still_fails_validation() {
        let failure = classify_upstream_failure(
            200,
            r#"{"object":"response","output":[{"type":"message","content":[]}]}"#,
            true,
            true,
        )
        .expect("empty response must fail validation");

        assert_eq!(failure.status, 502);
    }

    #[test]
    fn regular_attempt_limit_excludes_context_compression_retries() {
        let no_retry = TargetConfig {
            max_retries: 0,
            ..TargetConfig::default()
        };
        let two_retries = TargetConfig {
            max_retries: 2,
            ..TargetConfig::default()
        };

        assert_eq!(regular_attempt_limit(&no_retry), 1);
        assert_eq!(regular_attempt_limit(&two_retries), 3);
    }

    #[test]
    fn context_422_compacts_chat_history_before_retrying() {
        let body = json!({
            "model": "public-model",
            "max_tokens": 64,
            "messages": [
                {"role": "system", "content": "Always answer in Chinese."},
                {"role": "user", "content": "old-user-".repeat(180)},
                {"role": "assistant", "content": "old assistant response"},
                {"role": "user", "content": "What is the final answer?"}
            ]
        });
        let prepared = compact_request_context(&body, ProxyEndpoint::ChatCompletions)
            .expect("request can be compacted after an upstream 422");
        let messages = prepared["messages"].as_array().expect("messages array");

        assert!(estimate_json_tokens(&prepared) < estimate_json_tokens(&body));
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(
            messages.last().unwrap()["content"],
            "What is the final answer?"
        );
        assert!(messages.iter().all(|message| {
            message_role(message) != "user" || value_to_text(message).len() < 400
        }));
    }

    #[test]
    fn context_422_never_compacts_multimodal_input() {
        let body = json!({
            "model": "public-model",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "A very long question"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
                ]
            }]
        });

        assert!(compact_request_context(&body, ProxyEndpoint::ChatCompletions).is_none());
    }

    #[test]
    fn data_uri_image_is_not_estimated_as_text_context() {
        let body = json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "image_url",
                    "image_url": {"url": format!("data:image/png;base64,{}", "A".repeat(100_000))}
                }]
            }]
        });

        assert!(estimate_json_tokens(&body) < 10_000);
    }

    #[test]
    fn context_422_compacts_responses_input_before_retrying() {
        let body = json!({
            "model": "public-model",
            "max_output_tokens": 64,
            "instructions": "Preserve the latest answer request.",
            "input": "older context ".repeat(300)
        });
        let prepared = compact_request_context(&body, ProxyEndpoint::Responses)
            .expect("request can be compacted after an upstream 422");

        assert!(estimate_json_tokens(&prepared) < estimate_json_tokens(&body));
        assert!(prepared["input"]
            .as_str()
            .expect("compacted input")
            .contains("Failover Proxy compacted earlier context"));
    }

    #[test]
    fn responses_request_can_be_routed_to_chat_upstream() {
        let body = json!({
            "model": "public-model",
            "instructions": "answer in json",
            "input": "ping",
            "max_output_tokens": 8
        });
        let converted = build_upstream_body(
            &body,
            &target(),
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
            false,
        );
        assert_eq!(converted["model"], "real-model");
        assert_eq!(converted["messages"][0]["role"], "system");
        assert_eq!(converted["messages"][0]["content"], "answer in json");
        assert_eq!(converted["messages"][1]["role"], "user");
        assert_eq!(converted["messages"][1]["content"], "ping");
        assert_eq!(converted["max_tokens"], 8);
    }

    #[test]
    fn responses_image_request_uses_chat_image_shape() {
        let body = json!({
            "model": "public-model",
            "input": [{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "Describe this image"},
                    {"type": "input_image", "image_url": "data:image/png;base64,AAAA", "detail": "low"}
                ]
            }]
        });

        let converted = build_upstream_body(
            &body,
            &target(),
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
            false,
        );
        let image = &converted["messages"][0]["content"][1];

        assert_eq!(image["type"], "image_url");
        assert_eq!(image["image_url"]["url"], "data:image/png;base64,AAAA");
        assert_eq!(image["image_url"]["detail"], "low");
    }

    #[test]
    fn responses_tools_and_tool_history_are_converted_for_chat_upstream() {
        let body = json!({
            "model": "public-model",
            "input": [
                {"role": "user", "content": [{"type": "input_text", "text": "list files"}]},
                {"type": "function_call", "call_id": "call_1", "name": "shell", "arguments": "{\"command\":\"dir\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "Cargo.toml"}
            ],
            "tools": [{
                "type": "function",
                "name": "shell",
                "description": "Run a command",
                "parameters": {"type": "object"},
                "strict": true
            }],
            "tool_choice": {"type": "function", "name": "shell"}
        });

        let converted = build_upstream_body(
            &body,
            &target(),
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
            false,
        );

        assert_eq!(converted["tools"][0]["type"], "function");
        assert_eq!(converted["tools"][0]["function"]["name"], "shell");
        assert_eq!(converted["tool_choice"]["function"]["name"], "shell");
        assert_eq!(converted["messages"][0]["content"][0]["type"], "text");
        assert_eq!(converted["messages"][1]["role"], "assistant");
        assert_eq!(converted["messages"][1]["tool_calls"][0]["id"], "call_1");
        assert_eq!(converted["messages"][2]["role"], "tool");
        assert_eq!(converted["messages"][2]["tool_call_id"], "call_1");
        assert_eq!(converted["messages"][2]["content"], "Cargo.toml");
    }

    #[test]
    fn chat_tool_calls_are_returned_as_responses_function_calls() {
        let upstream = json!({
            "id": "chatcmpl_1",
            "created": 123,
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "shell", "arguments": "{\"command\":\"dir\"}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }]
        });

        let converted = response_payload_as(
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
            &upstream,
            "public-model",
            &target(),
        );

        assert_eq!(converted["object"], "response");
        assert_eq!(converted["output"][0]["type"], "function_call");
        assert_eq!(converted["output"][0]["call_id"], "call_1");
        assert_eq!(converted["output"][0]["name"], "shell");
        assert_eq!(converted["output"][0]["arguments"], "{\"command\":\"dir\"}");
    }

    #[test]
    fn fallback_responses_stream_contains_lifecycle_events() {
        let payload = json!({
            "id": "resp_1",
            "object": "response",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "pong", "annotations": []}]
            }]
        });

        let stream = synthetic_sse_text(ProxyEndpoint::Responses, &payload);
        assert!(stream.contains("event: response.created"));
        assert!(stream.contains("event: response.output_item.added"));
        assert!(stream.contains("event: response.output_text.delta"));
        assert!(stream.contains("event: response.output_item.done"));
        assert!(stream.contains("event: response.completed"));
        assert!(stream.ends_with("data: [DONE]\n\n"));
    }

    #[test]
    fn chat_stream_is_translated_to_responses_incrementally() {
        let mut transformer = StreamProtocolTransformer::new(
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
        );

        assert!(transformer
            .transform_chunk(&Bytes::from_static(
                b"data: {\"id\":\"chatcmpl_1\",\"created\":123,\"model\":\"vision-model\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}"
            ))
            .is_empty());
        let output = transformer.transform_chunk(&Bytes::from_static(
            b"\n\ndata: {\"id\":\"chatcmpl_1\",\"created\":123,\"model\":\"vision-model\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        ));
        let output = String::from_utf8_lossy(&output);

        assert!(output.contains("event: response.created"));
        assert!(output.contains("event: response.output_text.delta"));
        assert!(output.contains("\"delta\":\"hello\""));
        assert!(output.contains("event: response.completed"));
        assert!(output.contains("data: [DONE]"));
    }

    #[test]
    fn responses_stream_is_translated_to_chat_incrementally() {
        let mut transformer = StreamProtocolTransformer::new(
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::Responses,
        );
        let output = transformer.transform_chunk(&Bytes::from_static(
            b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
        ));
        let output = String::from_utf8_lossy(&output);
        assert!(output.contains("\"content\":\"hello\""));
        assert!(!output.contains("[DONE]"));

        let output = transformer.transform_chunk(&Bytes::from_static(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[]}}\n\n",
        ));
        let output = String::from_utf8_lossy(&output);
        assert!(output.contains("\"finish_reason\":\"stop\""));
        assert!(output.contains("data: [DONE]"));
    }

    #[test]
    fn completions_response_can_be_returned_as_chat_response() {
        let upstream = json!({
            "id": "cmpl-1",
            "object": "text_completion",
            "created": 123,
            "model": "real-model",
            "choices": [{"text": "pong", "finish_reason": "stop"}],
            "usage": {"total_tokens": 3}
        });
        let converted = response_payload_as(
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::Completions,
            &upstream,
            "public-model",
            &target(),
        );
        assert_eq!(converted["object"], "chat.completion");
        assert_eq!(converted["model"], "public-model");
        assert_eq!(converted["choices"][0]["message"]["content"], "pong");
        assert_eq!(converted["usage"]["total_tokens"], 3);
    }

    #[test]
    fn responses_response_can_be_returned_as_completions_response() {
        let upstream = json!({
            "id": "resp-1",
            "created_at": 456,
            "output_text": "done",
            "usage": {"output_tokens": 1}
        });
        let converted = response_payload_as(
            ProxyEndpoint::Completions,
            ProxyEndpoint::Responses,
            &upstream,
            "public-model",
            &target(),
        );
        assert_eq!(converted["object"], "text_completion");
        assert_eq!(converted["choices"][0]["text"], "done");
        assert_eq!(converted["created"], 456);
    }

    #[test]
    fn failed_model_error_entries_attach_errors_to_each_failed_model() {
        let errors = vec![
            AttemptError {
                target: "model-a".to_string(),
                attempt: Some(1),
                status: Some(429),
                message: "rate limited".to_string(),
                detail: Some("{\"error\":\"quota\"}".to_string()),
            },
            AttemptError {
                target: "model-b".to_string(),
                attempt: Some(1),
                message: "timeout".to_string(),
                ..AttemptError::default()
            },
        ];

        let entries = failed_model_error_entries(
            &["model-a".to_string(), "model-b".to_string()],
            &errors,
            "",
        );

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].model, "model-a");
        assert!(entries[0].error.contains("HTTP 429"));
        assert!(entries[0].error.contains("quota"));
        assert_eq!(entries[1].model, "model-b");
        assert!(entries[1].error.contains("timeout"));
    }

    #[test]
    fn stream_completion_detector_finds_done_marker() {
        let mut detector = StreamCompletionDetector::default();

        assert!(!detector.observe(&Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
        )));
        assert!(detector.observe(&Bytes::from_static(b"data: [DONE]\n\n")));
    }

    #[test]
    fn stream_completion_detector_handles_split_done_marker() {
        let mut detector = StreamCompletionDetector::default();

        assert!(!detector.observe(&Bytes::from_static(b"data: [DO")));
        assert!(detector.observe(&Bytes::from_static(b"NE]\n\n")));
    }

    #[test]
    fn stream_completion_detector_recognizes_responses_and_chat_completion_events() {
        let mut responses = StreamCompletionDetector::default();
        assert!(responses.observe(&Bytes::from_static(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{}}\n\n"
        )));

        let mut chat = StreamCompletionDetector::default();
        assert!(chat.observe(&Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
        )));
    }

    #[test]
    fn target_api_keys_round_robin_across_requests() {
        let runtime = ProxyRuntime::default();
        let model = model();
        let mut target = target();
        target.api_key = "sk-a".to_string();
        target.api_keys = vec!["sk-a".to_string(), "sk-b".to_string()];
        target.api_key_mode = ApiKeyMode::RoundRobin;

        assert_eq!(
            runtime.select_target_api_key(&model, &target),
            Some("sk-a".to_string())
        );
        assert_eq!(
            runtime.select_target_api_key(&model, &target),
            Some("sk-b".to_string())
        );
        assert_eq!(
            runtime.select_target_api_key(&model, &target),
            Some("sk-a".to_string())
        );
    }

    #[test]
    fn runtime_cleanup_removes_stale_round_robin_entries() {
        let runtime = ProxyRuntime::default();
        let model = model();
        let target = target();
        let api_key_cursor = format!("api-key:{}", target_key(&model, &target));
        runtime
            .round_robin
            .insert(model.public_name.clone(), AtomicU64::new(0));
        runtime
            .round_robin
            .insert(api_key_cursor.clone(), AtomicU64::new(0));
        runtime
            .round_robin
            .insert("removed-model".to_string(), AtomicU64::new(0));
        runtime.retain_round_robin_models(&[model]);
        assert!(runtime.round_robin.contains_key("public-model"));
        assert!(runtime.round_robin.contains_key(&api_key_cursor));
        assert!(!runtime.round_robin.contains_key("removed-model"));
        assert_eq!(runtime.round_robin_memory_usage().entries, 2);
    }

    #[tokio::test]
    async fn stream_abort_guard_releases_active_thread_immediately() {
        let state = test_state().await;
        let model = model();
        let slot = state.proxy_runtime.acquire(&model, "public-model").await;
        let thread_id = slot.thread_id.as_ref().expect("thread id").clone();

        let guard = StreamAbortGuard::new(
            state.clone(),
            model,
            thread_id.clone(),
            "public-model".to_string(),
            Vec::new(),
            Vec::new(),
            "upstream|real-model".to_string(),
            now_ms(),
        );

        assert_eq!(state.proxy_runtime.snapshot_threads().len(), 1);
        drop(guard);
        assert!(state.proxy_runtime.snapshot_threads().is_empty());
        drop(slot);
        assert!(state.proxy_runtime.snapshot_threads().is_empty());

        let stats = wait_for_abort_success_log(&state).await;
        assert_eq!(stats.successes, 1);
        assert_eq!(stats.failures, 0);
        assert_eq!(stats.logs[0].status, "success");
        assert_eq!(stats.logs[0].final_model, "upstream|real-model");
        assert!(stats.logs[0]
            .error
            .contains("客户端在流式响应完成前断开连接"));
        assert!(stats.logs[0].failed_models.is_empty());
    }

    #[tokio::test]
    async fn stream_continues_after_first_chunk_without_applying_target_timeout() {
        let state = test_state().await;
        let model = model();
        let mut target = target();
        target.timeout_ms = 5;
        let slot = state.proxy_runtime.acquire(&model, "public-model").await;
        let delayed_stream: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>> =
            Box::pin(async_stream::stream! {
                tokio::time::sleep(Duration::from_millis(30)).await;
                yield Ok::<Bytes, reqwest::Error>(Bytes::from_static(b"data: [DONE]\n\n"));
            });
        let inspected = StreamInspection {
            chunks: vec![Bytes::from_static(b"data: {\"delta\":\"hello\"}\n\n")],
            stream: Some(delayed_stream),
            failure: None,
        };

        let response = stream_response(
            state.clone(),
            model,
            target,
            slot.thread_id.as_ref().expect("thread id").clone(),
            "public-model".to_string(),
            Vec::new(),
            Vec::new(),
            "upstream|real-model".to_string(),
            now_ms(),
            now_ms(),
            StatusCode::OK,
            "text/event-stream".to_string(),
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::ChatCompletions,
            inspected,
            false,
            slot.into_stream_guard(),
        );

        let body = response
            .into_body()
            .collect()
            .await
            .expect("stream body")
            .to_bytes();
        assert_eq!(
            body,
            Bytes::from_static(b"data: {\"delta\":\"hello\"}\n\ndata: [DONE]\n\n")
        );
        assert_eq!(state.stats.snapshot().await.successes, 1);
    }

    #[tokio::test]
    async fn stream_response_translates_chat_events_for_responses_clients() {
        let state = test_state().await;
        let model = model();
        let target = target();
        let slot = state.proxy_runtime.acquire(&model, "public-model").await;
        let inspected = StreamInspection {
            chunks: vec![
                Bytes::from_static(
                    b"data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n",
                ),
                Bytes::from_static(
                    b"data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                ),
            ],
            stream: None,
            failure: None,
        };

        let response = stream_response(
            state,
            model,
            target,
            slot.thread_id.as_ref().expect("thread id").clone(),
            "public-model".to_string(),
            Vec::new(),
            Vec::new(),
            "upstream|real-model".to_string(),
            now_ms(),
            now_ms(),
            StatusCode::OK,
            "text/event-stream".to_string(),
            ProxyEndpoint::Responses,
            ProxyEndpoint::ChatCompletions,
            inspected,
            false,
            slot.into_stream_guard(),
        );

        let body = response
            .into_body()
            .collect()
            .await
            .expect("stream body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("event: response.output_text.delta"));
        assert!(body.contains("\"delta\":\"hello\""));
        assert!(body.contains("event: response.completed"));
        assert!(body.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn dropping_stream_body_after_first_chunk_releases_thread() {
        let state = test_state().await;
        let model = model();
        let target = target();
        let slot = state.proxy_runtime.acquire(&model, "public-model").await;
        let inspected = StreamInspection {
            chunks: vec![Bytes::from_static(b"data: {\"delta\":\"hello\"}\n\n")],
            stream: None,
            failure: None,
        };

        let response = stream_response(
            state.clone(),
            model,
            target,
            slot.thread_id.as_ref().expect("thread id").clone(),
            "public-model".to_string(),
            Vec::new(),
            Vec::new(),
            "upstream|real-model".to_string(),
            now_ms(),
            now_ms(),
            StatusCode::OK,
            "text/event-stream".to_string(),
            ProxyEndpoint::ChatCompletions,
            ProxyEndpoint::ChatCompletions,
            inspected,
            false,
            slot.into_stream_guard(),
        );
        let mut body = response.into_body();

        let first = body.frame().await.expect("first frame").expect("frame");
        assert_eq!(
            first.into_data().expect("data"),
            Bytes::from_static(b"data: {\"delta\":\"hello\"}\n\n")
        );
        drop(body);

        assert!(state.proxy_runtime.snapshot_threads().is_empty());

        let stats = wait_for_abort_success_log(&state).await;
        assert_eq!(stats.successes, 1);
        assert_eq!(stats.failures, 0);
        assert_eq!(stats.logs[0].status, "success");
        assert_eq!(stats.logs[0].final_model, "upstream|real-model");
        assert!(stats.logs[0]
            .error
            .contains("客户端在流式响应完成前断开连接"));
    }
}
