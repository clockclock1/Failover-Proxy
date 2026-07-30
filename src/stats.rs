use crate::config::{
    channel_label, channel_model_display_label, model_circuit, normalize_log_settings, target_key,
    Config, LogSettingsConfig, ModelConfig, TargetConfig,
};
use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    hash::Hash,
    io::ErrorKind,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    fs,
    sync::{Mutex, RwLock},
};
use uuid::Uuid;

const REQUEST_LOG_LIMIT: usize = 500;
const REQUEST_LOGS_CSV_HEADER: &str =
    "id,timestamp,chainName,originalModel,failedModels,failedModelErrors,finalModel,status,latency,error\n";
const MODEL_STATS_CSV_HEADER: &str =
    "channel,baseUrl,model,requests,successes,failures,lastStatus,lastError,lastLatencyMs,updatedAt\n";
const RUNTIME_STATS_CSV_HEADER: &str =
    "kind,key,model,target,upstreamModel,baseUrl,requests,successes,failures,failovers,ok,error,consecutiveFailures,disabledUntil,lastStatus,lastError,lastLatencyMs,avgLatencyMs,updatedAt\n";
pub const HISTORICAL_STATS_RETENTION_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeMemoryUsage {
    pub entries: usize,
    pub content_bytes: usize,
    pub hash_bucket_bytes: usize,
    pub lock_and_shard_bytes: usize,
    pub container_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<usize>,
    pub estimated_bytes: usize,
}

impl RuntimeMemoryUsage {
    pub fn finish(&mut self) {
        self.estimated_bytes = self
            .content_bytes
            .saturating_add(self.hash_bucket_bytes)
            .saturating_add(self.lock_and_shard_bytes)
            .saturating_add(self.container_bytes);
    }
}

/// Reports DashMap's actual reserved slots and fixed sharded-lock storage.
/// `raw-api` is enabled solely for this read-only accounting path.
pub fn dashmap_memory_overhead<K, V>(map: &DashMap<K, V>) -> RuntimeMemoryUsage
where
    K: Eq + Hash,
{
    let shards = map.shards();
    let capacity = shards
        .iter()
        .map(|shard| shard.read().capacity())
        .sum::<usize>();
    let mut usage = RuntimeMemoryUsage {
        capacity: Some(capacity),
        // One control byte per hashbrown slot; allocator rounding is reported
        // in the process-level remainder, not faked as exact table data.
        hash_bucket_bytes: capacity
            .saturating_mul(std::mem::size_of::<(K, V)>().saturating_add(1)),
        lock_and_shard_bytes: std::mem::size_of_val(shards),
        ..RuntimeMemoryUsage::default()
    };
    usage.finish();
    usage
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Stats {
    pub started_at: String,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub failovers: u64,
    pub targets: BTreeMap<String, TargetStats>,
    pub chains: BTreeMap<String, ChainStats>,
    pub channel_models: BTreeMap<String, ChannelModelStats>,
    pub logs: Vec<LogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct TargetStats {
    pub model: String,
    pub target: String,
    pub upstream_model: String,
    pub base_url: String,
    pub ok: u64,
    pub error: u64,
    pub consecutive_failures: u32,
    pub disabled_until: u64,
    pub last_status: u16,
    pub last_error: String,
    pub last_latency_ms: u64,
    pub avg_latency_ms: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ChainStats {
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub failovers: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelModelStats {
    pub name: String,
    pub base_url: String,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub models: BTreeMap<String, ChannelModelItemStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct ChannelModelItemStats {
    pub name: String,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_status: u16,
    pub last_error: String,
    pub last_latency_ms: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LogEntry {
    pub id: String,
    pub timestamp: u64,
    pub chain_name: String,
    pub original_model: String,
    pub failed_models: Vec<String>,
    pub failed_model_errors: Vec<LogModelError>,
    pub final_model: String,
    pub status: String,
    pub latency: u64,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
pub struct LogModelError {
    pub model: String,
    pub error: String,
}

#[derive(Default)]
struct RuntimeStatsSnapshot {
    requests: u64,
    successes: u64,
    failures: u64,
    failovers: u64,
    chains: BTreeMap<String, ChainStats>,
    targets: BTreeMap<String, TargetStats>,
}

#[derive(Clone)]
pub struct StatsStore {
    inner: Arc<RwLock<Stats>>,
    logs_path: Arc<PathBuf>,
    model_stats_path: Arc<PathBuf>,
    runtime_stats_path: Arc<PathBuf>,
    log_settings: Arc<RwLock<LogSettingsConfig>>,
    save_lock: Arc<Mutex<()>>,
    save_queued: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Default)]
pub struct FailureInfo {
    pub status: u16,
    pub message: String,
    pub body: String,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            started_at: chrono_like_now(),
            requests: 0,
            successes: 0,
            failures: 0,
            failovers: 0,
            targets: BTreeMap::new(),
            chains: BTreeMap::new(),
            channel_models: BTreeMap::new(),
            logs: Vec::new(),
        }
    }
}

impl Default for LogEntry {
    fn default() -> Self {
        Self {
            id: String::new(),
            timestamp: 0,
            chain_name: String::new(),
            original_model: String::new(),
            failed_models: Vec::new(),
            failed_model_errors: Vec::new(),
            final_model: String::new(),
            status: String::new(),
            latency: 0,
            error: String::new(),
        }
    }
}

impl StatsStore {
    pub async fn load(
        legacy_stats_path: PathBuf,
        logs_path: PathBuf,
        model_stats_path: PathBuf,
        runtime_stats_path: PathBuf,
        log_settings: LogSettingsConfig,
    ) -> Result<Self> {
        if let Some(parent) = runtime_stats_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let runtime_stats = load_runtime_stats_csv(&runtime_stats_path).await?;
        let legacy_stats = if runtime_stats.is_some() {
            None
        } else {
            match fs::read_to_string(&legacy_stats_path).await {
                Ok(text) => Some(
                    serde_json::from_str::<Stats>(&text)
                        .unwrap_or_default()
                        .normalize(),
                ),
                Err(err) if err.kind() == ErrorKind::NotFound => None,
                Err(err) => return Err(err.into()),
            }
        };
        let mut stats = legacy_stats.clone().unwrap_or_default();
        if let Ok(logs) = load_request_logs_csv(&logs_path).await {
            stats.logs = merge_logs(logs, legacy_stats.as_ref().map(|item| item.logs.clone()));
        }
        if let Ok(channel_models) = load_model_stats_csv(&model_stats_path).await {
            stats.channel_models = merge_channel_models(
                channel_models,
                legacy_stats
                    .as_ref()
                    .map(|item| item.channel_models.clone()),
            );
        }
        if let Some(runtime_stats) = runtime_stats {
            stats.requests = runtime_stats.requests;
            stats.successes = runtime_stats.successes;
            stats.failures = runtime_stats.failures;
            stats.failovers = runtime_stats.failovers;
            stats.chains = runtime_stats.chains;
            stats.targets = runtime_stats.targets;
        } else if legacy_stats.is_none() {
            rebuild_chain_totals_from_logs(&mut stats);
        }
        let log_settings = normalize_log_settings(log_settings);
        apply_log_limits(&mut stats.logs, &log_settings);
        let store = Self {
            inner: Arc::new(RwLock::new(stats)),
            logs_path: Arc::new(logs_path),
            model_stats_path: Arc::new(model_stats_path),
            runtime_stats_path: Arc::new(runtime_stats_path),
            log_settings: Arc::new(RwLock::new(log_settings)),
            save_lock: Arc::new(Mutex::new(())),
            save_queued: Arc::new(AtomicBool::new(false)),
        };
        store.save_now().await?;
        Ok(store)
    }

    pub async fn snapshot(&self) -> Stats {
        self.inner.read().await.clone().normalize()
    }

    pub async fn mutate<F>(&self, f: F)
    where
        F: FnOnce(&mut Stats),
    {
        let settings = self.log_settings.read().await.clone();
        let mut stats = self.inner.write().await;
        f(&mut stats);
        apply_log_limits(&mut stats.logs, &settings);
    }

    pub async fn save_now(&self) -> Result<()> {
        let _save_guard = self.save_lock.lock().await;
        let snapshot = self.snapshot().await;
        save_request_logs_csv(&self.logs_path, &snapshot.logs).await?;
        save_model_stats_csv(&self.model_stats_path, &snapshot.channel_models).await?;
        save_runtime_stats_csv(&self.runtime_stats_path, &snapshot).await
    }

    pub fn spawn_periodic_save(&self) {
        let store = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(err) = store.save_now().await {
                    tracing::warn!(error = %err, "cannot save stats");
                }
            }
        });
    }

    fn schedule_save_soon(&self) {
        if self
            .save_queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let store = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            if let Err(err) = store.save_now().await {
                tracing::warn!(error = %err, "cannot save stats");
            }
            store.save_queued.store(false, Ordering::Release);
        });
    }

    pub async fn record_target(
        &self,
        model: &ModelConfig,
        target: &TargetConfig,
        ok: bool,
        cfg: &Config,
        failure: FailureInfo,
        latency_ms: u64,
        disabled_until: u64,
        consecutive_failures: u32,
    ) {
        let key = target_key(model, target);
        let breaker = model_circuit(model, cfg);
        self.mutate(|stats| {
            let item = stats
                .targets
                .entry(key.clone())
                .or_insert_with(|| TargetStats {
                    model: model.public_name.clone(),
                    target: if !target.model_name.is_empty() {
                        target.model_name.clone()
                    } else if !target.name.is_empty() {
                        target.name.clone()
                    } else {
                        target.base_url.clone()
                    },
                    upstream_model: target.model_name.clone(),
                    base_url: target.base_url.clone(),
                    ..TargetStats::default()
                });
            let measured = latency_ms;
            if measured > 0 {
                item.last_latency_ms = measured;
                item.avg_latency_ms = if item.avg_latency_ms > 0 {
                    ((item.avg_latency_ms as f64 * 0.8) + (measured as f64 * 0.2)).round() as u64
                } else {
                    measured
                };
            }
            if ok {
                item.ok += 1;
                item.consecutive_failures = 0;
                item.disabled_until = 0;
                item.last_status = 0;
                item.last_error.clear();
            } else {
                item.error += 1;
                item.consecutive_failures = consecutive_failures
                    .max(1)
                    .max(if breaker.failure_threshold == 0 { 1 } else { 0 });
                item.disabled_until = disabled_until;
                item.last_status = failure.status;
                item.last_error = if failure.message.is_empty() {
                    trim_persisted_string(&failure.body)
                } else {
                    trim_persisted_string(&failure.message)
                };
            }
            item.updated_at = now_ms();
            record_channel(stats, target, ok, &failure, measured);
        })
        .await;
    }

    pub async fn clear_target_breaker_stats(&self, model: &ModelConfig, target: &TargetConfig) {
        let key = target_key(model, target);
        self.mutate(|stats| {
            if let Some(item) = stats.targets.get_mut(&key) {
                item.consecutive_failures = 0;
                item.disabled_until = 0;
                item.updated_at = now_ms();
            }
        })
        .await;
    }

    pub async fn chain_request(&self, public_name: &str) {
        let key = public_name.to_string();
        self.mutate(|stats| {
            stats.requests += 1;
            let chain = stats.chains.entry(key).or_default();
            chain.requests += 1;
            chain.updated_at = now_ms();
        })
        .await;
    }

    pub async fn chain_success(&self, public_name: &str, failover: bool) {
        let key = public_name.to_string();
        self.mutate(|stats| {
            stats.successes += 1;
            let chain = stats.chains.entry(key).or_default();
            chain.successes += 1;
            chain.updated_at = now_ms();
            if failover {
                stats.failovers += 1;
                chain.failovers += 1;
            }
        })
        .await;
    }

    pub async fn chain_failure(&self, public_name: &str) {
        let key = public_name.to_string();
        self.mutate(|stats| {
            stats.failures += 1;
            let chain = stats.chains.entry(key).or_default();
            chain.failures += 1;
            chain.updated_at = now_ms();
        })
        .await;
    }

    pub async fn add_log(&self, mut entry: LogEntry) {
        if entry.id.is_empty() {
            entry.id = Uuid::new_v4().to_string();
        }
        if entry.timestamp == 0 {
            entry.timestamp = now_ms();
        }
        let settings = self.log_settings.read().await.clone();
        entry.error = trim_string_chars(&entry.error, settings.max_error_chars);
        for item in &mut entry.failed_model_errors {
            item.error = trim_string_chars(&item.error, settings.max_error_chars);
        }
        self.mutate(|stats| {
            push_request_log(&mut stats.logs, entry, &settings);
        })
        .await;
        self.schedule_save_soon();
    }

    pub async fn clear_logs(&self) -> Result<()> {
        self.mutate(|stats| {
            stats.logs.clear();
            stats.logs.shrink_to_fit();
        })
        .await;
        self.save_now().await
    }

    pub async fn apply_log_settings(
        &self,
        settings: LogSettingsConfig,
    ) -> Result<LogSettingsConfig> {
        let normalized = normalize_log_settings(settings);
        {
            let mut guard = self.log_settings.write().await;
            *guard = normalized.clone();
        }
        self.mutate(|stats| {
            apply_log_limits(&mut stats.logs, &normalized);
        })
        .await;
        self.save_now().await?;
        Ok(normalized)
    }

    pub async fn log_settings(&self) -> LogSettingsConfig {
        self.log_settings.read().await.clone()
    }

    pub fn logs_path(&self) -> PathBuf {
        (*self.logs_path).clone()
    }

    pub fn model_stats_path(&self) -> PathBuf {
        (*self.model_stats_path).clone()
    }

    pub async fn avg_latency(&self, model: &ModelConfig, target: &TargetConfig) -> u64 {
        let key = target_key(model, target);
        self.inner
            .read()
            .await
            .targets
            .get(&key)
            .map(|item| item.avg_latency_ms)
            .unwrap_or(0)
    }

    pub async fn model_statistics_memory_usage(&self) -> RuntimeMemoryUsage {
        let stats = self.inner.read().await;
        let mut usage = RuntimeMemoryUsage {
            lock_and_shard_bytes: std::mem::size_of::<RwLock<Stats>>(),
            ..RuntimeMemoryUsage::default()
        };
        for (name, item) in &stats.chains {
            usage.entries += 1;
            usage.content_bytes += std::mem::size_of_val(item) + name.capacity();
        }
        for (key, item) in &stats.targets {
            usage.entries += 1;
            usage.content_bytes += std::mem::size_of_val(item)
                + key.capacity()
                + item.model.capacity()
                + item.target.capacity()
                + item.upstream_model.capacity()
                + item.base_url.capacity()
                + item.last_error.capacity();
        }
        for (channel_name, channel) in &stats.channel_models {
            usage.content_bytes += std::mem::size_of_val(channel)
                + channel_name.capacity()
                + channel.name.capacity()
                + channel.base_url.capacity();
            for (model_name, item) in &channel.models {
                usage.entries += 1;
                usage.content_bytes += std::mem::size_of_val(item)
                    + model_name.capacity()
                    + item.name.capacity()
                    + item.last_error.capacity();
            }
        }
        usage.finish();
        usage
    }

    pub async fn request_log_memory_usage(&self) -> RuntimeMemoryUsage {
        let stats = self.inner.read().await;
        let mut usage = RuntimeMemoryUsage {
            entries: stats.logs.len(),
            container_bytes: stats.logs.capacity() * std::mem::size_of::<LogEntry>(),
            ..RuntimeMemoryUsage::default()
        };
        for log in &stats.logs {
            usage.content_bytes += log.id.capacity()
                + log.chain_name.capacity()
                + log.original_model.capacity()
                + log.final_model.capacity()
                + log.status.capacity()
                + log.error.capacity()
                + log.failed_models.capacity() * std::mem::size_of::<String>()
                + log.failed_models.iter().map(String::capacity).sum::<usize>()
                + log.failed_model_errors.capacity() * std::mem::size_of::<LogModelError>();
            for error in &log.failed_model_errors {
                usage.content_bytes += error.model.capacity() + error.error.capacity();
            }
        }
        usage.finish();
        usage
    }

    pub async fn cleanup_runtime_models(&self, models: &[ModelConfig]) {
        let valid_chains = models
            .iter()
            .map(|model| model.public_name.clone())
            .collect::<HashSet<_>>();
        let valid_targets = models
            .iter()
            .flat_map(|model| {
                model
                    .targets
                    .iter()
                    .map(|target| target_key(model, target))
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        let mut valid_channel_models: BTreeMap<String, HashSet<String>> = BTreeMap::new();
        for model in models {
            for target in &model.targets {
                valid_channel_models
                    .entry(channel_label(target))
                    .or_default()
                    .insert(channel_model_display_label(target));
            }
        }
        let cutoff = now_ms().saturating_sub(HISTORICAL_STATS_RETENTION_MS);
        self.mutate(|stats| {
            stats.chains.retain(|name, item| {
                valid_chains.contains(name) || item.updated_at >= cutoff
            });
            stats.targets.retain(|key, item| {
                valid_targets.contains(key) || item.updated_at >= cutoff
            });
            stats.channel_models.retain(|channel_name, channel| {
                let valid_models = valid_channel_models.get(channel_name);
                channel.models.retain(|model_name, item| {
                    valid_models.is_some_and(|models| models.contains(model_name))
                        || item.updated_at >= cutoff
                });
                !channel.models.is_empty()
            });
        })
        .await;
    }
}

impl Stats {
    fn normalize(mut self) -> Self {
        trim_logs(&mut self.logs);
        for target in self.targets.values_mut() {
            target.last_error = trim_persisted_string(&target.last_error);
        }
        for channel in self.channel_models.values_mut() {
            for model in channel.models.values_mut() {
                model.last_error = trim_persisted_string(&model.last_error);
            }
        }
        self
    }
}

fn record_channel(
    stats: &mut Stats,
    target: &TargetConfig,
    ok: bool,
    failure: &FailureInfo,
    latency_ms: u64,
) {
    let channel_name = channel_label(target);
    let model_name = channel_model_display_label(target);
    let channel = stats
        .channel_models
        .entry(channel_name.clone())
        .or_insert_with(|| ChannelModelStats {
            name: channel_name,
            base_url: target.base_url.clone(),
            ..ChannelModelStats::default()
        });
    if channel.base_url.is_empty() {
        channel.base_url = target.base_url.clone();
    }
    channel.requests += 1;
    if ok {
        channel.successes += 1;
    } else {
        channel.failures += 1;
    }

    let item = channel
        .models
        .entry(model_name.clone())
        .or_insert_with(|| ChannelModelItemStats {
            name: model_name,
            ..ChannelModelItemStats::default()
        });
    item.requests += 1;
    if ok {
        item.successes += 1;
        item.last_status = 0;
        item.last_error.clear();
    } else {
        item.failures += 1;
        item.last_status = failure.status;
        item.last_error = if failure.message.is_empty() {
            trim_persisted_string(&failure.body)
        } else {
            trim_persisted_string(&failure.message)
        };
    }
    item.last_latency_ms = latency_ms;
    item.updated_at = now_ms();
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn chrono_like_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn trim_logs(logs: &mut Vec<LogEntry>) {
    logs.truncate(REQUEST_LOG_LIMIT);
    if logs.capacity() > REQUEST_LOG_LIMIT + 100 {
        logs.shrink_to_fit();
    }
}

fn apply_log_limits(logs: &mut Vec<LogEntry>, settings: &LogSettingsConfig) {
    logs.truncate(settings.max_entries);
    trim_logs_to_csv_size(logs, settings.max_bytes);
    if logs.capacity() > settings.max_entries + 100 {
        logs.shrink_to_fit();
    }
}

fn push_request_log(logs: &mut Vec<LogEntry>, entry: LogEntry, settings: &LogSettingsConfig) {
    if logs.len() >= settings.max_entries {
        logs.truncate(settings.max_entries.saturating_sub(1));
    }
    logs.insert(0, entry);
    apply_log_limits(logs, settings);
}

fn trim_persisted_string(value: &str) -> String {
    value.chars().take(1500).collect()
}

fn rebuild_chain_totals_from_logs(stats: &mut Stats) {
    stats.requests = 0;
    stats.successes = 0;
    stats.failures = 0;
    stats.failovers = 0;
    stats.chains.clear();

    for log in &stats.logs {
        stats.requests += 1;
        let succeeded = log.status == "success";
        let failed_over = succeeded && !log.failed_models.is_empty();
        if succeeded {
            stats.successes += 1;
            if failed_over {
                stats.failovers += 1;
            }
        } else {
            stats.failures += 1;
        }
        if log.chain_name.is_empty() {
            continue;
        }

        let chain = stats.chains.entry(log.chain_name.clone()).or_default();
        chain.requests += 1;
        if succeeded {
            chain.successes += 1;
            if failed_over {
                chain.failovers += 1;
            }
        } else {
            chain.failures += 1;
        }
    }
}

fn merge_logs(mut csv_logs: Vec<LogEntry>, legacy_logs: Option<Vec<LogEntry>>) -> Vec<LogEntry> {
    let mut seen = csv_logs
        .iter()
        .filter_map(|entry| {
            if entry.id.is_empty() {
                None
            } else {
                Some(entry.id.clone())
            }
        })
        .collect::<HashSet<_>>();
    if let Some(legacy_logs) = legacy_logs {
        for entry in legacy_logs {
            if !entry.id.is_empty() && !seen.insert(entry.id.clone()) {
                continue;
            }
            csv_logs.push(entry);
        }
    }
    csv_logs.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| right.id.cmp(&left.id))
    });
    csv_logs
}

fn merge_channel_models(
    mut csv_channels: BTreeMap<String, ChannelModelStats>,
    legacy_channels: Option<BTreeMap<String, ChannelModelStats>>,
) -> BTreeMap<String, ChannelModelStats> {
    let Some(legacy_channels) = legacy_channels else {
        return csv_channels;
    };
    for (channel_name, legacy_channel) in legacy_channels {
        let channel =
            csv_channels
                .entry(channel_name.clone())
                .or_insert_with(|| ChannelModelStats {
                    name: if legacy_channel.name.is_empty() {
                        channel_name
                    } else {
                        legacy_channel.name.clone()
                    },
                    base_url: legacy_channel.base_url.clone(),
                    ..ChannelModelStats::default()
                });
        if channel.name.is_empty() {
            channel.name = legacy_channel.name;
        }
        if channel.base_url.is_empty() {
            channel.base_url = legacy_channel.base_url;
        }
        channel.requests = channel.requests.max(legacy_channel.requests);
        channel.successes = channel.successes.max(legacy_channel.successes);
        channel.failures = channel.failures.max(legacy_channel.failures);
        for (model_name, legacy_model) in legacy_channel.models {
            let model_name = channel_model_display_name(&channel.name, &model_name);
            match channel.models.get_mut(&model_name) {
                Some(model) => merge_model_stat(model, legacy_model, &channel.name),
                None => {
                    channel.models.insert(
                        model_name.clone(),
                        normalize_channel_model_stat_name(&channel.name, &model_name, legacy_model),
                    );
                }
            }
        }
    }
    for channel in csv_channels.values_mut() {
        recompute_channel_totals(channel);
    }
    csv_channels
}

fn recompute_channel_totals(channel: &mut ChannelModelStats) {
    if channel.models.is_empty() {
        return;
    }
    channel.requests = channel.models.values().map(|item| item.requests).sum();
    channel.successes = channel.models.values().map(|item| item.successes).sum();
    channel.failures = channel.models.values().map(|item| item.failures).sum();
}

fn merge_model_stat(
    model: &mut ChannelModelItemStats,
    legacy: ChannelModelItemStats,
    channel_name: &str,
) {
    model.requests = model.requests.max(legacy.requests);
    model.successes = model.successes.max(legacy.successes);
    model.failures = model.failures.max(legacy.failures);
    if legacy.updated_at > model.updated_at {
        model.last_status = legacy.last_status;
        model.last_error = legacy.last_error;
        model.last_latency_ms = legacy.last_latency_ms;
        model.updated_at = legacy.updated_at;
    }
    if model.name.is_empty() {
        model.name = channel_model_display_name(channel_name, &legacy.name);
    }
}

fn normalize_channel_model_stat_name(
    channel_name: &str,
    model_key: &str,
    mut model: ChannelModelItemStats,
) -> ChannelModelItemStats {
    model.name = channel_model_display_name(
        channel_name,
        if model.name.is_empty() {
            model_key
        } else {
            &model.name
        },
    );
    model
}

fn channel_model_display_name(channel_name: &str, model_name: &str) -> String {
    if model_name.contains('|') {
        model_name.to_string()
    } else if channel_name.is_empty() {
        model_name.to_string()
    } else {
        format!("{channel_name}|{model_name}")
    }
}

fn trim_string_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn trim_logs_to_csv_size(logs: &mut Vec<LogEntry>, max_bytes: u64) {
    let mut total = logs_csv_size(logs);
    while total as u64 > max_bytes && !logs.is_empty() {
        if let Some(last) = logs.last() {
            total = total.saturating_sub(log_csv_row_size(last));
        }
        logs.pop();
    }
}

fn logs_csv_size(logs: &[LogEntry]) -> usize {
    REQUEST_LOGS_CSV_HEADER.len() + logs.iter().map(log_csv_row_size).sum::<usize>()
}

fn log_csv_row_size(log: &LogEntry) -> usize {
    let fields = log_to_csv_fields(log);
    fields
        .iter()
        .map(|field| csv_field_size(field))
        .sum::<usize>()
        + fields.len()
}

async fn save_request_logs_csv(path: &PathBuf, logs: &[LogEntry]) -> Result<()> {
    let mut text = String::with_capacity(logs_csv_size(logs));
    text.push_str(REQUEST_LOGS_CSV_HEADER);
    for log in logs {
        push_csv_row(&mut text, &log_to_csv_fields(log));
    }
    atomic_write(path, text.into_bytes()).await
}

async fn save_model_stats_csv(
    path: &PathBuf,
    channel_models: &BTreeMap<String, ChannelModelStats>,
) -> Result<()> {
    let mut text = String::from(MODEL_STATS_CSV_HEADER);
    for (channel_name, channel) in channel_models {
        for (model_name, model) in &channel.models {
            push_csv_row(
                &mut text,
                &[
                    channel_name.clone(),
                    channel.base_url.clone(),
                    model_name.clone(),
                    model.requests.to_string(),
                    model.successes.to_string(),
                    model.failures.to_string(),
                    model.last_status.to_string(),
                    model.last_error.clone(),
                    model.last_latency_ms.to_string(),
                    model.updated_at.to_string(),
                ],
            );
        }
    }
    atomic_write(path, text.into_bytes()).await
}

async fn load_request_logs_csv(path: &PathBuf) -> Result<Vec<LogEntry>> {
    let text = fs::read_to_string(path).await?;
    let rows = parse_csv(&text);
    Ok(rows
        .into_iter()
        .skip(1)
        .filter_map(csv_row_to_log_entry)
        .collect())
}

async fn load_model_stats_csv(path: &PathBuf) -> Result<BTreeMap<String, ChannelModelStats>> {
    let text = fs::read_to_string(path).await?;
    let rows = parse_csv(&text);
    let mut channels = BTreeMap::new();
    for row in rows.into_iter().skip(1).filter(|row| row.len() >= 10) {
        let channel_name = row[0].clone();
        let base_url = row[1].clone();
        let model_name = channel_model_display_name(&channel_name, &row[2]);
        let channel = channels
            .entry(channel_name.clone())
            .or_insert_with(|| ChannelModelStats {
                name: channel_name,
                base_url: base_url.clone(),
                ..ChannelModelStats::default()
            });
        if channel.base_url.is_empty() {
            channel.base_url = base_url;
        }
        let requests = row[3].parse().unwrap_or(0);
        let successes = row[4].parse().unwrap_or(0);
        let failures = row[5].parse().unwrap_or(0);
        channel.requests = channel.requests.saturating_add(requests);
        channel.successes = channel.successes.saturating_add(successes);
        channel.failures = channel.failures.saturating_add(failures);
        channel.models.insert(
            model_name.clone(),
            ChannelModelItemStats {
                name: model_name.clone(),
                requests,
                successes,
                failures,
                last_status: row[6].parse().unwrap_or(0),
                last_error: row[7].clone(),
                last_latency_ms: row[8].parse().unwrap_or(0),
                updated_at: row[9].parse().unwrap_or(0),
            },
        );
    }
    Ok(channels)
}

async fn save_runtime_stats_csv(path: &PathBuf, stats: &Stats) -> Result<()> {
    let mut text = String::from(RUNTIME_STATS_CSV_HEADER);
    push_csv_row(
        &mut text,
        &[
            "summary".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            stats.requests.to_string(),
            stats.successes.to_string(),
            stats.failures.to_string(),
            stats.failovers.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ],
    );
    for (name, chain) in &stats.chains {
        push_csv_row(
            &mut text,
            &[
                "chain".to_string(),
                name.clone(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                chain.requests.to_string(),
                chain.successes.to_string(),
                chain.failures.to_string(),
                chain.failovers.to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                chain.updated_at.to_string(),
            ],
        );
    }
    for (key, target) in &stats.targets {
        push_csv_row(
            &mut text,
            &[
                "target".to_string(),
                key.clone(),
                target.model.clone(),
                target.target.clone(),
                target.upstream_model.clone(),
                target.base_url.clone(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                target.ok.to_string(),
                target.error.to_string(),
                target.consecutive_failures.to_string(),
                target.disabled_until.to_string(),
                target.last_status.to_string(),
                target.last_error.clone(),
                target.last_latency_ms.to_string(),
                target.avg_latency_ms.to_string(),
                target.updated_at.to_string(),
            ],
        );
    }
    atomic_write(path, text.into_bytes()).await
}

async fn load_runtime_stats_csv(path: &PathBuf) -> Result<Option<RuntimeStatsSnapshot>> {
    let text = match fs::read_to_string(path).await {
        Ok(text) => text,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut snapshot = RuntimeStatsSnapshot::default();
    let mut found = false;
    for row in parse_csv(&text)
        .into_iter()
        .skip(1)
        .filter(|row| row.len() >= 18)
    {
        match row[0].as_str() {
            "summary" => {
                snapshot.requests = row[6].parse().unwrap_or(0);
                snapshot.successes = row[7].parse().unwrap_or(0);
                snapshot.failures = row[8].parse().unwrap_or(0);
                snapshot.failovers = row[9].parse().unwrap_or(0);
                found = true;
            }
            "chain" if !row[1].is_empty() => {
                snapshot.chains.insert(
                    row[1].clone(),
                    ChainStats {
                        requests: row[6].parse().unwrap_or(0),
                        successes: row[7].parse().unwrap_or(0),
                        failures: row[8].parse().unwrap_or(0),
                        failovers: row[9].parse().unwrap_or(0),
                        updated_at: runtime_stats_updated_at(&row),
                    },
                );
                found = true;
            }
            "target" if !row[1].is_empty() => {
                snapshot.targets.insert(
                    row[1].clone(),
                    TargetStats {
                        model: row[2].clone(),
                        target: row[3].clone(),
                        upstream_model: row[4].clone(),
                        base_url: row[5].clone(),
                        ok: row[10].parse().unwrap_or(0),
                        error: row[11].parse().unwrap_or(0),
                        consecutive_failures: row[12].parse().unwrap_or(0),
                        disabled_until: row[13].parse().unwrap_or(0),
                        last_status: row[14].parse().unwrap_or(0),
                        last_error: row[15].clone(),
                        last_latency_ms: row[16].parse().unwrap_or(0),
                        avg_latency_ms: row[17].parse().unwrap_or(0),
                        updated_at: runtime_stats_updated_at(&row),
                    },
                );
                found = true;
            }
            _ => {}
        }
    }
    Ok(found.then_some(snapshot))
}

fn runtime_stats_updated_at(row: &[String]) -> u64 {
    row.get(18)
        .and_then(|value| value.parse().ok())
        .filter(|updated_at| *updated_at > 0)
        .unwrap_or_else(now_ms)
}

async fn atomic_write(path: &PathBuf, bytes: Vec<u8>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_file_name(format!(
        "{}.{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("data.csv"),
        Uuid::new_v4()
    ));
    fs::write(&tmp, bytes).await?;
    fs::rename(&tmp, path).await?;
    Ok(())
}

fn log_to_csv_fields(log: &LogEntry) -> Vec<String> {
    vec![
        log.id.clone(),
        log.timestamp.to_string(),
        log.chain_name.clone(),
        log.original_model.clone(),
        serde_json::to_string(&log.failed_models).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(&log.failed_model_errors).unwrap_or_else(|_| "[]".to_string()),
        log.final_model.clone(),
        log.status.clone(),
        log.latency.to_string(),
        log.error.clone(),
    ]
}

fn csv_row_to_log_entry(row: Vec<String>) -> Option<LogEntry> {
    if row.len() >= 10 {
        return Some(LogEntry {
            id: row[0].clone(),
            timestamp: row[1].parse().unwrap_or(0),
            chain_name: row[2].clone(),
            original_model: row[3].clone(),
            failed_models: serde_json::from_str(&row[4]).unwrap_or_default(),
            failed_model_errors: serde_json::from_str(&row[5]).unwrap_or_default(),
            final_model: row[6].clone(),
            status: row[7].clone(),
            latency: row[8].parse().unwrap_or(0),
            error: row[9].clone(),
        });
    }
    if row.len() >= 9 {
        return Some(LogEntry {
            id: row[0].clone(),
            timestamp: row[1].parse().unwrap_or(0),
            chain_name: row[2].clone(),
            original_model: row[3].clone(),
            failed_models: serde_json::from_str(&row[4]).unwrap_or_default(),
            failed_model_errors: Vec::new(),
            final_model: row[5].clone(),
            status: row[6].clone(),
            latency: row[7].parse().unwrap_or(0),
            error: row[8].clone(),
        });
    }
    None
}

fn push_csv_row(out: &mut String, fields: &[String]) {
    for (idx, field) in fields.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        push_csv_field(out, field);
    }
    out.push('\n');
}

fn push_csv_field(out: &mut String, value: &str) {
    let must_quote =
        value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r');
    if !must_quote {
        out.push_str(value);
        return;
    }
    out.push('"');
    for ch in value.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
}

fn csv_field_size(value: &str) -> usize {
    let mut size = 0;
    let mut must_quote = false;
    for ch in value.chars() {
        if matches!(ch, ',' | '"' | '\n' | '\r') {
            must_quote = true;
        }
        size += if ch == '"' { 2 } else { ch.len_utf8() };
    }
    if must_quote {
        size + 2
    } else {
        size
    }
}

fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = text.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                field.push('"');
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                row.push(std::mem::take(&mut field));
            }
            '\n' if !quoted => {
                row.push(std::mem::take(&mut field));
                if !row.is_empty() {
                    rows.push(std::mem::take(&mut row));
                }
            }
            '\r' if !quoted => {}
            _ => field.push(ch),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(id: usize) -> LogEntry {
        LogEntry {
            id: id.to_string(),
            timestamp: id as u64,
            ..LogEntry::default()
        }
    }

    fn request_log(
        id: usize,
        chain_name: &str,
        status: &str,
        failed_models: Vec<&str>,
    ) -> LogEntry {
        LogEntry {
            id: id.to_string(),
            timestamp: id as u64,
            chain_name: chain_name.to_string(),
            status: status.to_string(),
            failed_models: failed_models.into_iter().map(str::to_string).collect(),
            ..LogEntry::default()
        }
    }

    fn model_stat(requests: u64, updated_at: u64, status: u16) -> ChannelModelItemStats {
        ChannelModelItemStats {
            name: "model-a".to_string(),
            requests,
            successes: requests.saturating_sub(1),
            failures: 1,
            last_status: status,
            last_error: format!("status {status}"),
            last_latency_ms: updated_at,
            updated_at,
        }
    }

    #[test]
    fn push_request_log_replaces_oldest_when_full() {
        let mut logs = (0..REQUEST_LOG_LIMIT).map(log).collect::<Vec<_>>();

        push_request_log(
            &mut logs,
            log(REQUEST_LOG_LIMIT),
            &LogSettingsConfig::default(),
        );

        assert_eq!(logs.len(), REQUEST_LOG_LIMIT);
        assert_eq!(logs.first().map(|entry| entry.id.as_str()), Some("500"));
        assert_eq!(logs.last().map(|entry| entry.id.as_str()), Some("498"));
        assert!(!logs.iter().any(|entry| entry.id == "499"));
    }

    #[test]
    fn trim_logs_keeps_only_limit() {
        let mut logs = (0..REQUEST_LOG_LIMIT + 10).map(log).collect::<Vec<_>>();

        trim_logs(&mut logs);

        assert_eq!(logs.len(), REQUEST_LOG_LIMIT);
        assert_eq!(logs.last().map(|entry| entry.id.as_str()), Some("499"));
    }

    #[test]
    fn csv_log_rows_support_failed_model_errors_and_legacy_rows() {
        let mut entry = log(7);
        entry.failed_models = vec!["model-a".to_string()];
        entry.failed_model_errors = vec![LogModelError {
            model: "model-a".to_string(),
            error: "HTTP 429".to_string(),
        }];
        entry.final_model = "model-b".to_string();
        entry.status = "success".to_string();
        entry.error = "model-a: HTTP 429".to_string();

        let mut text = String::new();
        push_csv_row(&mut text, &log_to_csv_fields(&entry));
        let parsed = parse_csv(&text);
        let parsed_entry = csv_row_to_log_entry(parsed[0].clone()).expect("new csv log");

        assert_eq!(parsed_entry.failed_model_errors.len(), 1);
        assert_eq!(parsed_entry.failed_model_errors[0].model, "model-a");
        assert_eq!(parsed_entry.failed_model_errors[0].error, "HTTP 429");

        let legacy = vec![
            "old".to_string(),
            "1".to_string(),
            "chain".to_string(),
            "public".to_string(),
            "[\"model-a\"]".to_string(),
            "model-b".to_string(),
            "success".to_string(),
            "25".to_string(),
            "legacy error".to_string(),
        ];
        let legacy_entry = csv_row_to_log_entry(legacy).expect("legacy csv log");
        assert_eq!(legacy_entry.failed_models, vec!["model-a"]);
        assert!(legacy_entry.failed_model_errors.is_empty());
        assert_eq!(legacy_entry.error, "legacy error");
    }

    #[test]
    fn record_channel_uses_channel_model_display_label() {
        let mut stats = Stats::default();
        let target = TargetConfig {
            name: "openrouter".to_string(),
            base_url: "https://openrouter.ai/api/v1".to_string(),
            model_name: "grok-4.3-high".to_string(),
            ..TargetConfig::default()
        };

        record_channel(&mut stats, &target, true, &FailureInfo::default(), 42);

        let channel = stats.channel_models.get("openrouter").expect("channel");
        let model = channel
            .models
            .get("openrouter|grok-4.3-high")
            .expect("model stats");
        assert_eq!(model.name, "openrouter|grok-4.3-high");
        assert_eq!(model.requests, 1);
    }

    #[test]
    fn model_stats_csv_reader_prefixes_legacy_model_names() {
        let rows = parse_csv(
            "channel,baseUrl,model,requests,successes,failures,lastStatus,lastError,lastLatencyMs,updatedAt\nopenrouter,https://openrouter.ai/api/v1,grok-4.3-high,2,1,1,429,rate limit,10,20\n",
        );
        let row = rows.into_iter().nth(1).expect("row");
        let channel_name = row[0].clone();
        let model_name = channel_model_display_name(&channel_name, &row[2]);

        assert_eq!(model_name, "openrouter|grok-4.3-high");
    }

    #[test]
    fn legacy_runtime_rows_receive_a_fresh_retention_timestamp() {
        let before = now_ms();
        let row = vec![String::new(); 18];
        assert!(runtime_stats_updated_at(&row) >= before);
    }

    #[tokio::test]
    async fn load_rebuilds_chain_totals_from_request_logs() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("failover-proxy-stats-rebuild-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let stats_path = dir.join("stats.json");
        let logs_path = dir.join("request-logs.csv");
        let model_stats_path = dir.join("model-stats.csv");
        let runtime_stats_path = dir.join("runtime-stats.csv");
        save_request_logs_csv(
            &logs_path,
            &[
                request_log(1, "auto-code", "success", vec![]),
                request_log(2, "auto-code", "success", vec!["primary"]),
                request_log(3, "auto-grok", "failed", vec!["primary"]),
            ],
        )
        .await?;

        let store = StatsStore::load(
            stats_path.clone(),
            logs_path,
            model_stats_path,
            runtime_stats_path.clone(),
            LogSettingsConfig::default(),
        )
        .await?;
        let snapshot = store.snapshot().await;

        assert_eq!(snapshot.requests, 3);
        assert_eq!(snapshot.successes, 2);
        assert_eq!(snapshot.failures, 1);
        assert_eq!(snapshot.failovers, 1);
        assert_eq!(snapshot.chains["auto-code"].requests, 2);
        assert_eq!(snapshot.chains["auto-code"].successes, 2);
        assert_eq!(snapshot.chains["auto-code"].failovers, 1);
        assert_eq!(snapshot.chains["auto-grok"].failures, 1);
        assert!(!stats_path.exists());
        assert!(runtime_stats_path.exists());

        fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn runtime_stats_csv_persists_chain_and_target_state() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("failover-proxy-runtime-stats-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let path = dir.join("runtime-stats.csv");
        let mut stats = Stats::default();
        stats.requests = 12;
        stats.successes = 10;
        stats.failures = 2;
        stats.failovers = 3;
        stats.chains.insert(
            "auto-code".to_string(),
            ChainStats {
                requests: 12,
                successes: 10,
                failures: 2,
                failovers: 3,
                updated_at: 123,
            },
        );
        stats.targets.insert(
            "auto-code/provider/model/url".to_string(),
            TargetStats {
                model: "auto-code".to_string(),
                target: "provider".to_string(),
                upstream_model: "model".to_string(),
                base_url: "https://example.test/v1".to_string(),
                ok: 10,
                error: 2,
                consecutive_failures: 1,
                disabled_until: 42,
                last_status: 429,
                last_error: "rate limit".to_string(),
                last_latency_ms: 120,
                avg_latency_ms: 95,
                updated_at: 456,
            },
        );

        save_runtime_stats_csv(&path, &stats).await?;
        let restored = load_runtime_stats_csv(&path).await?.expect("runtime stats");

        assert_eq!(restored.requests, 12);
        assert_eq!(restored.chains["auto-code"].failovers, 3);
        assert_eq!(restored.chains["auto-code"].updated_at, 123);
        assert_eq!(
            restored.targets["auto-code/provider/model/url"].last_status,
            429
        );
        assert_eq!(
            restored.targets["auto-code/provider/model/url"].last_error,
            "rate limit"
        );
        assert_eq!(
            restored.targets["auto-code/provider/model/url"].updated_at,
            456
        );

        fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_runtime_models_keeps_active_and_recent_stats_for_one_day() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("failover-proxy-stats-cleanup-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let store = StatsStore::load(
            dir.join("stats.json"),
            dir.join("request-logs.csv"),
            dir.join("model-stats.csv"),
            dir.join("runtime-stats.csv"),
            LogSettingsConfig::default(),
        )
        .await?;
        let active_model = ModelConfig {
            public_name: "active".to_string(),
            targets: vec![TargetConfig {
                name: "upstream".to_string(),
                base_url: "https://example.test/v1".to_string(),
                model_name: "model".to_string(),
                ..TargetConfig::default()
            }],
            ..ModelConfig::default()
        };
        let active_target = target_key(&active_model, &active_model.targets[0]);
        let now = now_ms();
        let stale = now.saturating_sub(HISTORICAL_STATS_RETENTION_MS + 60_000);
        let recent = now.saturating_sub(HISTORICAL_STATS_RETENTION_MS - 60_000);
        store
            .mutate(|stats| {
                stats.chains.insert(
                    "active".to_string(),
                    ChainStats {
                        updated_at: 0,
                        ..ChainStats::default()
                    },
                );
                stats.chains.insert(
                    "stale".to_string(),
                    ChainStats {
                        updated_at: stale,
                        ..ChainStats::default()
                    },
                );
                stats.chains.insert(
                    "recent-history".to_string(),
                    ChainStats {
                        updated_at: recent,
                        ..ChainStats::default()
                    },
                );
                stats.targets.insert(
                    active_target.clone(),
                    TargetStats {
                        updated_at: 0,
                        ..TargetStats::default()
                    },
                );
                stats.targets.insert(
                    "stale-target".to_string(),
                    TargetStats {
                        updated_at: stale,
                        ..TargetStats::default()
                    },
                );
                record_channel(
                    stats,
                    &active_model.targets[0],
                    true,
                    &FailureInfo::default(),
                    0,
                );
                let channel = stats
                    .channel_models
                    .get_mut(&channel_label(&active_model.targets[0]))
                    .expect("active channel");
                channel.models.insert(
                    "stale-model".to_string(),
                    ChannelModelItemStats {
                        name: "stale-model".to_string(),
                        updated_at: stale,
                        ..ChannelModelItemStats::default()
                    },
                );
                channel.models.insert(
                    "recent-model".to_string(),
                    ChannelModelItemStats {
                        name: "recent-model".to_string(),
                        updated_at: recent,
                        ..ChannelModelItemStats::default()
                    },
                );
            })
            .await;

        store
            .cleanup_runtime_models(std::slice::from_ref(&active_model))
            .await;
        let snapshot = store.snapshot().await;
        assert!(store.model_statistics_memory_usage().await.estimated_bytes > 0);
        assert!(snapshot.chains.contains_key("active"));
        assert!(snapshot.chains.contains_key("recent-history"));
        assert!(!snapshot.chains.contains_key("stale"));
        assert!(snapshot.targets.contains_key(&active_target));
        assert!(!snapshot.targets.contains_key("stale-target"));
        let channel = snapshot
            .channel_models
            .get(&channel_label(&active_model.targets[0]))
            .expect("active channel");
        assert!(!channel.models.contains_key("stale-model"));
        assert!(channel.models.contains_key("recent-model"));

        fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn queued_save_snapshots_state_after_prior_save_finishes() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("failover-proxy-save-lock-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let stats_path = dir.join("stats.json");
        let logs_path = dir.join("request-logs.csv");
        let model_stats_path = dir.join("model-stats.csv");
        let runtime_stats_path = dir.join("runtime-stats.csv");
        let store = StatsStore::load(
            stats_path,
            logs_path,
            model_stats_path,
            runtime_stats_path.clone(),
            LogSettingsConfig::default(),
        )
        .await?;

        let save_guard = store.save_lock.lock().await;
        store.chain_request("auto-code").await;
        let pending_save = tokio::spawn({
            let store = store.clone();
            async move { store.save_now().await }
        });
        tokio::task::yield_now().await;
        store.chain_request("auto-code").await;
        drop(save_guard);
        pending_save.await??;

        let persisted = load_runtime_stats_csv(&runtime_stats_path)
            .await?
            .expect("runtime stats");
        assert_eq!(persisted.requests, 2);
        assert_eq!(persisted.chains["auto-code"].requests, 2);

        fs::remove_dir_all(&dir).await?;
        Ok(())
    }

    #[tokio::test]
    async fn load_migrates_legacy_stats_json_into_csv_without_rewriting_it() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("failover-proxy-stats-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let stats_path = dir.join("stats.json");
        let logs_path = dir.join("request-logs.csv");
        let model_stats_path = dir.join("model-stats.csv");
        let runtime_stats_path = dir.join("runtime-stats.csv");

        let mut legacy = Stats::default();
        legacy.requests = 9;
        legacy.logs = vec![log(1), log(2)];
        legacy.channel_models.insert(
            "provider".to_string(),
            ChannelModelStats {
                name: "provider".to_string(),
                base_url: "https://legacy.example/v1".to_string(),
                requests: 10,
                successes: 8,
                failures: 2,
                models: BTreeMap::from([
                    ("model-a".to_string(), model_stat(10, 100, 500)),
                    ("model-b".to_string(), model_stat(3, 300, 200)),
                ]),
            },
        );
        fs::write(&stats_path, serde_json::to_vec_pretty(&legacy)?).await?;

        let mut existing_log = log(1);
        existing_log.timestamp = 10;
        save_request_logs_csv(&logs_path, &[existing_log, log(3)]).await?;
        save_model_stats_csv(
            &model_stats_path,
            &BTreeMap::from([(
                "provider".to_string(),
                ChannelModelStats {
                    name: "provider".to_string(),
                    base_url: "https://csv.example/v1".to_string(),
                    requests: 5,
                    successes: 4,
                    failures: 1,
                    models: BTreeMap::from([("model-a".to_string(), model_stat(5, 200, 200))]),
                },
            )]),
        )
        .await?;

        let store = StatsStore::load(
            stats_path.clone(),
            logs_path.clone(),
            model_stats_path.clone(),
            runtime_stats_path.clone(),
            LogSettingsConfig::default(),
        )
        .await?;
        let snapshot = store.snapshot().await;
        let migrated_logs = load_request_logs_csv(&logs_path).await?;
        let migrated_models = load_model_stats_csv(&model_stats_path).await?;

        assert!(stats_path.exists());
        assert!(runtime_stats_path.exists());
        assert_eq!(snapshot.requests, 9);
        assert_eq!(migrated_logs.len(), 3);
        assert_eq!(
            migrated_logs.iter().filter(|entry| entry.id == "1").count(),
            1
        );
        assert_eq!(
            migrated_logs.first().map(|entry| entry.id.as_str()),
            Some("1")
        );

        let provider = migrated_models.get("provider").expect("provider stats");
        let model_a = provider
            .models
            .get("provider|model-a")
            .expect("model-a stats");
        let model_b = provider
            .models
            .get("provider|model-b")
            .expect("model-b stats");
        assert_eq!(provider.requests, 13);
        assert_eq!(model_a.requests, 10);
        assert_eq!(model_a.name, "provider|model-a");
        assert_eq!(model_a.updated_at, 200);
        assert_eq!(model_a.last_status, 200);
        assert_eq!(model_b.requests, 3);

        fs::remove_dir_all(&dir).await?;
        Ok(())
    }
}
