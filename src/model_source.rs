use crate::{
    config::{normalize_model, Config, ModelConfig, ModelSourceConfig, TargetConfig},
    stats::RuntimeMemoryUsage,
};
use anyhow::{anyhow, Result};
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceModel {
    pub id: String,
}

#[derive(Debug, Clone, Default)]
pub struct ModelSourceCache {
    pub cache_key: String,
    pub fetched_at: u64,
    pub models: Vec<ModelConfig>,
    pub error: String,
}

#[derive(Clone)]
pub struct ModelSourceService {
    cache: Arc<RwLock<ModelSourceCache>>,
    client: Client,
}

#[derive(Clone, Default)]
pub struct ProviderHealthService {
    cache: Arc<RwLock<HashMap<String, Value>>>,
    client: Client,
}

impl ModelSourceService {
    pub fn new(client: Client) -> Self {
        Self {
            cache: Arc::new(RwLock::new(ModelSourceCache::default())),
            client,
        }
    }

    pub async fn error(&self) -> String {
        self.cache.read().await.error.clone()
    }

    pub async fn cached_models(&self) -> Vec<ModelConfig> {
        self.cache.read().await.models.clone()
    }

    pub async fn memory_usage(&self) -> RuntimeMemoryUsage {
        let cache = self.cache.read().await;
        let mut usage = model_configs_memory_usage(&cache.models);
        usage.lock_and_shard_bytes += std::mem::size_of::<RwLock<ModelSourceCache>>();
        usage.content_bytes += cache.cache_key.capacity() + cache.error.capacity();
        usage.finish();
        usage
    }

    pub async fn runtime_models(&self, cfg: &Config) -> Vec<ModelConfig> {
        let explicit = cfg
            .models
            .iter()
            .filter(|model| model.enabled)
            .cloned()
            .collect::<Vec<_>>();
        let source = match self.source_runtime_models(cfg, false).await {
            Ok(models) => models,
            Err(err) => {
                self.cache.write().await.error = err.to_string();
                Vec::new()
            }
        };
        let mut seen = std::collections::HashSet::new();
        explicit
            .into_iter()
            .chain(source.into_iter())
            .filter(|model| seen.insert(model.public_name.clone()))
            .collect()
    }

    pub async fn find_model(&self, cfg: &Config, public_name: &str) -> Option<ModelConfig> {
        self.runtime_models(cfg)
            .await
            .into_iter()
            .find(|model| model.enabled && model.public_name == public_name)
    }

    pub async fn source_runtime_models(
        &self,
        cfg: &Config,
        force: bool,
    ) -> Result<Vec<ModelConfig>> {
        let source = &cfg.model_source;
        if !source.enabled || source.url.is_empty() {
            return Ok(Vec::new());
        }
        let cache_key = source_cache_key(source);
        let max_age_ms = source.refresh_seconds.max(1) * 1000;
        {
            let cache = self.cache.read().await;
            if !force
                && cache.cache_key == cache_key
                && crate::stats::now_ms().saturating_sub(cache.fetched_at) < max_age_ms
            {
                return Ok(cache.models.clone());
            }
        }
        let remote = fetch_model_source(&self.client, source).await?;
        let filtered = filter_source_models(remote, source);
        let generated = filtered
            .into_iter()
            .map(|item| {
                let public_name = format!(
                    "{}{}{}",
                    source.public_prefix, item.id, source.public_suffix
                );
                normalize_model(ModelConfig {
                    public_name,
                    context_window_tokens: source.context_window_tokens,
                    enabled: true,
                    source_model_name: Some(item.id.clone()),
                    targets: source
                        .targets
                        .iter()
                        .cloned()
                        .map(|mut target| {
                            target.model_name = resolve_target_model_name(&target, &item.id);
                            target
                        })
                        .collect(),
                    ..ModelConfig::default()
                })
            })
            .collect::<Vec<_>>();
        let mut cache = self.cache.write().await;
        cache.cache_key = cache_key;
        cache.fetched_at = crate::stats::now_ms();
        cache.models = generated.clone();
        cache.error.clear();
        Ok(generated)
    }
}

impl ProviderHealthService {
    pub fn new(client: Client) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    pub async fn memory_usage(&self) -> RuntimeMemoryUsage {
        let cache = self.cache.read().await;
        let mut usage = RuntimeMemoryUsage {
            entries: cache.len(),
            capacity: Some(cache.capacity()),
            hash_bucket_bytes: cache
                .capacity()
                .saturating_mul(std::mem::size_of::<(String, Value)>().saturating_add(1)),
            lock_and_shard_bytes: std::mem::size_of::<RwLock<HashMap<String, Value>>>(),
            ..RuntimeMemoryUsage::default()
        };
        for (key, value) in cache.iter() {
            usage.content_bytes += key.capacity() + json_value_memory_usage(value);
        }
        usage.finish();
        usage
    }

    pub fn spawn_periodic_refresh(&self, config: Arc<RwLock<Config>>) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let cfg = config.read().await.clone();
                service.refresh_configured(&cfg).await;
            }
        });
    }

    pub async fn refresh_configured(&self, cfg: &Config) {
        self.refresh(configured_providers(cfg), true).await;
    }

    pub async fn cached_for(&self, providers: Vec<Value>) -> Vec<Value> {
        let cache = self.cache.read().await;
        providers
            .into_iter()
            .map(|provider| cached_provider_health(&cache, &provider))
            .collect()
    }

    pub async fn refresh_for(&self, providers: Vec<Value>) -> Vec<Value> {
        self.refresh(providers.clone(), false).await;
        self.cached_for(providers).await
    }

    async fn refresh(&self, providers: Vec<Value>, replace_all: bool) {
        if providers.is_empty() {
            if replace_all {
                self.cache.write().await.clear();
            }
            return;
        }
        let futures = providers
            .iter()
            .map(|provider| check_provider_health(&self.client, provider));
        let results = futures_util::future::join_all(futures).await;
        let mut next = HashMap::new();
        for (provider, result) in providers.into_iter().zip(results.into_iter()) {
            next.insert(provider_health_key(&provider), result);
        }
        let mut cache = self.cache.write().await;
        if replace_all {
            *cache = next;
        } else {
            cache.extend(next);
        }
    }
}

fn model_configs_memory_usage(models: &Vec<ModelConfig>) -> RuntimeMemoryUsage {
    let mut usage = RuntimeMemoryUsage {
        entries: models.len(),
        container_bytes: models.capacity() * std::mem::size_of::<ModelConfig>(),
        ..RuntimeMemoryUsage::default()
    };
    for model in models {
        usage.content_bytes += model.public_name.capacity()
            + model
                .source_model_name
                .as_ref()
                .map(String::capacity)
                .unwrap_or(0)
            + model.targets.capacity() * std::mem::size_of::<TargetConfig>();
        for target in &model.targets {
            usage.content_bytes += target.name.capacity()
                + target.base_url.capacity()
                + target.api_key.capacity()
                + target.model_name.capacity()
                + target.model_name_template.capacity()
                + target.api_keys.capacity() * std::mem::size_of::<String>();
            usage.estimated_bytes += target.api_keys.iter().map(String::capacity).sum::<usize>();
        }
    }
    usage.finish();
    usage
}

fn json_value_memory_usage(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(text) => text.capacity(),
        Value::Array(items) => {
            items.capacity() * std::mem::size_of::<Value>()
                + items.iter().map(json_value_memory_usage).sum::<usize>()
        }
        Value::Object(items) => {
            items
                    .iter()
                    .map(|(key, item)| key.capacity() + json_value_memory_usage(item))
                    .sum::<usize>()
        }
    }
}

pub async fn fetch_model_source(
    client: &Client,
    source: &ModelSourceConfig,
) -> Result<Vec<SourceModel>> {
    let mut request = client
        .get(&source.url)
        .timeout(Duration::from_millis(30_000))
        .header("accept", "application/json");
    if !source.api_key.is_empty() {
        request = request.bearer_auth(&source.api_key);
    }
    let res = request.send().await?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!(
            "Model source returned {}: {}",
            status.as_u16(),
            crate::proxy::trim_error(&text)
        ));
    }
    let payload: Value = serde_json::from_str(&text)?;
    Ok(extract_source_models(&payload))
}

pub async fn check_provider_health(client: &Client, provider: &Value) -> Value {
    let started = crate::stats::now_ms();
    let id = provider
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let name = provider
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let base_url = provider
        .get("baseUrl")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches('/')
        .to_string();
    let api_key = provider_api_key(provider);
    let timeout_ms = provider
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .unwrap_or(10_000);
    let mut result = serde_json::json!({
        "id": id,
        "name": name,
        "baseUrl": base_url,
        "status": "offline",
        "latency": 0u64,
        "models": [],
        "error": "",
        "checkedAt": 0u64
    });
    if base_url.is_empty() {
        result["latency"] = serde_json::json!(crate::stats::now_ms().saturating_sub(started));
        result["error"] = serde_json::json!("missing baseUrl");
        result["checkedAt"] = serde_json::json!(crate::stats::now_ms());
        return result;
    }
    let mut req = client
        .get(format!("{}/models", base_url))
        .timeout(Duration::from_millis(timeout_ms))
        .header("accept", "application/json");
    if !api_key.is_empty() {
        req = req.bearer_auth(api_key);
    }
    match req.send().await {
        Ok(res) => {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            result["latency"] = serde_json::json!(crate::stats::now_ms().saturating_sub(started));
            if !status.is_success() {
                result["error"] = serde_json::json!(format!(
                    "HTTP {}: {}",
                    status.as_u16(),
                    crate::proxy::trim_error(&text)
                ));
                result["checkedAt"] = serde_json::json!(crate::stats::now_ms());
                return result;
            }
            match serde_json::from_str::<Value>(&text) {
                Ok(payload) => {
                    let models = extract_source_models(&payload)
                        .into_iter()
                        .map(|item| item.id)
                        .collect::<Vec<_>>();
                    result["status"] = serde_json::json!("online");
                    result["models"] = serde_json::json!(models);
                }
                Err(err) => {
                    result["error"] = serde_json::json!(err.to_string());
                }
            }
        }
        Err(err) => {
            result["latency"] = serde_json::json!(crate::stats::now_ms().saturating_sub(started));
            result["error"] = serde_json::json!(if err.is_timeout() {
                "timeout".to_string()
            } else {
                err.to_string()
            });
        }
    }
    result["checkedAt"] = serde_json::json!(crate::stats::now_ms());
    result
}

pub fn configured_providers(cfg: &Config) -> Vec<Value> {
    let mut providers = cfg
        .providers
        .iter()
        .filter(|provider| !provider.base_url.is_empty())
        .map(|provider| {
            serde_json::json!({
                "id": provider.id.clone(),
                "name": if provider.name.is_empty() { provider.base_url.clone() } else { provider.name.clone() },
                "baseUrl": provider.base_url.clone(),
                "apiKey": provider.api_key.clone(),
                "apiKeys": provider.api_keys.clone(),
                "apiKeyMode": provider.api_key_mode,
                "timeoutMs": provider.timeout_ms.unwrap_or(10_000),
            })
        })
        .collect::<Vec<_>>();
    let mut targets: Vec<TargetConfig> = cfg
        .models
        .iter()
        .flat_map(|model| model.targets.clone())
        .collect();
    targets.extend(cfg.model_source.targets.clone());
    providers.extend(targets
        .into_iter()
        .filter(|target| !target.base_url.is_empty())
        .map(|target| {
            serde_json::json!({
                "id": format!("{}|{}|{}", target.name, target.base_url, target.api_key),
                "name": if target.name.is_empty() { target.base_url.clone() } else { target.name.clone() },
                "baseUrl": target.base_url,
                "apiKey": target.api_key,
                "apiKeys": target.api_keys,
                "apiKeyMode": target.api_key_mode,
                "timeoutMs": 10_000,
            })
        }));

    let mut seen = HashSet::new();
    providers
        .into_iter()
        .filter(|provider| seen.insert(provider_health_key(provider)))
        .collect()
}

pub fn provider_health_key(provider: &Value) -> String {
    let name = provider
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let base_url = provider
        .get("baseUrl")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches('/');
    let api_key = provider_api_key(provider);
    format!("{}|{}|{}", name, base_url, api_key)
}

fn provider_api_key(provider: &Value) -> &str {
    provider
        .get("apiKey")
        .and_then(Value::as_str)
        .filter(|key| !key.trim().is_empty())
        .or_else(|| {
            provider
                .get("apiKeys")
                .and_then(Value::as_array)
                .and_then(|keys| keys.iter().find_map(Value::as_str))
        })
        .unwrap_or("")
}

fn cached_provider_health(cache: &HashMap<String, Value>, provider: &Value) -> Value {
    let id = provider
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let name = provider
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let base_url = provider
        .get("baseUrl")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches('/')
        .to_string();
    let mut value = cache
        .get(&provider_health_key(provider))
        .cloned()
        .unwrap_or_else(|| {
            serde_json::json!({
                "status": "unknown",
                "latency": 0u64,
                "models": [],
                "error": "",
                "checkedAt": 0u64
            })
        });
    value["id"] = serde_json::json!(id);
    value["name"] = serde_json::json!(name);
    value["baseUrl"] = serde_json::json!(base_url);
    value
}

pub fn extract_source_models(payload: &Value) -> Vec<SourceModel> {
    let list = if payload.is_array() {
        payload.as_array()
    } else if payload.get("data").is_some_and(Value::is_array) {
        payload.get("data").and_then(Value::as_array)
    } else if payload.get("models").is_some_and(Value::is_array) {
        payload.get("models").and_then(Value::as_array)
    } else {
        None
    };
    list.unwrap_or(&Vec::new())
        .iter()
        .filter_map(|item| {
            if let Some(id) = item.as_str() {
                return Some(SourceModel { id: id.to_string() });
            }
            let id = item
                .get("id")
                .or_else(|| item.get("name"))
                .or_else(|| item.get("model"))
                .or_else(|| item.get("publicName"))
                .and_then(Value::as_str)?;
            Some(SourceModel { id: id.to_string() })
        })
        .collect()
}

pub fn filter_source_models(
    models: Vec<SourceModel>,
    source: &ModelSourceConfig,
) -> Vec<SourceModel> {
    let include = compile_pattern(&source.include);
    let exclude = compile_pattern(&source.exclude);
    models
        .into_iter()
        .filter(|item| {
            if include.as_ref().is_some_and(|re| !re.is_match(&item.id)) {
                return false;
            }
            if exclude.as_ref().is_some_and(|re| re.is_match(&item.id)) {
                return false;
            }
            true
        })
        .collect()
}

fn compile_pattern(pattern: &str) -> Option<Regex> {
    if pattern.is_empty() {
        return None;
    }
    Regex::new(pattern).ok()
}

pub fn resolve_target_model_name(target: &TargetConfig, source_model_name: &str) -> String {
    let template = if !target.model_name_template.is_empty() {
        target.model_name_template.as_str()
    } else if !target.model_name.is_empty() {
        target.model_name.as_str()
    } else {
        "{model}"
    };
    template.replace("{model}", source_model_name)
}

pub fn source_cache_key(source: &ModelSourceConfig) -> String {
    [
        source.enabled.to_string(),
        source.url.clone(),
        if source.api_key.is_empty() {
            "no-key"
        } else {
            "with-key"
        }
        .to_string(),
        source.include.clone(),
        source.exclude.clone(),
        source.public_prefix.clone(),
        source.public_suffix.clone(),
        source.context_window_tokens.to_string(),
    ]
    .join("|")
}
