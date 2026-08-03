use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};
use tokio::fs;
use uuid::Uuid;

pub const DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS: u32 = 1_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub admin_token: String,
    pub live_status_share_token: String,
    pub proxy_keys: Vec<ProxyKey>,
    pub failover_status_codes: Vec<u16>,
    pub request_timeout_ms: u64,
    pub log_settings: LogSettingsConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub model_source: ModelSourceConfig,
    pub providers: Vec<ProviderConfig>,
    pub models: Vec<ModelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyKey {
    pub name: String,
    pub key: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub cooldown_minutes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LogSettingsConfig {
    pub max_entries: usize,
    pub max_bytes: u64,
    pub max_error_chars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ModelSourceConfig {
    pub enabled: bool,
    pub url: String,
    pub api_key: String,
    pub refresh_seconds: u64,
    pub context_window_tokens: u32,
    pub include: String,
    pub exclude: String,
    pub public_prefix: String,
    pub public_suffix: String,
    pub targets: Vec<TargetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub api_keys: Vec<String>,
    pub api_key_mode: ApiKeyMode,
    pub models: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ModelConfig {
    pub public_name: String,
    pub context_window_tokens: u32,
    pub enabled: bool,
    pub strategy: FailoverStrategy,
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_model_name: Option<String>,
    pub targets: Vec<TargetConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TargetConfig {
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub api_keys: Vec<String>,
    pub api_key_mode: ApiKeyMode,
    pub model_name: String,
    pub model_name_template: String,
    pub enabled: bool,
    pub priority: u32,
    pub weight: u32,
    pub max_retries: u32,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum FailoverStrategy {
    Priority,
    RoundRobin,
    Weighted,
    LatencyBased,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ApiKeyMode {
    Single,
    RoundRobin,
    Random,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            admin_token: "admin".to_string(),
            live_status_share_token: String::new(),
            proxy_keys: vec![ProxyKey {
                name: "test-key".to_string(),
                key: "sk-local-test".to_string(),
                enabled: true,
            }],
            failover_status_codes: vec![401, 403, 408, 409, 429, 500, 502, 503, 504],
            request_timeout_ms: 120_000,
            log_settings: LogSettingsConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            model_source: ModelSourceConfig::default(),
            providers: Vec::new(),
            models: vec![ModelConfig {
                public_name: "gpt-failover".to_string(),
                context_window_tokens: DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS,
                enabled: true,
                strategy: FailoverStrategy::Priority,
                circuit_breaker: CircuitBreakerConfig::default(),
                source_model_name: None,
                targets: normalize_targets(vec![
                    TargetConfig {
                        name: "primary-openai".to_string(),
                        base_url: "https://api.openai.com/v1".to_string(),
                        api_key: "sk-replace-me".to_string(),
                        model_name: "gpt-4.1-mini".to_string(),
                        enabled: true,
                        ..TargetConfig::default()
                    },
                    TargetConfig {
                        name: "backup-openai".to_string(),
                        base_url: "https://api.openai.com/v1".to_string(),
                        api_key: "sk-replace-me-too".to_string(),
                        model_name: "gpt-4o-mini".to_string(),
                        enabled: false,
                        ..TargetConfig::default()
                    },
                ]),
            }],
        }
    }
}

impl Default for ProxyKey {
    fn default() -> Self {
        Self {
            name: String::new(),
            key: String::new(),
            enabled: true,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            cooldown_minutes: 10,
        }
    }
}

impl Default for LogSettingsConfig {
    fn default() -> Self {
        Self {
            max_entries: 500,
            max_bytes: 10 * 1024 * 1024,
            max_error_chars: 65_536,
        }
    }
}

impl Default for ModelSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: String::new(),
            api_key: String::new(),
            refresh_seconds: 300,
            context_window_tokens: DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS,
            include: String::new(),
            exclude: String::new(),
            public_prefix: String::new(),
            public_suffix: String::new(),
            targets: normalize_targets(vec![
                TargetConfig {
                    name: "primary-openai".to_string(),
                    base_url: "https://api.openai.com/v1".to_string(),
                    api_key: "sk-replace-me".to_string(),
                    model_name_template: "{model}".to_string(),
                    enabled: true,
                    ..TargetConfig::default()
                },
                TargetConfig {
                    name: "backup-openai".to_string(),
                    base_url: "https://api.openai.com/v1".to_string(),
                    api_key: "sk-replace-me-too".to_string(),
                    model_name_template: "{model}".to_string(),
                    enabled: false,
                    ..TargetConfig::default()
                },
            ]),
        }
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            public_name: String::new(),
            context_window_tokens: DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS,
            enabled: true,
            strategy: FailoverStrategy::Priority,
            circuit_breaker: CircuitBreakerConfig::default(),
            source_model_name: None,
            targets: Vec::new(),
        }
    }
}

impl Default for TargetConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            base_url: String::new(),
            api_key: String::new(),
            api_keys: Vec::new(),
            api_key_mode: ApiKeyMode::Single,
            model_name: String::new(),
            model_name_template: String::new(),
            enabled: true,
            priority: 1,
            weight: 1,
            max_retries: 0,
            timeout_ms: 120_000,
        }
    }
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            base_url: String::new(),
            api_key: String::new(),
            api_keys: Vec::new(),
            api_key_mode: ApiKeyMode::Single,
            models: Vec::new(),
            timeout_ms: None,
        }
    }
}

impl Default for FailoverStrategy {
    fn default() -> Self {
        FailoverStrategy::Priority
    }
}

impl Default for ApiKeyMode {
    fn default() -> Self {
        ApiKeyMode::Single
    }
}

pub fn normalize_config(mut cfg: Config) -> Config {
    let defaults = Config::default();
    if cfg.admin_token.is_empty() {
        cfg.admin_token = defaults.admin_token;
    }
    cfg.live_status_share_token = cfg.live_status_share_token.trim().to_string();
    if cfg.failover_status_codes.is_empty() {
        cfg.failover_status_codes = defaults.failover_status_codes;
    }
    if cfg.request_timeout_ms == 0 {
        cfg.request_timeout_ms = defaults.request_timeout_ms;
    }
    cfg.log_settings = normalize_log_settings(cfg.log_settings);
    cfg.circuit_breaker = normalize_circuit(cfg.circuit_breaker);
    cfg.model_source = normalize_model_source(cfg.model_source);
    cfg.providers = normalize_providers(cfg.providers);
    cfg.models = cfg.models.into_iter().map(normalize_model).collect();
    cfg.models = synchronize_model_targets_with_providers(cfg.models, &cfg.providers);
    cfg
}

pub fn normalize_model(mut model: ModelConfig) -> ModelConfig {
    if model.context_window_tokens == 0 {
        model.context_window_tokens = DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS;
    }
    model.circuit_breaker = normalize_circuit(model.circuit_breaker);
    model.targets = normalize_targets(model.targets);
    model
}

pub fn normalize_targets(targets: Vec<TargetConfig>) -> Vec<TargetConfig> {
    let mut out: Vec<TargetConfig> = targets
        .into_iter()
        .enumerate()
        .map(|(idx, mut target)| {
            if target.priority == 0 {
                target.priority = (idx + 1) as u32;
            }
            if target.weight == 0 {
                target.weight = 1;
            }
            if target.timeout_ms == 0 {
                target.timeout_ms = Config::default().request_timeout_ms;
            }
            normalize_api_key_fields(
                &mut target.api_key,
                &mut target.api_keys,
                &mut target.api_key_mode,
            );
            target
        })
        .collect();
    out.sort_by_key(|target| target.priority);
    for (idx, target) in out.iter_mut().enumerate() {
        target.priority = (idx + 1) as u32;
    }
    out
}

pub fn normalize_circuit(mut breaker: CircuitBreakerConfig) -> CircuitBreakerConfig {
    if breaker.failure_threshold == 0 {
        breaker.failure_threshold = 3;
    }
    if breaker.cooldown_minutes == 0 {
        breaker.cooldown_minutes = 10;
    }
    breaker
}

pub fn normalize_log_settings(mut settings: LogSettingsConfig) -> LogSettingsConfig {
    let defaults = LogSettingsConfig::default();
    if settings.max_entries == 0 {
        settings.max_entries = defaults.max_entries;
    }
    if settings.max_bytes == 0 {
        settings.max_bytes = defaults.max_bytes;
    }
    if settings.max_error_chars == 0 {
        settings.max_error_chars = defaults.max_error_chars;
    }
    settings.max_entries = settings.max_entries.clamp(1, 100_000);
    settings.max_bytes = settings.max_bytes.clamp(64 * 1024, 1024 * 1024 * 1024);
    settings.max_error_chars = settings.max_error_chars.clamp(256, 1024 * 1024);
    settings
}

pub fn normalize_model_source(mut source: ModelSourceConfig) -> ModelSourceConfig {
    if source.refresh_seconds == 0 {
        source.refresh_seconds = 300;
    }
    if source.context_window_tokens == 0 {
        source.context_window_tokens = DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS;
    }
    source.targets = normalize_targets(source.targets);
    source
}

pub fn normalize_providers(providers: Vec<ProviderConfig>) -> Vec<ProviderConfig> {
    providers
        .into_iter()
        .map(|mut provider| {
            provider.base_url = trim_slashes(&provider.base_url);
            normalize_api_key_fields(
                &mut provider.api_key,
                &mut provider.api_keys,
                &mut provider.api_key_mode,
            );
            provider.models = unique_strings(provider.models);
            provider
        })
        .filter(|provider| !provider.base_url.is_empty())
        .collect()
}

/// Removes targets whose matching provider no longer has the target model selected.
/// Targets without a matching provider are retained for backward compatibility with
/// configurations created before provider catalogs were introduced.
pub fn synchronize_model_targets_with_providers(
    models: Vec<ModelConfig>,
    providers: &[ProviderConfig],
) -> Vec<ModelConfig> {
    models
        .into_iter()
        .map(|mut model| {
            model.targets.retain(|target| {
                let Some(provider) = providers.iter().find(|provider| {
                    provider.name == target.name
                        && provider.base_url == target.base_url
                        && provider.api_key == target.api_key
                        && provider.api_keys == target.api_keys
                        && provider.api_key_mode == target.api_key_mode
                }) else {
                    return true;
                };
                provider
                    .models
                    .iter()
                    .any(|name| name == &target.model_name)
            });
            model.targets = normalize_targets(model.targets);
            model
        })
        .collect()
}

pub fn unique_strings(items: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    items
        .into_iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty() && seen.insert(item.clone()))
        .collect()
}

pub fn normalize_api_key_fields(
    api_key: &mut String,
    api_keys: &mut Vec<String>,
    mode: &mut ApiKeyMode,
) {
    *api_key = api_key.trim().to_string();
    *api_keys = unique_strings(std::mem::take(api_keys));
    if api_keys.is_empty() && !api_key.is_empty() {
        api_keys.push(api_key.clone());
    }
    if api_key.is_empty() {
        if let Some(first) = api_keys.first() {
            *api_key = first.clone();
        }
    }
    if api_keys.len() <= 1 {
        *mode = ApiKeyMode::Single;
    }
}

pub async fn ensure_config(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    if fs::metadata(path).await.is_err() {
        let text = serde_json::to_string_pretty(&Config::default())?;
        fs::write(path, text).await?;
    }
    Ok(())
}

pub async fn load_config(path: &Path) -> Result<Config> {
    ensure_config(path).await?;
    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("cannot read config {}", path.display()))?;
    let cfg: Config = serde_json::from_str(&text)
        .with_context(|| format!("cannot parse config {}", path.display()))?;
    Ok(normalize_config(cfg))
}

pub async fn save_config(path: &Path, cfg: &Config) -> Result<Config> {
    let normalized = normalize_config(cfg.clone());
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_file_name(format!(
        "{}.{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("config.json"),
        Uuid::new_v4()
    ));
    let text = serde_json::to_string_pretty(&normalized)?;
    fs::write(&tmp, text).await?;
    fs::rename(&tmp, path).await?;
    Ok(normalized)
}

pub fn model_circuit(model: &ModelConfig, cfg: &Config) -> CircuitBreakerConfig {
    let mut breaker = cfg.circuit_breaker.clone();
    let model_breaker = &model.circuit_breaker;
    breaker.failure_threshold = model_breaker.failure_threshold;
    breaker.cooldown_minutes = model_breaker.cooldown_minutes;
    normalize_circuit(breaker)
}

pub fn trim_slashes(input: &str) -> String {
    input.trim_end_matches('/').to_string()
}

pub fn endpoint_suffix(path: &str) -> String {
    let cleaned = path.trim_start_matches('/');
    let suffix = cleaned.strip_prefix("v1/").unwrap_or(cleaned);
    if suffix == "response" {
        "responses".to_string()
    } else {
        suffix.to_string()
    }
}

pub fn target_key(model: &ModelConfig, target: &TargetConfig) -> String {
    format!(
        "{}/{}/{}/{}",
        model.public_name, target.name, target.model_name, target.base_url
    )
}

pub fn target_label(target: &TargetConfig) -> String {
    channel_model_display_label(target)
}

pub fn channel_label(target: &TargetConfig) -> String {
    if !target.name.is_empty() {
        target.name.clone()
    } else {
        provider_name_from_url(&target.base_url).unwrap_or_else(|| "unknown".to_string())
    }
}

pub fn channel_model_label(target: &TargetConfig) -> String {
    if !target.model_name.is_empty() {
        target.model_name.clone()
    } else if !target.model_name_template.is_empty() {
        target.model_name_template.clone()
    } else if !target.name.is_empty() {
        target.name.clone()
    } else if !target.base_url.is_empty() {
        target.base_url.clone()
    } else {
        "unknown".to_string()
    }
}

pub fn channel_model_display_label(target: &TargetConfig) -> String {
    format!("{}|{}", channel_label(target), channel_model_label(target))
}

pub fn provider_name_from_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        endpoint_suffix, normalize_model, normalize_model_source, normalize_targets, target_label,
        ApiKeyMode, Config, ModelConfig, ModelSourceConfig, ProviderConfig, TargetConfig,
        DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS,
    };

    #[test]
    fn response_alias_uses_openai_responses_endpoint() {
        assert_eq!(endpoint_suffix("/v1/response"), "responses");
        assert_eq!(endpoint_suffix("/response"), "responses");
        assert_eq!(endpoint_suffix("/v1/responses"), "responses");
        assert_eq!(endpoint_suffix("/responses"), "responses");
    }

    #[test]
    fn target_label_includes_channel_and_model() {
        let target = TargetConfig {
            name: "openrouter".to_string(),
            model_name: "grok-4.3-high".to_string(),
            ..TargetConfig::default()
        };

        assert_eq!(target_label(&target), "openrouter|grok-4.3-high");
    }

    #[test]
    fn normalize_targets_preserves_multi_key_mode() {
        let targets = normalize_targets(vec![TargetConfig {
            api_keys: vec![" sk-a ".to_string(), "sk-b".to_string(), "sk-a".to_string()],
            api_key_mode: ApiKeyMode::RoundRobin,
            ..TargetConfig::default()
        }]);

        assert_eq!(targets[0].api_key, "sk-a");
        assert_eq!(targets[0].api_keys, vec!["sk-a", "sk-b"]);
        assert_eq!(targets[0].api_key_mode, ApiKeyMode::RoundRobin);
    }

    #[test]
    fn provider_catalog_prunes_only_unselected_matching_target_models() {
        let provider = ProviderConfig {
            id: "provider-1".to_string(),
            name: "provider".to_string(),
            base_url: "https://api.example.com/v1".to_string(),
            api_key: "sk-provider".to_string(),
            api_keys: vec!["sk-provider".to_string()],
            models: vec!["kept-model".to_string()],
            ..ProviderConfig::default()
        };
        let provider_name = provider.name.clone();
        let provider_base_url = provider.base_url.clone();
        let provider_api_key = provider.api_key.clone();
        let provider_api_keys = provider.api_keys.clone();
        let matching_target = move |model_name: &str| TargetConfig {
            name: provider_name.clone(),
            base_url: provider_base_url.clone(),
            api_key: provider_api_key.clone(),
            api_keys: provider_api_keys.clone(),
            model_name: model_name.to_string(),
            ..TargetConfig::default()
        };
        let config = Config {
            providers: vec![provider],
            models: vec![ModelConfig {
                targets: vec![
                    matching_target("kept-model"),
                    matching_target("removed-model"),
                    TargetConfig {
                        name: "legacy-provider".to_string(),
                        base_url: "https://legacy.example.com/v1".to_string(),
                        api_key: "sk-legacy".to_string(),
                        model_name: "legacy-model".to_string(),
                        ..TargetConfig::default()
                    },
                ],
                ..ModelConfig::default()
            }],
            ..Config::default()
        };

        let normalized = super::normalize_config(config);
        let target_models = normalized.models[0]
            .targets
            .iter()
            .map(|target| target.model_name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(target_models, vec!["kept-model", "legacy-model"]);
    }

    #[test]
    fn model_context_window_can_be_customized_and_legacy_zero_uses_default() {
        let custom = normalize_model(ModelConfig {
            context_window_tokens: 262_144,
            ..ModelConfig::default()
        });
        assert_eq!(custom.context_window_tokens, 262_144);

        let legacy = normalize_model(ModelConfig {
            context_window_tokens: 0,
            ..ModelConfig::default()
        });
        assert_eq!(
            legacy.context_window_tokens,
            DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS
        );

        let source = normalize_model_source(ModelSourceConfig {
            context_window_tokens: 0,
            ..ModelSourceConfig::default()
        });
        assert_eq!(
            source.context_window_tokens,
            DEFAULT_PROXY_CONTEXT_WINDOW_TOKENS
        );
    }

    #[test]
    fn legacy_breaker_fields_are_ignored_when_loading_and_removed_when_saving() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "circuitBreaker": {
                "failureThreshold": 4,
                "rateLimitKeyCooldownSeconds": 60,
                "authKeyCooldownMinutes": 30,
                "transientFailureThreshold": 9,
                "transientCooldownSeconds": 99,
                "compatibilityCooldownMinutes": 99
            }
        }))
        .expect("legacy config remains readable");

        assert_eq!(config.circuit_breaker.failure_threshold, 4);
        let serialized = serde_json::to_value(config).expect("config serializes");
        let breaker = &serialized["circuitBreaker"];
        assert!(breaker.get("rateLimitKeyCooldownSeconds").is_none());
        assert!(breaker.get("authKeyCooldownMinutes").is_none());
        assert!(breaker.get("transientFailureThreshold").is_none());
        assert!(breaker.get("transientCooldownSeconds").is_none());
        assert!(breaker.get("compatibilityCooldownMinutes").is_none());
    }
}
