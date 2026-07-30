use crate::{
    config::{model_circuit, target_key, CircuitBreakerConfig, Config, ModelConfig, TargetConfig},
    stats::{dashmap_memory_overhead, now_ms, FailureInfo, RuntimeMemoryUsage, StatsStore},
};
use dashmap::DashMap;
use std::{collections::HashSet, sync::Arc};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
enum FailureKind {
    /// A bad or exhausted credential is isolated at the API-key layer.
    Authentication,
    /// A 429 is isolated at the API-key layer so sibling keys can continue.
    RateLimited,
    /// The upstream endpoint does not support the requested protocol.
    Compatibility,
    /// Timeouts, connection errors, and server-side failures.
    Transient,
    #[default]
    Other,
}

#[derive(Debug, Clone, Default)]
pub struct BreakerState {
    pub failures: u32,
    pub disabled_until: u64,
    kind: FailureKind,
}

#[derive(Clone, Default)]
pub struct CircuitBreakers {
    inner: Arc<DashMap<String, BreakerState>>,
}

impl CircuitBreakers {
    pub fn is_open(&self, model: &ModelConfig, target: &TargetConfig) -> bool {
        let key = target_key(model, target);
        let Some(item) = self.inner.get(&key) else {
            return false;
        };
        let disabled_until = item.disabled_until;
        if disabled_until == 0 {
            return false;
        }
        if now_ms() >= disabled_until {
            drop(item);
            self.inner.remove(&key);
            return false;
        }
        true
    }

    pub async fn is_open_and_cleanup(
        &self,
        model: &ModelConfig,
        target: &TargetConfig,
        stats: &StatsStore,
    ) -> bool {
        let key = target_key(model, target);
        let Some(item) = self.inner.get(&key) else {
            return false;
        };
        let disabled_until = item.disabled_until;
        if disabled_until == 0 {
            return false;
        }
        if now_ms() >= disabled_until {
            drop(item);
            self.inner.remove(&key);
            stats.clear_target_breaker_stats(model, target).await;
            return false;
        }
        true
    }

    pub async fn record_success(
        &self,
        model: &ModelConfig,
        target: &TargetConfig,
        cfg: &Config,
        stats: &StatsStore,
        latency_ms: u64,
    ) {
        let key = target_key(model, target);
        self.inner.remove(&key);
        stats
            .record_target(
                model,
                target,
                true,
                cfg,
                FailureInfo::default(),
                latency_ms,
                0,
                0,
            )
            .await;
    }

    pub async fn record_failure(
        &self,
        model: &ModelConfig,
        target: &TargetConfig,
        cfg: &Config,
        stats: &StatsStore,
        failure: FailureInfo,
        latency_ms: u64,
    ) {
        let key = target_key(model, target);
        let breaker_cfg = model_circuit(model, cfg);
        let kind = classify_failure_kind(&failure);
        let Some((failure_threshold, cooldown_ms)) = target_breaker_policy(kind, &breaker_cfg)
        else {
            // Authentication and rate-limit failures are scoped to the API
            // key by ProxyRuntime.  Never let one exhausted/bad key poison a
            // target that may contain several healthy credentials.
            self.inner.remove(&key);
            stats
                .record_target(model, target, false, cfg, failure, latency_ms, 0, 0)
                .await;
            return;
        };
        let mut state = self.inner.entry(key).or_default();
        if state.kind != kind {
            state.failures = 0;
            state.disabled_until = 0;
            state.kind = kind;
        }
        state.failures += 1;
        if state.failures >= failure_threshold {
            state.disabled_until = now_ms() + cooldown_ms;
        }
        let disabled_until = state.disabled_until;
        let failures = state.failures;
        drop(state);
        stats
            .record_target(
                model,
                target,
                false,
                cfg,
                failure,
                latency_ms,
                disabled_until,
                failures,
            )
            .await;
    }

    pub async fn reset_model(&self, model: &ModelConfig, stats: &StatsStore) {
        for target in &model.targets {
            self.inner.remove(&target_key(model, target));
            stats.clear_target_breaker_stats(model, target).await;
        }
    }

    pub fn cleanup_expired(&self) {
        let now = now_ms();
        let expired = self
            .inner
            .iter()
            .filter_map(|entry| {
                (entry.disabled_until > 0 && entry.disabled_until <= now)
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        for key in expired {
            self.inner.remove(&key);
        }
    }

    pub fn memory_usage(&self) -> RuntimeMemoryUsage {
        let mut usage = dashmap_memory_overhead(&self.inner);
        for entry in self.inner.iter() {
            usage.entries += 1;
            usage.content_bytes += entry.key().capacity();
        }
        usage.finish();
        usage
    }

    pub fn retain_targets(&self, models: &[ModelConfig]) {
        let valid = models
            .iter()
            .flat_map(|model| {
                model
                    .targets
                    .iter()
                    .map(|target| target_key(model, target))
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        let stale = self
            .inner
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
            self.inner.remove(&key);
        }
    }
}

fn classify_failure_kind(failure: &FailureInfo) -> FailureKind {
    match failure.status {
        401 | 403 => FailureKind::Authentication,
        429 => FailureKind::RateLimited,
        404 | 405 | 406 | 415 | 501 => FailureKind::Compatibility,
        0 | 408 | 409 | 500 | 502 | 503 | 504 => FailureKind::Transient,
        status if status >= 500 => FailureKind::Transient,
        _ => FailureKind::Other,
    }
}

fn target_breaker_policy(kind: FailureKind, cfg: &CircuitBreakerConfig) -> Option<(u32, u64)> {
    match kind {
        FailureKind::Authentication | FailureKind::RateLimited => None,
        FailureKind::Compatibility => Some((1, cfg.compatibility_cooldown_minutes * 60 * 1000)),
        FailureKind::Transient => Some((
            cfg.transient_failure_threshold,
            cfg.transient_cooldown_seconds * 1000,
        )),
        FailureKind::Other => Some((cfg.failure_threshold, cfg.cooldown_minutes * 60 * 1000)),
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_failure_kind, target_breaker_policy, FailureKind};
    use crate::{config::CircuitBreakerConfig, stats::FailureInfo};

    #[test]
    fn isolates_key_scoped_failures_from_target_breaker() {
        assert_eq!(
            classify_failure_kind(&FailureInfo {
                status: 429,
                ..FailureInfo::default()
            }),
            FailureKind::RateLimited
        );
        assert!(target_breaker_policy(
            FailureKind::Authentication,
            &CircuitBreakerConfig::default(),
        )
        .is_none());
    }

    #[test]
    fn compatibility_opens_immediately_but_transient_uses_own_threshold() {
        let cfg = CircuitBreakerConfig::default();
        assert_eq!(
            target_breaker_policy(FailureKind::Compatibility, &cfg),
            Some((1, 10 * 60 * 1000))
        );
        assert_eq!(
            target_breaker_policy(FailureKind::Transient, &cfg),
            Some((3, 60 * 1000))
        );
    }

    #[test]
    fn cleanup_expired_removes_only_elapsed_open_breakers() {
        let breakers = super::CircuitBreakers::default();
        breakers.inner.insert(
            "expired".to_string(),
            super::BreakerState {
                disabled_until: crate::stats::now_ms().saturating_sub(1),
                ..super::BreakerState::default()
            },
        );
        breakers.inner.insert(
            "active".to_string(),
            super::BreakerState {
                disabled_until: crate::stats::now_ms().saturating_add(60_000),
                ..super::BreakerState::default()
            },
        );

        breakers.cleanup_expired();

        assert!(!breakers.inner.contains_key("expired"));
        assert!(breakers.inner.contains_key("active"));
        assert_eq!(breakers.memory_usage().entries, 1);
    }
}
