use crate::{
    config::Config,
    stats::{dashmap_memory_overhead, RuntimeMemoryUsage},
};
use axum::http::HeaderMap;
use constant_time_eq::constant_time_eq;
use dashmap::DashMap;
use rand::{distributions::Alphanumeric, Rng};
use serde::Serialize;
use std::sync::Arc;

const ADMIN_SESSION_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_ADMIN_SESSIONS: usize = 128;

#[derive(Clone, Default)]
pub struct AuthState {
    sessions: Arc<DashMap<String, AdminSession>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSession {
    pub created_at: u64,
}

impl AuthState {
    pub fn create_admin_session(&self) -> String {
        self.cleanup_sessions();
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(43)
            .map(char::from)
            .collect();
        self.sessions.insert(
            token.clone(),
            AdminSession {
                created_at: crate::stats::now_ms(),
            },
        );
        self.enforce_session_limit();
        token
    }

    pub fn delete_admin_session(&self, headers: &HeaderMap) {
        if let Some(session) = header_value(headers, "x-admin-session") {
            self.sessions.remove(&session);
        }
    }

    pub fn create_live_status_share_token() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect()
    }

    pub fn memory_usage(&self) -> RuntimeMemoryUsage {
        let mut usage = dashmap_memory_overhead(&self.sessions);
        for session in self.sessions.iter() {
            usage.entries += 1;
            usage.content_bytes += session.key().capacity();
        }
        usage.finish();
        usage
    }

    pub fn is_admin(&self, headers: &HeaderMap, cfg: &Config) -> bool {
        if let Some(session) = header_value(headers, "x-admin-session") {
            if let Some(item) = self.sessions.get(&session) {
                if is_session_live(item.created_at) {
                    return true;
                }
                drop(item);
                self.sessions.remove(&session);
            }
        }
        let token = cfg.admin_token.as_bytes();
        if let Some(header) = header_value(headers, "x-admin-token") {
            if secure_eq(header.as_bytes(), token) {
                return true;
            }
        }
        if let Some(bearer) = bearer(headers) {
            return secure_eq(bearer.as_bytes(), token);
        }
        false
    }

    fn cleanup_sessions(&self) {
        let expired = self
            .sessions
            .iter()
            .filter_map(|entry| {
                if is_session_live(entry.value().created_at) {
                    None
                } else {
                    Some(entry.key().clone())
                }
            })
            .collect::<Vec<_>>();
        for session in expired {
            self.sessions.remove(&session);
        }
    }

    fn enforce_session_limit(&self) {
        if self.sessions.len() <= MAX_ADMIN_SESSIONS {
            return;
        }
        let mut sessions = self
            .sessions
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().created_at))
            .collect::<Vec<_>>();
        sessions.sort_by_key(|(_, created_at)| *created_at);
        let excess = sessions.len().saturating_sub(MAX_ADMIN_SESSIONS);
        for (session, _) in sessions.into_iter().take(excess) {
            self.sessions.remove(&session);
        }
    }

}

pub fn is_live_status_share_token(token: &str, cfg: &Config) -> bool {
    !cfg.live_status_share_token.is_empty()
        && secure_eq(token.as_bytes(), cfg.live_status_share_token.as_bytes())
}

pub fn is_proxy_key(headers: &HeaderMap, cfg: &Config) -> bool {
    let Some(token) = bearer(headers) else {
        return false;
    };
    cfg.proxy_keys
        .iter()
        .any(|item| item.enabled && secure_eq(token.as_bytes(), item.key.as_bytes()))
}

pub fn bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("authorization")?.to_str().ok()?;
    let mut parts = raw.splitn(2, char::is_whitespace);
    let scheme = parts.next()?.trim();
    let token = parts.next()?.trim();
    if scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() {
        Some(token.to_string())
    } else {
        None
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .map(|value| value.to_string())
}

fn secure_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && constant_time_eq(a, b)
}

fn is_session_live(created_at: u64) -> bool {
    crate::stats::now_ms().saturating_sub(created_at) <= ADMIN_SESSION_TTL_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_status_share_token_is_high_entropy() {
        let share = AuthState::create_live_status_share_token();

        assert_eq!(share.len(), 64);
        assert!(share.chars().all(|ch| ch.is_ascii_alphanumeric()));
    }
}
