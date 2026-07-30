use crate::{
    config::{normalize_log_settings, save_config, Config, LogSettingsConfig, ModelConfig},
    model_source::{configured_providers, fetch_model_source, filter_source_models},
    AppState,
};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{json, Value};

include!(concat!(env!("OUT_DIR"), "/embedded_assets.rs"));

pub async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let cfg = state.config.read().await.clone();
    let token = body.get("token").and_then(Value::as_str).unwrap_or("");
    if token != cfg.admin_token {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let session = state.auth.create_admin_session();
    let _ = headers;
    admin_json(json!({ "ok": true, "session": session }))
}

pub async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    state.auth.delete_admin_session(&headers);
    admin_json(json!({ "ok": true }))
}

pub async fn session(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    admin_json(json!({ "ok": state.auth.is_admin(&headers, &cfg) }))
}

pub async fn create_live_status_share(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let mut cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    cfg.live_status_share_token = crate::auth::AuthState::create_live_status_share_token();
    match save_config(&state.config_path, &cfg).await {
        Ok(normalized) => {
            let path = format!("/share/live-status/{}", normalized.live_status_share_token);
            *state.config.write().await = normalized;
            admin_json(json!({ "ok": true, "path": path, "persistent": true }))
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn list_live_status_shares(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let path = (!cfg.live_status_share_token.is_empty())
        .then(|| format!("/share/live-status/{}", cfg.live_status_share_token));
    admin_json(json!({ "enabled": path.is_some(), "path": path, "persistent": true }))
}

pub async fn revoke_live_status_share(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let mut cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    cfg.live_status_share_token.clear();
    match save_config(&state.config_path, &cfg).await {
        Ok(normalized) => {
            *state.config.write().await = normalized;
            admin_json(json!({ "ok": true }))
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn shared_live_status(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !crate::auth::is_live_status_share_token(&token, &cfg) {
        return send_error(
            StatusCode::NOT_FOUND,
            "Invalid or expired live status share link",
            None,
        );
    }
    let memory = process_memory();
    let runtime_state_memory = runtime_state_memory(&state, &memory).await;
    no_store(
        Json(json!({
            "activeThreads": state.proxy_runtime.snapshot_threads(),
            "memory": memory,
            "runtimeStateMemory": runtime_state_memory,
        }))
        .into_response(),
    )
}

pub async fn live_status_share_page(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !crate::auth::is_live_status_share_token(&token, &cfg) {
        return send_error(
            StatusCode::NOT_FOUND,
            "Invalid or expired live status share link",
            None,
        );
    }
    let mut response = embedded_response("index.html");
    response
        .headers_mut()
        .insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    no_store(response)
}

pub async fn get_config(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    admin_json(cfg)
}

pub async fn post_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(next_config): Json<Config>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    match save_config(&state.config_path, &next_config).await {
        Ok(normalized) => {
            *state.config.write().await = normalized.clone();
            if let Err(err) = state
                .stats
                .apply_log_settings(normalized.log_settings.clone())
                .await
            {
                tracing::warn!(error = %err, "cannot apply log settings");
            }
            let mut runtime_models = normalized.models.clone();
            runtime_models.extend(state.model_source.cached_models().await);
            cleanup_runtime_state(&state, &runtime_models).await;
            admin_json(json!({ "ok": true, "config": normalized }))
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn get_log_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let stats = state.stats.snapshot().await;
    admin_json(json!({
        "ok": true,
        "settings": state.stats.log_settings().await,
        "logCount": stats.logs.len(),
        "logsPath": state.stats.logs_path().to_string_lossy(),
        "modelStatsPath": state.stats.model_stats_path().to_string_lossy(),
    }))
}

pub async fn post_log_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<LogSettingsConfig>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let settings = normalize_log_settings(body);
    let mut next_config = cfg.clone();
    next_config.log_settings = settings.clone();
    match save_config(&state.config_path, &next_config).await {
        Ok(normalized) => {
            *state.config.write().await = normalized.clone();
            match state
                .stats
                .apply_log_settings(normalized.log_settings.clone())
                .await
            {
                Ok(applied) => admin_json(json!({ "ok": true, "settings": applied })),
                Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
            }
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn clear_logs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    match state.stats.clear_logs().await {
        Ok(()) => admin_json(json!({ "ok": true, "logCount": 0 })),
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn get_stats(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let mut value =
        serde_json::to_value(state.stats.snapshot().await).unwrap_or_else(|_| json!({}));
    value["activeThreads"] = json!(state.proxy_runtime.snapshot_threads());
    let memory = process_memory();
    value["memory"] = memory.clone();
    value["runtimeStateMemory"] = runtime_state_memory(&state, &memory).await;
    admin_json(value)
}

pub async fn get_page_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(page): Path<String>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }

    let value = match page.as_str() {
        "dashboard" => {
            let stats = state.stats.snapshot().await;
            json!({
                "startedAt": stats.started_at,
                "requests": stats.requests,
                "successes": stats.successes,
                "failures": stats.failures,
                "failovers": stats.failovers,
                "chains": stats.chains,
                "logs": stats.logs.into_iter().take(10).collect::<Vec<_>>(),
            })
        }
        "chains" => {
            let stats = state.stats.snapshot().await;
            json!({
                "requests": stats.requests,
                "successes": stats.successes,
                "failures": stats.failures,
                "failovers": stats.failovers,
                "chains": stats.chains,
            })
        }
        "model-stats" => {
            let stats = state.stats.snapshot().await;
            json!({
                "requests": stats.requests,
                "successes": stats.successes,
                "failures": stats.failures,
                "failovers": stats.failovers,
                "channelModels": stats.channel_models,
            })
        }
        "live-status" => {
            let memory = process_memory();
            let runtime_state_memory = runtime_state_memory(&state, &memory).await;
            json!({
                "activeThreads": state.proxy_runtime.snapshot_threads(),
                "memory": memory,
                "runtimeStateMemory": runtime_state_memory,
            })
        }
        "logs" => {
            let stats = state.stats.snapshot().await;
            json!({
                "logs": stats.logs,
                "logSettings": state.stats.log_settings().await,
                "logsPath": state.stats.logs_path().to_string_lossy(),
                "modelStatsPath": state.stats.model_stats_path().to_string_lossy(),
            })
        }
        _ => json!({}),
    };
    admin_json(value)
}

pub async fn health(State(state): State<AppState>) -> Response {
    let cfg = state.config.read().await.clone();
    let models = state.model_source.runtime_models(&cfg).await;
    admin_json(json!({
        "ok": true,
        "startedAt": state.stats.snapshot().await.started_at,
        "configPath": state.config_path.to_string_lossy(),
        "runtimeStatsPath": state.runtime_stats_path.to_string_lossy(),
        "models": models.into_iter().map(|m| m.public_name).collect::<Vec<_>>(),
        "modelSourceError": state.model_source.error().await
    }))
}

pub async fn providers_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let providers = body
        .get("providers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| configured_providers(&cfg));
    let refresh = body
        .get("refresh")
        .or_else(|| body.get("force"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let results = if refresh {
        state.provider_health.refresh_for(providers).await
    } else {
        state.provider_health.cached_for(providers).await
    };
    admin_json(json!({ "ok": true, "providers": results }))
}

pub async fn model_source_preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let mut source_value = serde_json::to_value(cfg.model_source).unwrap_or_else(|_| json!({}));
    merge_json(&mut source_value, body);
    source_value["enabled"] = json!(true);
    let source =
        match serde_json::from_value(source_value).map(crate::config::normalize_model_source) {
            Ok(source) => source,
            Err(err) => return send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
        };
    match fetch_model_source(&state.client, &source).await {
        Ok(models) => {
            let filtered = filter_source_models(models, &source);
            admin_json(json!({
                "ok": true,
                "count": filtered.len(),
                "models": filtered.into_iter().take(200).map(|m| m.id).collect::<Vec<_>>()
            }))
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn model_source_refresh(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    match state.model_source.source_runtime_models(&cfg, true).await {
        Ok(models) => {
            let mut runtime_models = cfg.models.clone();
            runtime_models.extend(models.clone());
            cleanup_runtime_state(&state, &runtime_models).await;
            admin_json(json!({
                "ok": true,
                "count": models.len(),
                "models": models.into_iter().take(200).map(|m| m.public_name).collect::<Vec<_>>()
            }))
        }
        Err(err) => send_error(StatusCode::BAD_REQUEST, &err.to_string(), None),
    }
}

pub async fn model_tests_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let cfg = state.config.read().await.clone();
    if !state.auth.is_admin(&headers, &cfg) {
        return send_error(StatusCode::UNAUTHORIZED, "Invalid admin token", None);
    }
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .take(50)
        .filter(|target| {
            target
                .get("baseUrl")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
                && target
                    .get("modelName")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return send_error(
            StatusCode::BAD_REQUEST,
            "No testable models were provided",
            None,
        );
    }
    let capabilities = normalize_capabilities(body.get("capabilities"));
    let mut results = Vec::new();
    for target in targets {
        results.push(test_model(&state.client, &target, &capabilities).await);
    }
    admin_json(json!({ "ok": true, "results": results }))
}

pub async fn static_ui() -> Response {
    embedded_response("index.html")
}

#[allow(dead_code)]
const LIVE_STATUS_SHARE_HTML: &str = r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><title>Failover Proxy · 实时状态</title><style>body{margin:0;background:#07111f;color:#e5edf8;font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}main{max-width:1100px;margin:0 auto;padding:32px 20px}h1{margin:0;font-size:26px}.sub{color:#9badc8;margin:8px 0 24px}.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.card{background:#101d31;border:1px solid #274563;border-radius:12px;padding:18px}.label{font-size:13px;color:#9badc8}.value{font-size:26px;font-weight:700;margin-top:8px}.thread{margin-top:14px}.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;overflow-wrap:anywhere}.empty{color:#9badc8;text-align:center;padding:36px}.error{color:#ff9c9c}@media(max-width:700px){.grid{grid-template-columns:1fr}}</style></head><body><main><h1>实时状况</h1><p class="sub">只读分享链接 · 每秒自动刷新</p><section id="summary" class="grid"></section><section id="threads"></section></main><script>const esc=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));const bytes=v=>{v=Number(v)||0;if(!v)return'-';const u=['B','KB','MB','GB','TB'];const i=Math.min(u.length-1,Math.floor(Math.log(v)/Math.log(1024)));return(v/1024**i).toFixed(i?1:0)+' '+u[i]};const endpoint='/api/share/live-status/'+encodeURIComponent(location.pathname.split('/').pop());async function refresh(){try{const res=await fetch(endpoint,{cache:'no-store',credentials:'omit'});if(!res.ok)throw Error('分享链接无效或已过期');const data=await res.json(),threads=data.activeThreads||[],memory=data.memory||{},metric=memory.primaryMetric||'workingSetBytes',usage=memory[metric];document.querySelector('#summary').innerHTML=`<div class="card"><div class="label">活动线程</div><div class="value">${threads.length}</div></div><div class="card"><div class="label">涉及链路</div><div class="value">${new Set(threads.map(x=>x.chainName)).size}</div></div><div class="card"><div class="label">内存占用</div><div class="value">${bytes(usage)}</div><div class="label">${esc(memory.primaryMetricLabel||metric)}</div></div>`;document.querySelector('#threads').innerHTML=threads.length?threads.map(t=>`<article class="card thread"><strong>${esc(t.chainName)}</strong><div class="label">${esc(t.status)}</div><p>请求模型：<span class="mono">${esc(t.requestedModel||'-')}</span></p><p>正在尝试：<span class="mono">${esc(t.targetModel||t.targetName||'-')}</span></p><p>尝试次数：${esc(t.attempt||0)} / ${esc(t.maxAttempts||0)}</p></article>`).join(''):'<div class="card empty">当前没有活动线程</div>'}catch(error){document.querySelector('#threads').innerHTML=`<div class="card error">${esc(error.message)}</div>`}}refresh();setInterval(refresh,1000);</script></body></html>"#;

#[allow(dead_code)]
fn share_live_status_html() -> &'static str {
    r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="referrer" content="no-referrer"><link rel="stylesheet" href="/app.css"><title>Failover Proxy · 实时状况</title></head><body data-theme="dark" class="uiverse-shell min-h-dvh bg-slate-100"><main class="mx-auto max-w-7xl p-3 pb-8 sm:p-4 md:p-6 lg:p-8"><div class="space-y-6"><div class="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between"><div><div class="inline-flex w-fit items-center gap-2 rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700"><span class="h-2.5 w-2.5 rounded-full bg-current"></span>Live Threads</div><h2 class="mt-3 text-2xl font-bold text-slate-800">实时状况</h2><p class="mt-1 text-slate-500">查看代理 API 当前创建的线程、正在尝试的目标模型和进程内存占用。</p></div><div class="inline-flex w-fit items-center gap-2 self-start rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700 xl:self-auto"><span class="inline-block h-3 w-3 rounded-full border-2 border-current border-l-transparent"></span>自动刷新</div></div><section id="summary" class="grid grid-cols-1 gap-3 md:grid-cols-3"></section><section id="threads" class="live-status-thread-area space-y-3"></section></div></main><script>const esc=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));const bytes=v=>{v=Number(v)||0;if(!v)return'-';const u=['B','KB','MB','GB','TB'];const i=Math.min(u.length-1,Math.floor(Math.log(v)/Math.log(1024)));return(v/1024**i).toFixed(i?1:0)+' '+u[i]};const endpoint='/api/share/live-status/'+encodeURIComponent(location.pathname.split('/').pop());async function refresh(){const threadsEl=document.querySelector('#threads');try{const res=await fetch(endpoint,{cache:'no-store',credentials:'omit'});if(!res.ok)throw Error('分享链接无效或已撤销');const data=await res.json(),threads=data.activeThreads||[],memory=data.memory||{},metric=memory.primaryMetric||'workingSetBytes',usage=memory[metric];document.querySelector('#summary').innerHTML=`<div class="motion-card rounded-xl border border-slate-200 bg-white p-4"><div class="flex items-center justify-between"><span class="text-sm text-slate-500">活动线程</span><span class="h-2.5 w-2.5 rounded-full bg-blue-500"></span></div><p class="mt-2 text-2xl font-bold text-slate-800">${threads.length}</p></div><div class="motion-card rounded-xl border border-slate-200 bg-white p-4"><div class="flex items-center justify-between"><span class="text-sm text-slate-500">涉及链路</span><span class="h-2.5 w-2.5 rounded-full bg-violet-500"></span></div><p class="mt-2 text-2xl font-bold text-slate-800">${new Set(threads.map(x=>x.chainName)).size}</p></div><div class="motion-card rounded-xl border border-slate-200 bg-white p-4"><div class="flex items-center justify-between"><span class="text-sm text-slate-500">内存占用</span><span class="h-2.5 w-2.5 rounded-full bg-emerald-500"></span></div><p class="mt-2 text-2xl font-bold text-slate-800">${bytes(usage)}</p><p class="mt-1 text-xs text-slate-400">${esc(memory.primaryMetricLabel||metric)}</p></div>`;threadsEl.innerHTML=threads.length?threads.map(t=>`<article class="motion-card rounded-xl border border-slate-200 bg-white p-4"><div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between"><div class="min-w-0"><div class="flex flex-wrap items-center gap-2"><span class="inline-flex h-7 w-7 items-center justify-center rounded-lg bg-blue-50 text-blue-700"><span class="h-2.5 w-2.5 rounded-full bg-current"></span></span><h3 class="truncate font-semibold text-slate-800">${esc(t.chainName)}</h3><span class="rounded-full border border-blue-200 bg-blue-50 px-2 py-1 text-xs text-blue-700">${esc(t.phase||'调用中')}</span></div><p class="mt-2 text-sm text-slate-600">${esc(t.status)}</p></div></div><div class="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3"><div class="rounded-lg bg-slate-50 p-3"><p class="text-xs text-slate-400">请求模型</p><p class="mt-1 truncate font-mono text-sm text-slate-700">${esc(t.requestedModel||'-')}</p></div><div class="rounded-lg bg-slate-50 p-3"><p class="text-xs text-slate-400">正在尝试</p><p class="mt-1 truncate font-mono text-sm text-blue-700">${esc(t.targetModel||t.targetName||'-')}</p></div><div class="rounded-lg bg-slate-50 p-3"><p class="text-xs text-slate-400">尝试次数</p><p class="mt-1 font-mono text-sm text-slate-700">${esc(t.attempt||0)} / ${esc(t.maxAttempts||0)}</p></div></div></article>`).join(''):'<div class="rounded-xl border border-dashed border-slate-200 bg-white py-16 text-center"><p class="text-sm text-slate-500">当前没有活动线程</p></div>'}catch(error){threadsEl.innerHTML=`<div class="rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">${esc(error.message)}</div>`}}refresh();setInterval(refresh,1000);</script></body></html>"#
}

fn admin_json<T: Serialize>(value: T) -> Response {
    no_store(Json(value).into_response())
}

pub async fn app_css() -> Response {
    embedded_response("app.css")
}

pub async fn app_js() -> Response {
    embedded_response("app.js")
}

pub async fn app_core_js() -> Response {
    embedded_response("app-core.js")
}

pub async fn static_chunk(Path(path): Path<String>) -> Response {
    embedded_response(&format!("chunks/{}", sanitize_asset_path(&path)))
}

pub async fn static_asset(Path(path): Path<String>) -> Response {
    embedded_response(&sanitize_asset_path(&path))
}

fn embedded_response(path: &str) -> Response {
    let Some((body, content_type)) = embedded_asset(path) else {
        return send_error(StatusCode::NOT_FOUND, "Not Found", None);
    };
    named_asset(path, body, content_type)
}

fn named_asset(name: &str, body: &'static [u8], content_type: &'static str) -> Response {
    let mut response = Response::new(Body::from(Bytes::from_static(body)));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("inline; filename=\"{}\"", name))
            .unwrap_or_else(|_| HeaderValue::from_static("inline")),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate, max-age=0"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert(header::EXPIRES, HeaderValue::from_static("0"));
    response
}

fn sanitize_asset_path(path: &str) -> String {
    path.split('/')
        .filter(|segment| {
            !segment.is_empty() && *segment != "." && *segment != ".." && !segment.contains('\\')
        })
        .collect::<Vec<_>>()
        .join("/")
}

async fn test_model(client: &reqwest::Client, target: &Value, capabilities: &[String]) -> Value {
    let started = crate::stats::now_ms();
    let mut results = Vec::new();
    for capability in capabilities {
        results.push(run_capability(client, target, capability).await);
    }
    let base_url = target.get("baseUrl").and_then(Value::as_str).unwrap_or("");
    let model_name = target
        .get("modelName")
        .and_then(Value::as_str)
        .unwrap_or("");
    json!({
        "id": target.get("id").and_then(Value::as_str).unwrap_or(""),
        "providerId": target.get("providerId").and_then(Value::as_str).unwrap_or(""),
        "providerName": target.get("providerName").or_else(|| target.get("name")).and_then(Value::as_str).unwrap_or(base_url),
        "baseUrl": base_url,
        "modelName": model_name,
        "startedAt": started,
        "latencyMs": crate::stats::now_ms().saturating_sub(started),
        "results": results
    })
}

async fn run_capability(client: &reqwest::Client, target: &Value, capability: &str) -> Value {
    let body = match capability {
        "tool" => json!({
            "messages": [{"role": "user", "content": "Return the current city by calling the tool."}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_city",
                    "description": "Get city",
                    "parameters": {"type": "object", "properties": {}, "required": []}
                }
            }],
            "tool_choice": "auto",
            "stream": false
        }),
        "vision" => json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "What color is this image?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII="}}
            ]}],
            "stream": false
        }),
        _ => json!({
            "messages": [{"role": "user", "content": "Say OK in one short sentence."}],
            "stream": false
        }),
    };
    let started = crate::stats::now_ms();
    let base_url = target
        .get("baseUrl")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches('/');
    let model_name = target
        .get("modelName")
        .and_then(Value::as_str)
        .unwrap_or("");
    let api_key = target_api_key(target);
    let mut payload = body;
    payload["model"] = json!(model_name);
    let res = client
        .post(format!("{}/chat/completions", base_url))
        .timeout(std::time::Duration::from_millis(45_000))
        .bearer_auth(api_key)
        .json(&payload)
        .send()
        .await;
    match res {
        Ok(res) => {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            if status.is_success() {
                json!({
                    "capability": capability,
                    "status": "passed",
                    "latencyMs": crate::stats::now_ms().saturating_sub(started),
                    "detail": "Request completed",
                    "evidence": crate::proxy::trim_error(&text)
                })
            } else {
                json!({
                    "capability": capability,
                    "status": "failed",
                    "latencyMs": crate::stats::now_ms().saturating_sub(started),
                    "detail": format!("HTTP {}", status.as_u16()),
                    "evidence": crate::proxy::trim_error(&text)
                })
            }
        }
        Err(err) => json!({
            "capability": capability,
            "status": "failed",
            "detail": if err.is_timeout() { "Request timed out".to_string() } else { err.to_string() },
            "evidence": ""
        }),
    }
}

fn target_api_key(target: &Value) -> &str {
    target
        .get("apiKey")
        .and_then(Value::as_str)
        .filter(|key| !key.trim().is_empty())
        .or_else(|| {
            target
                .get("apiKeys")
                .and_then(Value::as_array)
                .and_then(|keys| keys.iter().find_map(Value::as_str))
        })
        .unwrap_or("")
}

fn normalize_capabilities(value: Option<&Value>) -> Vec<String> {
    let allowed = ["text", "vision", "tool"];
    let mut out = value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .filter(|item| allowed.contains(item))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    out.sort();
    out.dedup();
    if out.is_empty() {
        vec!["text".to_string(), "vision".to_string(), "tool".to_string()]
    } else {
        out
    }
}

fn merge_json(target: &mut Value, patch: Value) {
    if let (Some(target), Some(patch)) = (target.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            target.insert(key.clone(), value.clone());
        }
    }
}

pub fn spawn_runtime_state_cleanup(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let cfg = state.config.read().await.clone();
            let models = state.model_source.runtime_models(&cfg).await;
            cleanup_runtime_state(&state, &models).await;
        }
    });
}

pub async fn cleanup_runtime_state(state: &AppState, models: &[ModelConfig]) {
    state.proxy_runtime.cleanup_expired_api_key_cooldowns();
    state.circuit_breakers.cleanup_expired();
    state.proxy_runtime.retain_round_robin_models(models);
    state.circuit_breakers.retain_targets(models);
    state.stats.cleanup_runtime_models(models).await;
}

async fn runtime_state_memory(state: &AppState, process: &Value) -> Value {
    let circuit_breakers = state.circuit_breakers.memory_usage();
    let api_key_cooldowns = state.proxy_runtime.api_key_cooldown_memory_usage();
    let round_robin = state.proxy_runtime.round_robin_memory_usage();
    let model_statistics = state.stats.model_statistics_memory_usage().await;
    let provider_health_cache = state.provider_health.memory_usage().await;
    let model_source_cache = state.model_source.memory_usage().await;
    let active_threads = state.proxy_runtime.active_thread_memory_usage();
    let request_logs = state.stats.request_log_memory_usage().await;
    let admin_sessions = state.auth.memory_usage();
    let accounted_table_bytes = [
        circuit_breakers.estimated_bytes,
        api_key_cooldowns.estimated_bytes,
        round_robin.estimated_bytes,
        model_statistics.estimated_bytes,
        provider_health_cache.estimated_bytes,
        model_source_cache.estimated_bytes,
        active_threads.estimated_bytes,
        request_logs.estimated_bytes,
        admin_sessions.estimated_bytes,
    ]
    .into_iter()
    .sum::<usize>();
    let (process_bytes, process_metric) = [
        ("privateCommitBytes", "私有提交内存"),
        ("privateResidentBytes", "私有驻留内存"),
        ("physicalFootprintBytes", "物理足迹"),
        ("pssBytes", "比例驻留内存"),
        ("rssBytes", "驻留内存"),
    ]
    .into_iter()
    .find_map(|(key, label)| process.get(key)?.as_u64().map(|value| (value as usize, label)))
    .unwrap_or((0, "进程内存"));
    json!({
        "circuitBreakers": circuit_breakers,
        "apiKeyCooldowns": api_key_cooldowns,
        "roundRobin": round_robin,
        "modelStatistics": model_statistics,
        "providerHealthCache": provider_health_cache,
        "modelSourceCache": model_source_cache,
        "activeThreads": active_threads,
        "requestLogs": request_logs,
        "adminSessions": admin_sessions,
        "processRemainder": {
            "accountedTableBytes": accounted_table_bytes,
            "processBytes": process_bytes,
            "processMetricLabel": process_metric,
            "unattributedBytes": process_bytes.saturating_sub(accounted_table_bytes),
            "note": "未归类部分包括 Tokio 线程栈、代码和动态库、TLS、请求/响应体、连接池及系统或分配器保留页；它不能直接等同于内存碎片"
        },
        "networkBuffers": {
            "measuredBytes": Value::Null,
            "activeStreamCount": active_threads.entries,
            "maxIdleConnectionsPerHost": 32,
            "idleTimeoutSeconds": 90,
            "note": "reqwest/hyper 没有公开每条连接的发送/接收缓冲区大小；这里显示连接池上限和当前流式请求数，不把它猜成字节数"
        },
        "allocator": {
            "name": if cfg!(feature = "mimalloc") { "mimalloc" } else { "系统分配器" },
            "fragmentationBytes": Value::Null,
            "note": if cfg!(feature = "mimalloc") {
                "当前构建使用 mimalloc，但未启用其统计导出接口，不能可靠拆出碎片字节"
            } else {
                "系统分配器未提供跨平台碎片统计；进程剩余项可用于观察总体保留内存"
            }
        },
        "estimateNote": "各表已包含内容容量、哈希桶、DashMap 分片和锁的静态占用；BTreeMap 内部节点及分配器按页取整无法由 Rust 标准库精确读取"
    })
}

fn process_memory() -> Value {
    #[cfg(windows)]
    {
        windows_process_memory()
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        proc_status_memory()
    }
    #[cfg(target_os = "macos")]
    {
        macos_process_memory()
    }
    #[cfg(all(not(windows), not(unix)))]
    {
        json!({
            "platform": "unknown",
            "pid": std::process::id(),
            "primaryMetric": "residentBytes",
            "primaryMetricLabel": "当前驻留内存",
            "residentBytes": 0u64,
            "collectionError": "该操作系统尚未实现原生内存采样"
        })
    }
}

#[cfg(windows)]
fn windows_process_memory() -> Value {
    use std::ffi::c_void;

    #[repr(C)]
    #[allow(non_snake_case)]
    struct ProcessMemoryCountersEx {
        cb: u32,
        PageFaultCount: u32,
        PeakWorkingSetSize: usize,
        WorkingSetSize: usize,
        QuotaPeakPagedPoolUsage: usize,
        QuotaPagedPoolUsage: usize,
        QuotaPeakNonPagedPoolUsage: usize,
        QuotaNonPagedPoolUsage: usize,
        PagefileUsage: usize,
        PeakPagefileUsage: usize,
        PrivateUsage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
    }

    #[link(name = "psapi")]
    extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut c_void,
            counters: *mut ProcessMemoryCountersEx,
            cb: u32,
        ) -> i32;
    }

    let mut counters = ProcessMemoryCountersEx {
        cb: std::mem::size_of::<ProcessMemoryCountersEx>() as u32,
        PageFaultCount: 0,
        PeakWorkingSetSize: 0,
        WorkingSetSize: 0,
        QuotaPeakPagedPoolUsage: 0,
        QuotaPagedPoolUsage: 0,
        QuotaPeakNonPagedPoolUsage: 0,
        QuotaNonPagedPoolUsage: 0,
        PagefileUsage: 0,
        PeakPagefileUsage: 0,
        PrivateUsage: 0,
    };
    let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) } != 0;
    json!({
        "platform": "windows",
        "pid": std::process::id(),
        "primaryMetric": "workingSetBytes",
        "primaryMetricLabel": "工作集（当前驻留物理内存，含共享页）",
        "workingSetBytes": if ok { counters.WorkingSetSize as u64 } else { 0 },
        "peakWorkingSetBytes": if ok { counters.PeakWorkingSetSize as u64 } else { 0 },
        "privateCommitBytes": if ok { counters.PrivateUsage as u64 } else { 0 },
        "collectionError": if ok { Value::Null } else { json!("GetProcessMemoryInfo 调用失败") }
    })
}

#[cfg(all(unix, not(target_os = "macos")))]
fn proc_status_memory() -> Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let read_kb = |contents: &str, name: &str| -> u64 {
        contents
            .lines()
            .find_map(|line| {
                let value = line.strip_prefix(name)?.trim();
                value
                    .split_whitespace()
                    .next()
                    .and_then(|item| item.parse::<u64>().ok())
            })
            .unwrap_or(0)
            * 1024
    };
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok();
    let rollup = rollup.as_deref().unwrap_or_default();
    let rollup_rss = read_kb(rollup, "Rss:");
    let rss = if rollup_rss > 0 {
        rollup_rss
    } else {
        read_kb(&status, "VmRSS:")
    };
    let pss = read_kb(rollup, "Pss:");
    let private_resident =
        read_kb(rollup, "Private_Clean:").saturating_add(read_kb(rollup, "Private_Dirty:"));
    let primary_is_pss = pss > 0;
    json!({
        "platform": "linux",
        "pid": std::process::id(),
        "primaryMetric": if primary_is_pss { "pssBytes" } else { "rssBytes" },
        "primaryMetricLabel": if primary_is_pss {
            "比例驻留内存（PSS，已分摊共享页）"
        } else {
            "驻留内存（RSS，smaps_rollup 不可用时回退）"
        },
        "rssBytes": rss,
        "pssBytes": pss,
        "privateResidentBytes": private_resident,
        "swapBytes": read_kb(rollup, "Swap:"),
        "peakRssBytes": read_kb(&status, "VmHWM:"),
        "virtualBytes": read_kb(&status, "VmSize:"),
        "dataBytes": read_kb(&status, "VmData:"),
        "collectionError": if rollup.is_empty() {
            json!("无法读取 /proc/self/smaps_rollup，已回退为 RSS")
        } else {
            Value::Null
        }
    })
}

#[cfg(target_os = "macos")]
fn macos_process_memory() -> Value {
    use std::ffi::c_void;

    #[repr(C)]
    struct RUsageInfoV2 {
        _uuid_and_prefix: [u8; 16 + 6 * std::mem::size_of::<u64>()],
        resident_size: u64,
        phys_footprint: u64,
        _remaining: [u64; 10],
    }

    #[link(name = "proc")]
    extern "C" {
        fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut c_void) -> i32;
    }

    const RUSAGE_INFO_V2: i32 = 2;
    let mut usage: RUsageInfoV2 = unsafe { std::mem::zeroed() };
    let ok = unsafe {
        proc_pid_rusage(
            std::process::id() as i32,
            RUSAGE_INFO_V2,
            &mut usage as *mut RUsageInfoV2 as *mut c_void,
        )
    } == 0;
    json!({
        "platform": "macos",
        "pid": std::process::id(),
        "primaryMetric": "physicalFootprintBytes",
        "primaryMetricLabel": "物理足迹（macOS 内存压力计入值）",
        "physicalFootprintBytes": if ok { usage.phys_footprint } else { 0 },
        "rssBytes": if ok { usage.resident_size } else { 0 },
        "collectionError": if ok { Value::Null } else { json!("proc_pid_rusage 调用失败") }
    })
}

fn send_error(status: StatusCode, message: &str, details: Option<Value>) -> Response {
    let body = match details {
        Some(details) => {
            json!({ "error": { "message": message, "type": "proxy_error", "details": details } })
        }
        None => json!({ "error": { "message": message, "type": "proxy_error" } }),
    };
    no_store((status, Json(body)).into_response())
}

#[cfg(test)]
mod memory_tests {
    #[cfg(windows)]
    use super::windows_process_memory;

    #[cfg(target_os = "linux")]
    use super::proc_status_memory;

    #[cfg(windows)]
    #[test]
    fn windows_memory_does_not_label_commit_usage_as_virtual_memory() {
        let memory = windows_process_memory();

        assert!(memory["workingSetBytes"].as_u64().unwrap_or_default() > 0);
        assert_eq!(memory["platform"], "windows");
        assert_eq!(memory["primaryMetric"], "workingSetBytes");
        assert!(memory.get("virtualBytes").is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_memory_uses_kernel_resident_and_proportional_metrics() {
        let memory = proc_status_memory();

        assert_eq!(memory["platform"], "linux");
        assert!(memory["rssBytes"].as_u64().unwrap_or_default() > 0);
        assert!(matches!(
            memory["primaryMetric"].as_str(),
            Some("pssBytes" | "rssBytes")
        ));
        assert!(memory.get("workingSetBytes").is_none());
    }
}
