mod executor_admin;

use std::{collections::HashMap, sync::Arc};

use alloy_primitives::Address;
use anyhow::Result;
use axum::{
    extract::{DefaultBodyLimit, Form, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, DiscoveredPool, PoolVariant},
};
use base_arb_storage::{
    postgres::{ensure_registry_schema, PostgresStore},
    redis::RedisStore,
    TickStateStore,
};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sqlx::{FromRow, PgPool, Row};
use tracing::info;
use tracing_subscriber::EnvFilter;

const LOGO_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="Dexmoney logo">
  <defs>
    <linearGradient id="g" x1="10" y1="8" x2="54" y2="58" gradientUnits="userSpaceOnUse">
      <stop stop-color="#4ad295"/>
      <stop offset="0.52" stop-color="#22a7f2"/>
      <stop offset="1" stop-color="#ffb347"/>
    </linearGradient>
  </defs>
  <rect width="64" height="64" rx="16" fill="#0d1117"/>
  <path d="M17 37.5c2.4 8.1 11.3 12.5 19.2 9.5 5.2-2 8.6-6.5 9.5-11.6" fill="none" stroke="url(#g)" stroke-width="5.6" stroke-linecap="round"/>
  <path d="M47.5 25.7c-2.9-7.2-11.3-11-18.8-8.1-4.8 1.8-8.1 5.9-9.2 10.7" fill="none" stroke="url(#g)" stroke-width="5.6" stroke-linecap="round"/>
  <path d="M46.5 35.3l7.7 1.4-5.3 5.7-2.4-7.1Z" fill="#ffb347"/>
  <path d="M18.2 28.7l-7.7-1.5 5.4-5.6 2.3 7.1Z" fill="#4ad295"/>
  <circle cx="32" cy="32" r="8.8" fill="#101923" stroke="#e6edf3" stroke-width="2.2"/>
  <path d="M28 32h8" stroke="#4ad295" stroke-width="3" stroke-linecap="round"/>
  <path d="M32 28v8" stroke="#22a7f2" stroke-width="3" stroke-linecap="round"/>
</svg>"##;
const UNI_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;
const MONITOR_FORM_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    pool: Arc<PgPool>,
    tick_store: RedisStore,
    settings: Settings,
    provider: ChainProvider,
    admin_password: Option<String>,
}

#[derive(Debug, FromRow)]
struct DexEventRow {
    created_at: DateTime<Utc>,
    block_number: i64,
    dex: String,
    event_type: String,
    pool_address: String,
    tx_hash: String,
}

#[derive(Debug, FromRow)]
struct UnknownTopicRow {
    last_seen: DateTime<Utc>,
    dex: String,
    pool_address: String,
    topic0: Option<String>,
    event_count: i64,
}

#[derive(Debug, FromRow)]
struct PoolStateRow {
    updated_at: DateTime<Utc>,
    block_number: i64,
    source: String,
    dex: String,
    variant: Option<String>,
    pool_address: String,
    token0: String,
    token1: String,
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    fee: Option<i64>,
    reserve0: Option<String>,
    reserve1: Option<String>,
    sqrt_price_x96: Option<String>,
    liquidity: Option<String>,
    tick: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct TickCoverage {
    count: usize,
    latest_updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
struct PoolStateWarningRow {
    created_at: DateTime<Utc>,
    pool_address: String,
    dex: String,
    variant: String,
    block_number: i64,
    drift_bps: i64,
    message: String,
}

#[derive(Debug, FromRow)]
struct PoolStateValidationRow {
    created_at: DateTime<Utc>,
    pool_address: String,
    dex: String,
    variant: String,
    block_number: i64,
    drift_bps: i64,
    passed: bool,
    message: String,
}

#[derive(Debug, FromRow)]
struct OpportunityRow {
    id: String,
    created_at: DateTime<Utc>,
    block_number: i64,
    strategy: String,
    path_name: String,
    amount_in: String,
    expected_profit: String,
    quote_modes: Option<String>,
    ticks_used: Option<i64>,
    crossed_ticks: Option<i64>,
    tick_range_exhausted: Option<bool>,
    v3_pools_without_ticks: Option<i64>,
    status: String,
}

#[derive(Debug, FromRow)]
struct SimulationRow {
    created_at: DateTime<Utc>,
    opportunity_id: String,
    path_name: String,
    success: bool,
    simulated_profit: Option<String>,
    gas_estimate: Option<String>,
    gas_cost_cap: Option<String>,
    gas_cost_expected: Option<String>,
    net_simulated_profit: Option<String>,
    max_fee_per_gas: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    block_number: Option<i64>,
    revert_reason: Option<String>,
}

#[derive(Debug, FromRow)]
struct TransactionRow {
    created_at: DateTime<Utc>,
    eoa: String,
    tx_hash: Option<String>,
    nonce: i64,
    status: String,
    realized_profit: Option<String>,
    revert_reason: Option<String>,
}

#[derive(Debug, FromRow)]
struct TokenPairRow {
    created_at: DateTime<Utc>,
    chain_id: i64,
    symbol: String,
    token0: String,
    token1: String,
    enabled: bool,
    token0_search_amounts: Option<String>,
    token1_search_amounts: Option<String>,
    token0_min_profit: Option<String>,
    token1_min_profit: Option<String>,
    token0_default_search_amounts: Option<String>,
    token1_default_search_amounts: Option<String>,
    token0_default_min_profit: Option<String>,
    token1_default_min_profit: Option<String>,
}

#[derive(Debug, FromRow)]
struct TokenSearchDefaultRow {
    chain_id: i64,
    symbol: String,
    token_address: String,
    enabled: bool,
    search_amounts: Option<String>,
    min_profit: Option<String>,
    two_hop_search_amounts: Option<String>,
    two_hop_min_profit: Option<String>,
    multihop_search_amounts: Option<String>,
    multihop_min_profit: Option<String>,
}

#[derive(Debug, FromRow)]
struct PoolRegistryRow {
    created_at: DateTime<Utc>,
    last_update_time: Option<DateTime<Utc>>,
    dex: String,
    variant: String,
    pair_symbol: Option<String>,
    pool_address: String,
    factory_address: Option<String>,
    token0: String,
    token1: String,
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    fee_bps: Option<i64>,
    stable: Option<bool>,
    enabled: bool,
    source: String,
}

#[derive(Debug, FromRow)]
struct ObservedPoolRow {
    updated_at: DateTime<Utc>,
    symbol: Option<String>,
    pool_address: String,
    family: String,
    dex: Option<String>,
    variant: Option<String>,
    factory_address: Option<String>,
    txs_30d: i64,
    logs_30d: i64,
    import_status: String,
    import_reason: Option<String>,
}

#[derive(Debug, FromRow)]
struct FactoryRegistryRow {
    updated_at: DateTime<Utc>,
    factory_address: String,
    dex: String,
    variant: String,
    trusted: bool,
    enabled: bool,
    source: String,
    notes: Option<String>,
    observed_pools: i64,
    first_seen_block: Option<i64>,
    latest_seen_block: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AddTokenForm {
    password: String,
    token_address: String,
}

#[derive(Debug, Deserialize)]
struct RediscoverPairForm {
    password: String,
    token0: String,
    token1: String,
}

#[derive(Debug, Deserialize)]
struct RediscoverAllPairsForm {
    password: String,
}

#[derive(Debug, Deserialize)]
struct ReconcileRegistryForm {
    password: String,
}

#[derive(Debug, Deserialize)]
struct ImportObservedPoolsForm {
    password: String,
    address: String,
    days: Option<i64>,
    min_pool_txs: Option<i64>,
    pool_limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct FactoryRegistryForm {
    password: String,
    factory_address: String,
    dex: String,
    variant: String,
    trusted: Option<String>,
    enabled: Option<String>,
    notes: String,
}

#[derive(Debug, Deserialize)]
struct DeletePairForm {
    password: String,
    token0: String,
    token1: String,
}

#[derive(Debug, Deserialize)]
struct SearchConfigForm {
    password: String,
    token0: String,
    token1: String,
    token0_search_amounts: String,
    token1_search_amounts: String,
    token0_min_profit: String,
    token1_min_profit: String,
}

#[derive(Debug, Deserialize)]
struct TokenSearchDefaultForm {
    password: String,
    token_address: String,
    executor_scope: Option<String>,
    search_amounts: String,
    min_profit: String,
}

#[derive(Debug, Clone)]
struct TokenUpdateChange {
    symbol: String,
    token_address: String,
    scope: String,
    old_search_amounts: Option<String>,
    new_search_amounts: Option<String>,
    old_min_profit: Option<String>,
    new_min_profit: Option<String>,
    became_anchor: bool,
}

#[derive(Debug, Clone)]
struct ObservedPoolCandidate {
    pool_address: String,
    topic0: String,
    family: String,
    swap_logs: i64,
    txs: i64,
    first_block: i64,
    latest_block: i64,
    registry_symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthQuery {
    password: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let pool = PgPool::connect(&settings.postgres_url).await?;
    let tick_store = RedisStore::connect(&settings.redis_url).await?;
    ensure_registry_schema(&pool).await?;
    let state = AppState {
        pool: Arc::new(pool),
        tick_store,
        provider: ChainProvider::from_settings(&settings),
        admin_password: settings.monitor_web_password.clone(),
        settings,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/registry", get(registry_page))
        .route("/registry/tokens", get(registry_tokens_page))
        .route("/registry/pairs", get(registry_pairs_page))
        .route("/registry/factories", get(registry_factories_page))
        .route("/registry/pools", get(registry_pools_page))
        .route("/registry/observed", get(registry_observed_page))
        .route("/activity", get(activity_page))
        .route("/execution", get(execution_page))
        .route("/tokens", post(add_token))
        .route("/pairs/rediscover", post(rediscover_pair))
        .route("/pairs/rediscover-all", post(rediscover_all_pairs))
        .route("/registry/reconcile", post(reconcile_registry))
        .route("/registry/import-observed", post(import_observed_pools))
        .route("/registry/factory", post(update_factory_registry))
        .route("/pairs/delete", post(delete_pair))
        .route("/pairs/remove", post(remove_pair))
        .route("/pairs/search-config", post(update_pair_search_config))
        .route("/tokens/search-default", post(update_token_search_default))
        .route(
            "/tokens/search-defaults",
            post(update_token_search_defaults),
        )
        .route("/favicon.ico", get(favicon))
        .route("/favicon.svg", get(favicon))
        .route("/healthz", get(healthz))
        .layer(DefaultBodyLimit::max(MONITOR_FORM_BODY_LIMIT_BYTES))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8085").await?;
    info!("monitor-web listening on 0.0.0.0:8085");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

async fn favicon() -> impl IntoResponse {
    ([("content-type", "image/svg+xml; charset=utf-8")], LOGO_SVG)
}

async fn index(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }

    let pool_states = fetch_pool_states(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let tick_coverage = fetch_tick_coverage(&state.tick_store, &pool_states)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let warnings = fetch_pool_state_warnings(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let validations = fetch_pool_state_validations(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let opportunities = fetch_opportunities(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
        <section class="card">
          <h2>Block Validations</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>State Warnings</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Pool States</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Opportunities</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
        render_pool_state_validations_table(&validations),
        render_pool_state_warnings_table(&warnings),
        render_pool_states_table(&pool_states, &tick_coverage),
        render_opportunities_table(&opportunities),
    );

    Ok(Html(render_page(
        "Overview",
        "Current monitored pools and recent opportunities.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn registry_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }

    render_registry_response(&state.pool, auth.password.as_deref(), None).await
}

async fn registry_tokens_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }
    let rows = fetch_token_search_defaults(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let content = format!(
        r#"{nav}
        <section class="card">
          <h2>Tokens</h2>
          <div class="card-body">{table}</div>
        </section>"#,
        nav = render_registry_nav(auth.password.as_deref(), "tokens"),
        table = render_token_search_defaults(&rows),
    );
    Ok(Html(render_page(
        "Registry / Tokens",
        "Funding anchors and token-level search defaults.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn registry_pairs_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }
    let rows = fetch_token_pairs(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let content = format!(
        r#"{nav}
        <section class="card">
          <h2>Token Pairs</h2>
          <div class="card-body">{table}</div>
        </section>"#,
        nav = render_registry_nav(auth.password.as_deref(), "pairs"),
        table = render_token_pairs_table(&rows),
    );
    Ok(Html(render_page(
        "Registry / Pairs",
        "Configured token pairs and pool discovery actions.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn registry_factories_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }
    let rows = fetch_factory_registry(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let content = format!(
        r#"{nav}
        <section class="card">
          <h2>Factories</h2>
          <div class="card-body">{table}</div>
        </section>"#,
        nav = render_registry_nav(auth.password.as_deref(), "factories"),
        table = render_factory_registry_table(&rows),
    );
    Ok(Html(render_page(
        "Registry / Factories",
        "Trusted factories and observed factory candidates.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn registry_pools_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }
    let rows = fetch_registry_pools(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let content = format!(
        r#"{nav}
        <section class="card">
          <h2>Pool Registry</h2>
          <div class="card-body">{table}</div>
        </section>"#,
        nav = render_registry_nav(auth.password.as_deref(), "pools"),
        table = render_registry_pools_table(&rows),
    );
    Ok(Html(render_page(
        "Registry / Pools",
        "Enabled pool registry rows and latest activity.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn registry_observed_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }
    let rows = fetch_observed_pools(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let content = format!(
        r#"{nav}
        <section class="card">
          <h2>Observed Pools</h2>
          <div class="card-body">{table}</div>
        </section>"#,
        nav = render_registry_nav(auth.password.as_deref(), "observed"),
        table = render_observed_pools_table(&rows),
    );
    Ok(Html(render_page(
        "Registry / Observed",
        "Pools observed from competitor traces and import status.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn activity_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }

    let events = fetch_dex_events(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let unknown_topics = fetch_unknown_topics(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let pool_states = fetch_pool_states(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let tick_coverage = fetch_tick_coverage(&state.tick_store, &pool_states)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
        <section class="card">
          <h2>DEX Events</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Unknown Topics</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Pool States</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
        render_events_table(&events),
        render_unknown_topics_table(&unknown_topics),
        render_pool_states_table(&pool_states, &tick_coverage),
    );

    Ok(Html(render_page(
        "Activity",
        "Recent chain events and state snapshots.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn execution_page(
    State(state): State<AppState>,
    Query(auth): Query<AuthQuery>,
) -> Result<Html<String>, axum::http::StatusCode> {
    if !password_matches_query(state.admin_password.as_deref(), auth.password.as_deref()) {
        return Ok(Html(render_login()));
    }

    let opportunities = fetch_opportunities(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let simulations = fetch_simulations(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let transactions = fetch_transactions(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
        <section class="card">
          <h2>Opportunities</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Simulations</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Transactions</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
        render_opportunities_table(&opportunities),
        render_simulations_table(&simulations),
        render_transactions_table(&transactions),
    );

    Ok(Html(render_page(
        "Execution",
        "Searcher, simulation, and transaction records.",
        auth.password.as_deref(),
        None,
        &content,
    )))
}

async fn add_token(
    State(state): State<AppState>,
    Form(form): Form<AddTokenForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let token: Address = match form.token_address.trim().parse() {
        Ok(token) => token,
        Err(_) => {
            return render_registry_response(&state.pool, None, Some("invalid token address"))
                .await;
        }
    };
    let symbol = token_symbol(&state.provider, token).await;
    if upsert_token_registry(&state.pool, state.settings.chain_id, token, &symbol)
        .await
        .is_err()
    {
        return render_registry_response(&state.pool, None, Some("token registry write failed"))
            .await;
    }

    let discovery = discover_pairs_for_new_token(&state, token).await;
    let message = format!(
        "added token {symbol} {token:#x}; {}",
        summarize_discovery("anchor pair discovery", discovery),
    );
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn rediscover_pair(
    State(state): State<AppState>,
    Form(form): Form<RediscoverPairForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let result = match discover_and_upsert_pair(&state, &form.token0, &form.token1).await {
        Ok(result) => result,
        Err(status) => {
            return render_registry_response(
                &state.pool,
                None,
                Some(discovery_error_message(status)),
            )
            .await;
        }
    };
    let message = format!(
        "rediscovered pair {symbol}; discovered {} pools; {}",
        result.discovered_count,
        result.executor_report,
        symbol = result.symbol,
    );
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn rediscover_all_pairs(
    State(state): State<AppState>,
    Form(form): Form<RediscoverAllPairsForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let pairs = fetch_enabled_token_pairs(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if pairs.is_empty() {
        return render_registry_response(
            &state.pool,
            None,
            Some("no enabled token pairs to rediscover"),
        )
        .await;
    }

    let mut succeeded = 0usize;
    let mut failed = Vec::new();
    let mut discovered_total = 0usize;

    for pair in pairs {
        match discover_and_upsert_pair(&state, &pair.token0, &pair.token1).await {
            Ok(result) => {
                succeeded += 1;
                discovered_total += result.discovered_count;
            }
            Err(status) => {
                failed.push(format!(
                    "{}: {}",
                    pair.symbol,
                    discovery_error_message(status)
                ));
            }
        }
    }

    let failure_summary = if failed.is_empty() {
        "no failures".to_string()
    } else {
        let mut shown = failed
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        if failed.len() > 4 {
            shown.push_str(&format!("; +{} more", failed.len() - 4));
        }
        shown
    };
    let message = format!(
        "rediscovered enabled pairs: {succeeded} succeeded, {} failed, {discovered_total} pools discovered; {failure_summary}",
        failed.len()
    );
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn reconcile_registry(
    State(state): State<AppState>,
    Form(form): Form<ReconcileRegistryForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let anchors = fetch_funding_anchors(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if anchors.is_empty() {
        return render_registry_response(
            &state.pool,
            None,
            Some("no funding anchors configured; set token defaults first"),
        )
        .await;
    }

    let mut total = TokenPairDiscoverySummary::default();
    for anchor in anchors {
        let summary = discover_pairs_for_anchor(&state, anchor).await;
        total.attempted += summary.attempted;
        total.succeeded += summary.succeeded;
        total.discovered_pools += summary.discovered_pools;
        total.skipped += summary.skipped;
        total.failed.extend(summary.failed);
    }

    let message = summarize_discovery("registry reconcile", total);
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn import_observed_pools(
    State(state): State<AppState>,
    Form(form): Form<ImportObservedPoolsForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }
    let address: Address = match form.address.trim().parse() {
        Ok(address) => address,
        Err(_) => {
            return render_registry_response(&state.pool, None, Some("invalid observed address"))
                .await;
        }
    };
    let days = form.days.unwrap_or(30).max(1);
    let min_pool_txs = form.min_pool_txs.unwrap_or(100).max(1);
    let pool_limit = form.pool_limit.unwrap_or(200).max(1);
    let to_block = state
        .provider
        .get_block_number()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let from_block =
        to_block.saturating_sub((days as u64).saturating_mul(APPROX_BASE_BLOCKS_PER_DAY));
    let rows = fetch_observed_pool_candidates(
        &state.pool,
        address,
        from_block,
        to_block,
        min_pool_txs,
        pool_limit,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut imported = 0usize;
    let mut observed_only = 0usize;
    let mut already_registered = 0usize;
    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };

    for row in rows {
        let pool_address: Address = match row.pool_address.parse() {
            Ok(address) => address,
            Err(err) => {
                observed_only += 1;
                info!(pool = %row.pool_address, error = %err, "observed pool address parse failed");
                continue;
            }
        };
        if row.registry_symbol.is_some() {
            already_registered += 1;
        }
        match state
            .provider
            .resolve_observed_pool_for_registry(&state.settings, pool_address, &row.topic0)
            .await
        {
            Ok(discovered) => {
                let symbol = pair_symbol(
                    &state.provider,
                    discovered.state.token0,
                    discovered.state.token1,
                )
                .await;
                upsert_token_registry(
                    &state.pool,
                    state.settings.chain_id,
                    discovered.state.token0,
                    symbol.split('/').next().unwrap_or_default(),
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                upsert_token_registry(
                    &state.pool,
                    state.settings.chain_id,
                    discovered.state.token1,
                    symbol.split('/').nth(1).unwrap_or_default(),
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let (token0, token1) =
                    canonical_pair(discovered.state.token0, discovered.state.token1);
                let pair_id = store
                    .upsert_token_pair(state.settings.chain_id, token0, token1, &symbol)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                store
                    .upsert_discovered_pool(pair_id, &discovered)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                upsert_observed_pool_row(
                    &store,
                    state.settings.chain_id,
                    &row,
                    pool_address,
                    Some(&discovered),
                    Some(&symbol),
                    "imported",
                    None,
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if let Some(factory) = discovered.factory_address {
                    store
                        .upsert_factory_registry(
                            state.settings.chain_id,
                            factory,
                            dex_to_string(discovered.state.dex),
                            variant_to_string(discovered.state.variant),
                            true,
                            true,
                            "observed_import",
                            Some("resolved through supported registry settings"),
                            Some(row.first_block),
                            Some(row.latest_block),
                            1,
                        )
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                }
                imported += 1;
            }
            Err(err) => {
                let metadata = state
                    .provider
                    .resolve_observed_pool_metadata(pool_address, &row.topic0)
                    .await
                    .ok();
                let symbol = match metadata.as_ref() {
                    Some(metadata) => {
                        Some(pair_symbol(&state.provider, metadata.token0, metadata.token1).await)
                    }
                    None => None,
                };
                store
                    .upsert_observed_pool(
                        state.settings.chain_id,
                        pool_address,
                        &row.topic0,
                        &row.family,
                        metadata.as_ref().map(|metadata| metadata.token0),
                        metadata.as_ref().map(|metadata| metadata.token1),
                        symbol.as_deref(),
                        metadata
                            .as_ref()
                            .and_then(|metadata| metadata.factory_address),
                        None,
                        None,
                        metadata.as_ref().and_then(|metadata| metadata.fee_bps),
                        metadata.as_ref().and_then(|metadata| metadata.fee_pips),
                        metadata.as_ref().and_then(|metadata| metadata.tick_spacing),
                        metadata.as_ref().and_then(|metadata| metadata.stable),
                        row.txs,
                        row.swap_logs,
                        Some(row.first_block),
                        Some(row.latest_block),
                        "observed_only",
                        Some(&err.to_string()),
                    )
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if let Some(factory) = metadata
                    .as_ref()
                    .and_then(|metadata| metadata.factory_address)
                {
                    store
                        .upsert_factory_registry(
                            state.settings.chain_id,
                            factory,
                            inferred_dex_for_topic(&row.topic0),
                            inferred_variant_for_topic(&row.topic0),
                            false,
                            true,
                            "observed_import",
                            Some(&err.to_string()),
                            Some(row.first_block),
                            Some(row.latest_block),
                            1,
                        )
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                }
                observed_only += 1;
            }
        }
    }

    let message = format!(
        "observed pool import: imported {imported}, observed-only {observed_only}, already registered {already_registered}, blocks {from_block}..{to_block}"
    );
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn update_factory_registry(
    State(state): State<AppState>,
    Form(form): Form<FactoryRegistryForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }
    let factory: Address = match form.factory_address.trim().parse() {
        Ok(factory) => factory,
        Err(_) => {
            return render_registry_response(&state.pool, None, Some("invalid factory address"))
                .await;
        }
    };
    if !factory_variant_is_consistent(&form.dex, &form.variant) {
        return render_registry_response(
            &state.pool,
            None,
            Some("invalid factory DEX/variant combination"),
        )
        .await;
    }
    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    let notes = form.notes.trim();
    store
        .upsert_factory_registry(
            state.settings.chain_id,
            factory,
            form.dex.trim(),
            form.variant.trim(),
            form.trusted.is_some(),
            form.enabled.is_some(),
            "manual",
            (!notes.is_empty()).then_some(notes),
            None,
            None,
            0,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let message = format!(
        "factory updated: {factory:#x} {} {} trusted={} enabled={}",
        form.dex.trim(),
        form.variant.trim(),
        form.trusted.is_some(),
        form.enabled.is_some()
    );
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn delete_pair(
    State(state): State<AppState>,
    Form(form): Form<DeletePairForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let token0: Address = form
        .token0
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token1: Address = form
        .token1
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let (token0, token1) = canonical_pair(token0, token1);

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    store
        .disable_token_pair(state.settings.chain_id, token0, token1)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    render_registry_response(&state.pool, None, Some("token pair disabled")).await
}

async fn remove_pair(
    State(state): State<AppState>,
    Form(form): Form<DeletePairForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let token0: Address = form
        .token0
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token1: Address = form
        .token1
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let (token0, token1) = canonical_pair(token0, token1);

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    let pools_deleted = store
        .delete_token_pair(state.settings.chain_id, token0, token1)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let message = format!("token pair deleted; removed {pools_deleted} associated pools");
    render_registry_response(&state.pool, None, Some(&message)).await
}

async fn update_pair_search_config(
    State(state): State<AppState>,
    Form(form): Form<SearchConfigForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let token0: Address = form
        .token0
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token1: Address = form
        .token1
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let (token0, token1) = canonical_pair(token0, token1);

    let token0_amounts = normalize_raw_amount_list(&form.token0_search_amounts)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token1_amounts = normalize_raw_amount_list(&form.token1_search_amounts)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token0_min_profit =
        normalize_raw_amount(&form.token0_min_profit).map_err(|_| StatusCode::BAD_REQUEST)?;
    let token1_min_profit =
        normalize_raw_amount(&form.token1_min_profit).map_err(|_| StatusCode::BAD_REQUEST)?;

    if token0_amounts.is_some() && token0_min_profit.is_none() {
        return render_registry_response(
            &state.pool,
            None,
            Some("token0 min profit raw is required when token0 search amounts are set"),
        )
        .await;
    }
    if token1_amounts.is_some() && token1_min_profit.is_none() {
        return render_registry_response(
            &state.pool,
            None,
            Some("token1 min profit raw is required when token1 search amounts are set"),
        )
        .await;
    }

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    store
        .update_token_pair_search_config(
            state.settings.chain_id,
            token0,
            token1,
            token0_amounts.as_deref(),
            token1_amounts.as_deref(),
            token0_min_profit.as_deref(),
            token1_min_profit.as_deref(),
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    render_registry_response(&state.pool, None, Some("pair search config updated")).await
}

async fn update_token_search_default(
    State(state): State<AppState>,
    Form(form): Form<TokenSearchDefaultForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let token_address: Address = form
        .token_address
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let search_amounts =
        normalize_raw_amount_list(&form.search_amounts).map_err(|_| StatusCode::BAD_REQUEST)?;
    let min_profit = normalize_raw_amount(&form.min_profit).map_err(|_| StatusCode::BAD_REQUEST)?;

    if search_amounts.is_some() != min_profit.is_some() {
        return render_registry_response(
            &state.pool,
            None,
            Some("token default amounts raw and min profit raw must be set together"),
        )
        .await;
    }

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    store
        .update_token_search_default(
            state.settings.chain_id,
            token_address,
            form.executor_scope.as_deref().unwrap_or("all"),
            search_amounts.as_deref(),
            min_profit.as_deref(),
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    render_registry_response(&state.pool, None, Some("token search default updated")).await
}

async fn update_token_search_defaults(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let password = form.get("password").map(String::as_str).unwrap_or_default();
    if !password_matches(state.admin_password.as_deref(), password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let rows = fetch_token_search_defaults(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let token_addresses = form
        .get("token_addresses")
        .map(String::as_str)
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>();
    if token_addresses.is_empty() {
        return render_registry_response(&state.pool, None, Some("no tokens submitted")).await;
    }

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    let mut changes = Vec::new();
    let mut discovery_results = Vec::new();

    for token_address in token_addresses {
        let token_address = token_address.trim().to_lowercase();
        let Some(row) = rows
            .iter()
            .find(|row| row.token_address.eq_ignore_ascii_case(&token_address))
        else {
            continue;
        };
        let token: Address = row
            .token_address
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        for (scope, amount_prefix, profit_prefix, old_amounts, old_profit) in [
            (
                "all",
                "search_amounts",
                "min_profit",
                row.search_amounts.clone(),
                row.min_profit.clone(),
            ),
            (
                "two_hop",
                "two_hop_search_amounts",
                "two_hop_min_profit",
                row.two_hop_search_amounts.clone(),
                row.two_hop_min_profit.clone(),
            ),
            (
                "multihop",
                "multihop_search_amounts",
                "multihop_min_profit",
                row.multihop_search_amounts.clone(),
                row.multihop_min_profit.clone(),
            ),
        ] {
            let amount_key = format!("{amount_prefix}_{token_address}");
            let profit_key = format!("{profit_prefix}_{token_address}");
            let search_amounts = match form.get(&amount_key) {
                Some(raw_amounts) => {
                    normalize_raw_amount_list(raw_amounts).map_err(|_| StatusCode::BAD_REQUEST)?
                }
                None => old_amounts.clone(),
            };
            let min_profit = match form.get(&profit_key) {
                Some(raw_min_profit) => {
                    normalize_raw_amount(raw_min_profit).map_err(|_| StatusCode::BAD_REQUEST)?
                }
                None => old_profit.clone(),
            };
            if search_amounts.is_some() != min_profit.is_some() {
                return render_registry_response(
                    &state.pool,
                    None,
                    Some("token default amounts raw and min profit raw must be set together"),
                )
                .await;
            }

            if search_amounts == old_amounts && min_profit == old_profit {
                continue;
            }

            let was_anchor = old_amounts.is_some() && old_profit.is_some();
            let became_anchor = !was_anchor && search_amounts.is_some() && min_profit.is_some();
            store
                .update_token_search_default(
                    state.settings.chain_id,
                    token,
                    scope,
                    search_amounts.as_deref(),
                    min_profit.as_deref(),
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            upsert_token_registry(&state.pool, state.settings.chain_id, token, &row.symbol)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let change = TokenUpdateChange {
                symbol: row.symbol.clone(),
                token_address: row.token_address.clone(),
                scope: scope.to_string(),
                old_search_amounts: old_amounts,
                new_search_amounts: search_amounts.clone(),
                old_min_profit: old_profit,
                new_min_profit: min_profit.clone(),
                became_anchor,
            };
            if became_anchor {
                discovery_results.push((
                    row.symbol.clone(),
                    discover_pairs_for_anchor(&state, token).await,
                ));
            }
            changes.push(change);
        }
    }

    let mut message = summarize_token_changes(&changes);
    if !discovery_results.is_empty() {
        let discovery_summary = discovery_results
            .into_iter()
            .map(|(symbol, result)| {
                format!(
                    "{symbol}: {}",
                    summarize_discovery("anchor rediscovery", result)
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        if !message.is_empty() {
            message.push_str("; ");
        }
        message.push_str(&discovery_summary);
    }
    if message.is_empty() {
        message = "no token defaults changed".to_string();
    }

    render_registry_response(&state.pool, None, Some(&message)).await
}

struct PairDiscoveryResult {
    symbol: String,
    discovered_count: usize,
    executor_report: String,
}

fn discovery_error_message(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "invalid token pair input",
        StatusCode::NOT_FOUND => {
            "no supported Aerodrome Classic, Aerodrome Slipstream, Uniswap V3, or Pancake V3 pools found for this pair"
        }
        StatusCode::BAD_GATEWAY => "pool discovery RPC failed; check monitor-web logs",
        StatusCode::INTERNAL_SERVER_ERROR => "pool registry database write failed",
        _ => "token pair discovery failed",
    }
}

async fn discover_and_upsert_pair(
    state: &AppState,
    token_a: &str,
    token_b: &str,
) -> Result<PairDiscoveryResult, StatusCode> {
    let token_a: Address = token_a
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let token_b: Address = token_b
        .trim()
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    discover_and_upsert_pair_addresses(state, token_a, token_b).await
}

async fn discover_and_upsert_pair_addresses(
    state: &AppState,
    token_a: Address,
    token_b: Address,
) -> Result<PairDiscoveryResult, StatusCode> {
    let (token0, token1) = canonical_pair(token_a, token_b);
    if token0 == token1 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let symbol = pair_symbol(&state.provider, token0, token1).await;
    let discovered = state
        .provider
        .discover_pools_for_pair(&state.settings, token0, token1)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    if discovered.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let store = PostgresStore {
        pool: (*state.pool).clone(),
    };
    upsert_token_registry(
        &state.pool,
        state.settings.chain_id,
        token0,
        symbol.split('/').next().unwrap_or_default(),
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    upsert_token_registry(
        &state.pool,
        state.settings.chain_id,
        token1,
        symbol.split('/').nth(1).unwrap_or_default(),
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let pair_id = store
        .upsert_token_pair(state.settings.chain_id, token0, token1, &symbol)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for pool in &discovered {
        store
            .upsert_discovered_pool(pair_id, pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }
    let executor_report = match executor_admin::configure_executor_for_pair(
        &state.provider,
        &state.settings,
        &discovered,
        token0,
        token1,
    )
    .await
    {
        Ok(report) => report.summary(),
        Err(err) => format!("executor auto-config failed after registry update: {err}"),
    };

    Ok(PairDiscoveryResult {
        symbol,
        discovered_count: discovered.len(),
        executor_report,
    })
}

#[derive(Default)]
struct TokenPairDiscoverySummary {
    attempted: usize,
    succeeded: usize,
    discovered_pools: usize,
    skipped: usize,
    failed: Vec<String>,
}

async fn discover_pairs_for_new_token(
    state: &AppState,
    token: Address,
) -> TokenPairDiscoverySummary {
    let anchors = fetch_funding_anchors(&state.pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|anchor| *anchor != token)
        .collect::<Vec<_>>();
    discover_pairs_for_token_set(state, token, anchors).await
}

async fn discover_pairs_for_anchor(state: &AppState, anchor: Address) -> TokenPairDiscoverySummary {
    let tokens = fetch_enabled_tokens(&state.pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|token| *token != anchor)
        .collect::<Vec<_>>();
    discover_pairs_for_token_set(state, anchor, tokens).await
}

async fn discover_pairs_for_token_set(
    state: &AppState,
    token: Address,
    peers: Vec<Address>,
) -> TokenPairDiscoverySummary {
    let mut summary = TokenPairDiscoverySummary::default();
    for peer in peers {
        summary.attempted += 1;
        match discover_and_upsert_pair_addresses(state, token, peer).await {
            Ok(result) => {
                summary.succeeded += 1;
                summary.discovered_pools += result.discovered_count;
            }
            Err(StatusCode::NOT_FOUND) => {
                summary.skipped += 1;
            }
            Err(status) => {
                summary
                    .failed
                    .push(discovery_error_message(status).to_string());
            }
        }
    }
    summary
}

fn summarize_discovery(label: &str, summary: TokenPairDiscoverySummary) -> String {
    let failure_summary = if summary.failed.is_empty() {
        "no failures".to_string()
    } else {
        let mut shown = summary
            .failed
            .iter()
            .take(3)
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        if summary.failed.len() > 3 {
            shown.push_str(&format!("; +{} more", summary.failed.len() - 3));
        }
        shown
    };
    format!(
        "{label}: {} attempted, {} pairs discovered, {} pools, {} no-pool skips, {failure_summary}",
        summary.attempted, summary.succeeded, summary.discovered_pools, summary.skipped
    )
}

async fn render_registry_response(
    _pool: &PgPool,
    auth_password: Option<&str>,
    flash: Option<&str>,
) -> Result<Html<String>, StatusCode> {
    let content = format!(
        r#"
        {registry_nav}
        <section class="admin">
          <form method="post" action="/tokens">
            <label>Password
              {password_input}
            </label>
            <label>Token
              <input name="token_address" placeholder="0x..." required>
            </label>
            <button type="submit">Add Token + Discover Anchor Pairs</button>
          </form>
          {rediscover_all_form}
          {reconcile_form}
          {import_observed_form}
        </section>
        <section class="registry-grid">
          {registry_cards}
        </section>
        "#,
        registry_nav = render_registry_nav(auth_password, "home"),
        registry_cards = render_registry_cards(auth_password),
        password_input = password_input(None, true),
        rediscover_all_form = render_rediscover_all_form(),
        reconcile_form = render_reconcile_form(),
        import_observed_form = render_import_observed_form(),
    );

    Ok(Html(render_page(
        "Registry",
        "Add tokens, configure funding anchors, and inspect discovered pools.",
        auth_password,
        flash,
        &content,
    )))
}

async fn fetch_token_pairs(pool: &PgPool) -> Result<Vec<TokenPairRow>> {
    Ok(sqlx::query_as::<_, TokenPairRow>(
        r#"
        SELECT
            tp.created_at,
            tp.chain_id,
            tp.symbol,
            tp.token0,
            tp.token1,
            tp.enabled,
            tp.token0_search_amounts,
            tp.token1_search_amounts,
            tp.token0_min_profit,
            tp.token1_min_profit,
            d0.search_amounts AS token0_default_search_amounts,
            d1.search_amounts AS token1_default_search_amounts,
            d0.min_profit AS token0_default_min_profit,
            d1.min_profit AS token1_default_min_profit
        FROM token_pairs tp
        LEFT JOIN token_search_defaults d0
          ON d0.chain_id = tp.chain_id
         AND d0.token_address = tp.token0
         AND d0.executor_scope = 'all'
        LEFT JOIN token_search_defaults d1
          ON d1.chain_id = tp.chain_id
         AND d1.token_address = tp.token1
         AND d1.executor_scope = 'all'
        ORDER BY tp.created_at DESC
        LIMIT 50
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_enabled_token_pairs(pool: &PgPool) -> Result<Vec<TokenPairRow>> {
    Ok(sqlx::query_as::<_, TokenPairRow>(
        r#"
        SELECT
            tp.created_at,
            tp.chain_id,
            tp.symbol,
            tp.token0,
            tp.token1,
            tp.enabled,
            tp.token0_search_amounts,
            tp.token1_search_amounts,
            tp.token0_min_profit,
            tp.token1_min_profit,
            d0.search_amounts AS token0_default_search_amounts,
            d1.search_amounts AS token1_default_search_amounts,
            d0.min_profit AS token0_default_min_profit,
            d1.min_profit AS token1_default_min_profit
        FROM token_pairs tp
        LEFT JOIN token_search_defaults d0
          ON d0.chain_id = tp.chain_id
         AND d0.token_address = tp.token0
         AND d0.executor_scope = 'all'
        LEFT JOIN token_search_defaults d1
          ON d1.chain_id = tp.chain_id
         AND d1.token_address = tp.token1
         AND d1.executor_scope = 'all'
        WHERE tp.enabled = TRUE
        ORDER BY tp.created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_token_search_defaults(pool: &PgPool) -> Result<Vec<TokenSearchDefaultRow>> {
    Ok(sqlx::query_as::<_, TokenSearchDefaultRow>(
        r#"
        WITH token_set AS (
            SELECT chain_id, token_address, symbol, enabled
            FROM tokens
            UNION
            SELECT chain_id, token0 AS token_address, split_part(symbol, '/', 1) AS symbol, TRUE AS enabled
            FROM token_pairs
            WHERE enabled = TRUE
            UNION
            SELECT chain_id, token1 AS token_address, split_part(symbol, '/', 2) AS symbol, TRUE AS enabled
            FROM token_pairs
            WHERE enabled = TRUE
            UNION
            SELECT chain_id, token_address, token_address AS symbol, TRUE AS enabled
            FROM token_search_defaults
        )
        SELECT
            token_set.chain_id,
            MIN(token_set.symbol) AS symbol,
            token_set.token_address,
            BOOL_OR(token_set.enabled) AS enabled,
            cfg_all.search_amounts,
            cfg_all.min_profit,
            cfg_two_hop.search_amounts AS two_hop_search_amounts,
            cfg_two_hop.min_profit AS two_hop_min_profit,
            cfg_multihop.search_amounts AS multihop_search_amounts,
            cfg_multihop.min_profit AS multihop_min_profit
        FROM token_set
        LEFT JOIN token_search_defaults cfg_all
          ON cfg_all.chain_id = token_set.chain_id
         AND cfg_all.token_address = token_set.token_address
         AND cfg_all.executor_scope = 'all'
        LEFT JOIN token_search_defaults cfg_two_hop
          ON cfg_two_hop.chain_id = token_set.chain_id
         AND cfg_two_hop.token_address = token_set.token_address
         AND cfg_two_hop.executor_scope = 'two_hop'
        LEFT JOIN token_search_defaults cfg_multihop
          ON cfg_multihop.chain_id = token_set.chain_id
         AND cfg_multihop.token_address = token_set.token_address
         AND cfg_multihop.executor_scope = 'multihop'
        GROUP BY
            token_set.chain_id,
            token_set.token_address,
            cfg_all.search_amounts,
            cfg_all.min_profit,
            cfg_two_hop.search_amounts,
            cfg_two_hop.min_profit,
            cfg_multihop.search_amounts,
            cfg_multihop.min_profit
        ORDER BY
            (
                (cfg_all.search_amounts IS NOT NULL AND cfg_all.min_profit IS NOT NULL)
                OR (cfg_two_hop.search_amounts IS NOT NULL AND cfg_two_hop.min_profit IS NOT NULL)
                OR (cfg_multihop.search_amounts IS NOT NULL AND cfg_multihop.min_profit IS NOT NULL)
            ) DESC,
            MIN(token_set.symbol),
            token_set.token_address
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_funding_anchors(pool: &PgPool) -> Result<Vec<Address>> {
    let rows: Vec<String> = sqlx::query_scalar(
        r#"
        WITH anchors AS (
            SELECT token_address
            FROM token_search_defaults
            WHERE search_amounts IS NOT NULL
              AND min_profit IS NOT NULL
            UNION
            SELECT token0 AS token_address
            FROM token_pairs
            WHERE enabled = TRUE
              AND token0_search_amounts IS NOT NULL
              AND token0_min_profit IS NOT NULL
            UNION
            SELECT token1 AS token_address
            FROM token_pairs
            WHERE enabled = TRUE
              AND token1_search_amounts IS NOT NULL
              AND token1_min_profit IS NOT NULL
        )
        SELECT token_address
        FROM anchors
        ORDER BY token_address
        "#,
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|value| value.parse().map_err(Into::into))
        .collect()
}

async fn fetch_enabled_tokens(pool: &PgPool) -> Result<Vec<Address>> {
    let rows: Vec<String> = sqlx::query_scalar(
        r#"
        WITH token_set AS (
            SELECT token_address
            FROM tokens
            WHERE enabled = TRUE
            UNION
            SELECT token0 AS token_address
            FROM token_pairs
            WHERE enabled = TRUE
            UNION
            SELECT token1 AS token_address
            FROM token_pairs
            WHERE enabled = TRUE
            UNION
            SELECT token_address
            FROM token_search_defaults
        )
        SELECT token_address
        FROM token_set
        ORDER BY token_address
        "#,
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|value| value.parse().map_err(Into::into))
        .collect()
}

async fn upsert_token_registry(
    pool: &PgPool,
    chain_id: u64,
    token: Address,
    symbol: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tokens (chain_id, token_address, symbol, enabled, created_at, updated_at)
        VALUES ($1, $2, $3, TRUE, NOW(), NOW())
        ON CONFLICT (chain_id, token_address)
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            enabled = TRUE,
            updated_at = NOW()
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(address_to_string(token))
    .bind(symbol)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn upsert_observed_pool_row(
    store: &PostgresStore,
    chain_id: u64,
    row: &ObservedPoolCandidate,
    pool_address: Address,
    discovered: Option<&DiscoveredPool>,
    symbol: Option<&str>,
    import_status: &str,
    import_reason: Option<&str>,
) -> Result<()> {
    let state = discovered.map(|discovered| &discovered.state);
    store
        .upsert_observed_pool(
            chain_id,
            pool_address,
            &row.topic0,
            &row.family,
            state.map(|state| state.token0),
            state.map(|state| state.token1),
            symbol,
            discovered.and_then(|discovered| discovered.factory_address),
            state.map(|state| dex_to_string(state.dex)),
            state.map(|state| variant_to_string(state.variant)),
            state.map(|state| state.fee_bps),
            state.and_then(|state| state.fee_pips),
            discovered.and_then(|discovered| discovered.tick_spacing),
            discovered.and_then(|discovered| discovered.stable),
            row.txs,
            row.swap_logs,
            Some(row.first_block),
            Some(row.latest_block),
            import_status,
            import_reason,
        )
        .await
}

async fn fetch_registry_pools(pool: &PgPool) -> Result<Vec<PoolRegistryRow>> {
    Ok(sqlx::query_as::<_, PoolRegistryRow>(
        r#"
        SELECT
            p.created_at,
            GREATEST(events.last_event_at, states.last_state_at) AS last_update_time,
            p.dex,
            p.variant,
            tp.symbol AS pair_symbol,
            p.pool_address,
            p.factory_address,
            p.token0,
            p.token1,
            split_part(tp.symbol, '/', 1) AS token0_symbol,
            split_part(tp.symbol, '/', 2) AS token1_symbol,
            p.fee_bps,
            p.stable,
            p.enabled,
            p.source
        FROM pools p
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN (
            SELECT lower(pool_address) AS pool_address, MAX(created_at) AS last_event_at
            FROM dex_events
            GROUP BY lower(pool_address)
        ) events ON events.pool_address = lower(p.pool_address)
        LEFT JOIN (
            SELECT lower(pool_address) AS pool_address, MAX(updated_at) AS last_state_at
            FROM pool_states
            GROUP BY lower(pool_address)
        ) states ON states.pool_address = lower(p.pool_address)
        ORDER BY p.created_at DESC
        LIMIT 100
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_observed_pools(pool: &PgPool) -> Result<Vec<ObservedPoolRow>> {
    Ok(sqlx::query_as::<_, ObservedPoolRow>(
        r#"
        SELECT
            updated_at,
            symbol,
            pool_address,
            family,
            dex,
            variant,
            factory_address,
            txs_30d,
            logs_30d,
            import_status,
            import_reason
        FROM observed_pools
        ORDER BY updated_at DESC, txs_30d DESC
        LIMIT 100
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_factory_registry(pool: &PgPool) -> Result<Vec<FactoryRegistryRow>> {
    Ok(sqlx::query_as::<_, FactoryRegistryRow>(
        r#"
        SELECT
            updated_at,
            factory_address,
            dex,
            variant,
            trusted,
            enabled,
            source,
            notes,
            observed_pools,
            first_seen_block,
            latest_seen_block
        FROM factory_registry
        ORDER BY trusted DESC, observed_pools DESC, updated_at DESC
        LIMIT 200
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_observed_pool_candidates(
    pool: &PgPool,
    address: Address,
    from_block: u64,
    to_block: u64,
    min_pool_txs: i64,
    pool_limit: i64,
) -> Result<Vec<ObservedPoolCandidate>> {
    let seed = format!("{address:#x}").to_ascii_lowercase();
    let rows = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT lower(tx_hash) AS tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
        ),
        swap_logs AS (
            SELECT
                lower(log->>'address') AS pool_address,
                lower(log->'topics'->>0) AS topic0,
                lower(ot.tx_hash) AS tx_hash,
                ot.block_number
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            WHERE lower(log->'topics'->>0) = ANY($4)
              AND ot.block_number BETWEEN $2 AND $3
        )
        SELECT
            sl.pool_address,
            sl.topic0,
            CASE sl.topic0
                WHEN $5 THEN 'v3/slipstream'
                WHEN $6 THEN 'pancake-v3'
                WHEN $7 THEN 'classic-v2'
                ELSE sl.topic0
            END AS family,
            COUNT(*)::bigint AS swap_logs,
            COUNT(DISTINCT sl.tx_hash)::bigint AS txs,
            MIN(sl.block_number)::bigint AS first_block,
            MAX(sl.block_number)::bigint AS latest_block,
            tp.symbol AS registry_symbol
        FROM swap_logs sl
        LEFT JOIN pools p ON lower(p.pool_address) = sl.pool_address
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        GROUP BY sl.pool_address, sl.topic0, tp.symbol
        HAVING COUNT(DISTINCT sl.tx_hash) >= $8
        ORDER BY COUNT(DISTINCT sl.tx_hash) DESC, COUNT(*) DESC
        LIMIT $9
        "#,
    )
    .bind(seed)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .bind(vec![
        UNI_V3_SWAP_TOPIC.to_string(),
        PANCAKE_V3_SWAP_TOPIC.to_string(),
        CLASSIC_SWAP_TOPIC.to_string(),
    ])
    .bind(UNI_V3_SWAP_TOPIC)
    .bind(PANCAKE_V3_SWAP_TOPIC)
    .bind(CLASSIC_SWAP_TOPIC)
    .bind(min_pool_txs)
    .bind(pool_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| ObservedPoolCandidate {
            pool_address: row.get("pool_address"),
            topic0: row.get("topic0"),
            family: row.get("family"),
            swap_logs: row.get("swap_logs"),
            txs: row.get("txs"),
            first_block: row.get("first_block"),
            latest_block: row.get("latest_block"),
            registry_symbol: row.get("registry_symbol"),
        })
        .collect())
}

async fn fetch_dex_events(pool: &PgPool) -> Result<Vec<DexEventRow>> {
    Ok(sqlx::query_as::<_, DexEventRow>(
        r#"
        SELECT created_at, block_number, dex, event_type, pool_address, tx_hash
        FROM dex_events
        WHERE event_type <> 'Unknown'
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_unknown_topics(pool: &PgPool) -> Result<Vec<UnknownTopicRow>> {
    Ok(sqlx::query_as::<_, UnknownTopicRow>(
        r#"
        SELECT
            MAX(created_at) AS last_seen,
            dex,
            pool_address,
            raw_data_json->'topics'->>0 AS topic0,
            COUNT(*)::BIGINT AS event_count
        FROM dex_events
        WHERE event_type = 'Unknown'
        GROUP BY dex, pool_address, topic0
        ORDER BY event_count DESC, last_seen DESC
        LIMIT 50
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_pool_states(pool: &PgPool) -> Result<Vec<PoolStateRow>> {
    Ok(sqlx::query_as::<_, PoolStateRow>(
        r#"
        SELECT
            ps.updated_at,
            ps.block_number,
            ps.source,
            ps.dex, p.variant, ps.pool_address,
            ps.token0, ps.token1,
            split_part(tp.symbol, '/', 1) AS token0_symbol,
            split_part(tp.symbol, '/', 2) AS token1_symbol,
            ps.fee, ps.reserve0, ps.reserve1,
            ps.sqrt_price_x96, ps.liquidity, ps.tick
        FROM pools p
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        INNER JOIN LATERAL (
            SELECT
                updated_at, block_number, dex, pool_address, token0, token1, fee,
                reserve0, reserve1, sqrt_price_x96, liquidity, tick, source
            FROM pool_states
            WHERE pool_address = p.pool_address
            ORDER BY updated_at DESC
            LIMIT 1
        ) ps ON TRUE
        WHERE p.enabled = TRUE
        ORDER BY ps.updated_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_tick_coverage(
    tick_store: &RedisStore,
    rows: &[PoolStateRow],
) -> Result<HashMap<String, TickCoverage>> {
    let mut out = HashMap::new();
    for row in rows {
        if !is_v3_variant(row.variant.as_deref()) {
            continue;
        }
        let Ok(pool) = row.pool_address.parse::<Address>() else {
            continue;
        };
        let ticks = tick_store.get_pool_ticks(pool).await?;
        let latest_updated_at = ticks.iter().map(|tick| tick.updated_at).max();
        out.insert(
            row.pool_address.to_lowercase(),
            TickCoverage {
                count: ticks.len(),
                latest_updated_at,
            },
        );
    }
    Ok(out)
}

async fn fetch_pool_state_warnings(pool: &PgPool) -> Result<Vec<PoolStateWarningRow>> {
    Ok(sqlx::query_as::<_, PoolStateWarningRow>(
        r#"
        SELECT created_at, pool_address, dex, variant, block_number, drift_bps, message
        FROM pool_state_warnings
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_pool_state_validations(pool: &PgPool) -> Result<Vec<PoolStateValidationRow>> {
    Ok(sqlx::query_as::<_, PoolStateValidationRow>(
        r#"
        SELECT created_at, pool_address, dex, variant, block_number, drift_bps, passed, message
        FROM pool_state_validations
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_opportunities(pool: &PgPool) -> Result<Vec<OpportunityRow>> {
    Ok(sqlx::query_as::<_, OpportunityRow>(
        r#"
        SELECT
            id::text AS id,
            created_at,
            block_number,
            strategy,
            path_json->>'name' AS path_name,
            amount_in,
            expected_profit,
            path_json->'diagnostics'->>'modes' AS quote_modes,
            (path_json->'diagnostics'->>'ticks_used')::BIGINT AS ticks_used,
            (path_json->'diagnostics'->>'crossed_ticks')::BIGINT AS crossed_ticks,
            (path_json->'diagnostics'->>'tick_range_exhausted')::BOOLEAN AS tick_range_exhausted,
            (path_json->'diagnostics'->>'v3_pools_without_ticks')::BIGINT AS v3_pools_without_ticks,
            status
        FROM opportunities
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_simulations(pool: &PgPool) -> Result<Vec<SimulationRow>> {
    Ok(sqlx::query_as::<_, SimulationRow>(
        r#"
        SELECT
            s.created_at,
            s.opportunity_id::text AS opportunity_id,
            COALESCE(o.path_json->>'name', '-') AS path_name,
            s.success,
            s.simulated_profit,
            s.gas_estimate,
            s.gas_cost_cap,
            s.gas_cost_expected,
            s.net_simulated_profit,
            s.max_fee_per_gas,
            s.max_priority_fee_per_gas,
            s.block_number,
            s.revert_reason
        FROM simulations s
        LEFT JOIN opportunities o ON o.id = s.opportunity_id
        ORDER BY s.created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_transactions(pool: &PgPool) -> Result<Vec<TransactionRow>> {
    Ok(sqlx::query_as::<_, TransactionRow>(
        r#"
        SELECT created_at, eoa, tx_hash, nonce, status, realized_profit, revert_reason
        FROM transactions
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

fn render_page(
    title: &str,
    subtitle: &str,
    auth_password: Option<&str>,
    flash: Option<&str>,
    content: &str,
) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="/favicon.svg" type="image/svg+xml">
  <link rel="shortcut icon" href="/favicon.ico">
  <title>Base Arb Monitor - {title}</title>
  <style>
    :root {{
      --bg: #0d1117;
      --panel: #161b22;
      --panel-2: #1f2630;
      --text: #e6edf3;
      --muted: #93a1b2;
      --line: #2d3742;
      --accent: #4ad295;
      --warn: #ffb347;
      --bad: #ff7b72;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 24px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      background: radial-gradient(circle at top, #142033 0%, var(--bg) 42%);
      color: var(--text);
    }}
    h1 {{ margin: 0; font-size: 28px; }}
    p {{ margin: 0 0 18px; color: var(--muted); }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .brand-logo {{
      width: 42px;
      height: 42px;
      flex: 0 0 auto;
      filter: drop-shadow(0 8px 18px rgba(74,210,149,0.18));
    }}
    nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      margin: 18px 0;
    }}
    nav a {{
      color: var(--text);
      text-decoration: none;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(255,255,255,0.03);
    }}
    nav a:hover {{
      border-color: var(--accent);
      color: var(--accent);
    }}
    .stack {{
      display: flex;
      flex-direction: column;
      gap: 18px;
    }}
    .admin {{
      margin: 18px 0;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(22,27,34,0.82);
    }}
    .admin {{
      display: grid;
      gap: 14px;
    }}
    .admin form {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      align-items: end;
    }}
    .registry-tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 14px;
    }}
    .registry-tab {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      color: var(--text);
      text-decoration: none;
      background: rgba(255,255,255,0.03);
    }}
    .registry-tab.active,
    .registry-tab:hover {{
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(74,210,149,0.08);
    }}
    .registry-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 12px;
    }}
    .registry-card-link {{
      display: grid;
      gap: 8px;
      min-height: 118px;
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 14px;
      color: var(--text);
      text-decoration: none;
      background:
        linear-gradient(135deg, rgba(34,167,242,0.10), transparent 40%),
        rgba(22,27,34,0.72);
    }}
    .registry-card-link:hover {{
      border-color: var(--accent);
      transform: translateY(-1px);
    }}
    .registry-card-link strong {{
      font-size: 16px;
    }}
    .registry-card-link span {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }}
    .inline-form {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-width: 430px;
      margin-right: 8px;
    }}
    .inline-form .password-wrap {{
      width: 180px;
    }}
    .inline-form button {{
      padding: 8px 10px;
      white-space: nowrap;
    }}
    .pair-card-list {{
      display: grid;
      gap: 14px;
    }}
    .pair-card {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 16px;
      background:
        linear-gradient(135deg, rgba(74,210,149,0.08), transparent 34%),
        rgba(13,17,23,0.58);
      padding: 14px;
    }}
    .pair-card-header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 12px;
    }}
    .pair-card-title {{
      display: grid;
      gap: 4px;
    }}
    .pair-card-title strong {{
      font-size: 16px;
    }}
    .pair-card-meta {{
      color: var(--muted);
      font-size: 12px;
    }}
    .pair-card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }}
    .pair-field {{
      display: grid;
      gap: 5px;
      min-width: 0;
      padding: 10px;
      border: 1px solid rgba(255,255,255,0.06);
      border-radius: 12px;
      background: rgba(255,255,255,0.025);
    }}
    .pair-field-label {{
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .pair-actions {{
      display: grid;
      gap: 10px;
    }}
    .pair-actions-row {{
      display: flex;
      flex-direction: column;
      gap: 8px;
      align-items: stretch;
    }}
    .pair-actions-row .inline-form {{
      min-width: 0;
      margin-right: 0;
      width: 100%;
    }}
    .pair-actions-row .inline-form button {{
      width: 100%;
      padding: 7px 9px;
    }}
    .factory-form {{
      display: grid;
      grid-template-columns: minmax(140px, 1fr);
      gap: 6px;
      min-width: 220px;
    }}
    .factory-form select,
    .factory-form input {{
      width: 100%;
      min-width: 0;
    }}
    .compact-check {{
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: var(--muted);
    }}
    .compact-check input {{
      width: auto;
    }}
    .search-config-form {{
      display: grid;
      grid-template-columns: repeat(2, minmax(220px, 1fr));
      gap: 8px;
      width: 100%;
      margin-bottom: 8px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255,255,255,0.03);
    }}
    .search-config-form .password-wrap,
    .search-config-form button {{
      grid-column: span 2;
    }}
    .bulk-token-form {{
      padding: 12px;
      display: grid;
      gap: 12px;
    }}
    .bulk-token-toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: end;
    }}
    .bulk-token-toolbar label {{
      min-width: 260px;
    }}
    .compact-table th,
    .compact-table td {{
      padding: 7px 9px;
      vertical-align: middle;
    }}
    .compact-input {{
      min-width: 210px;
      padding: 7px 9px;
      border-radius: 8px;
      font-size: 12px;
    }}
    @media (max-width: 720px) {{
      .pair-card-header {{
        display: grid;
      }}
      .search-config-form {{
        grid-template-columns: 1fr;
      }}
      .search-config-form .password-wrap,
      .search-config-form button {{
        grid-column: span 1;
      }}
    }}
    .delete-btn {{
      background: var(--bad);
      color: #190704;
    }}
    .danger-btn {{
      border: 1px solid var(--bad);
      background: rgba(255,109,84,0.14);
      color: var(--bad);
    }}
    .password-wrap {{
      display: flex;
      align-items: stretch;
      gap: 6px;
    }}
    .password-wrap input {{
      min-width: 0;
    }}
    .toggle-password {{
      flex: 0 0 auto;
      padding: 8px 9px;
      color: var(--text);
      background: rgba(255,255,255,0.06);
      border: 1px solid var(--line);
    }}
    label {{ display: grid; gap: 6px; color: var(--muted); font-size: 12px; }}
    input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0d1117;
      color: var(--text);
      padding: 10px 12px;
      font: inherit;
    }}
    button {{
      border: 0;
      border-radius: 10px;
      padding: 11px 14px;
      color: #07110c;
      background: var(--accent);
      font-weight: 700;
      cursor: pointer;
    }}
    .flash {{
      margin: 0 0 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      color: var(--accent);
      background: rgba(74,210,149,0.08);
    }}
    .muted {{
      color: var(--muted);
    }}
    .card {{
      background: linear-gradient(180deg, var(--panel) 0%, var(--panel-2) 100%);
      border: 1px solid var(--line);
      border-radius: 14px;
      overflow: hidden;
      box-shadow: 0 18px 50px rgba(0,0,0,0.22);
    }}
    .card h2 {{
      margin: 0;
      padding: 14px 16px;
      font-size: 14px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      border-bottom: 1px solid var(--line);
    }}
    .card-body {{
      overflow: visible;
    }}
    .table-scroll {{
      width: 100%;
      overflow-x: auto;
      overflow-y: hidden;
      -webkit-overflow-scrolling: touch;
    }}
    .table-scroll table {{
      min-width: max-content;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    th, td {{
      padding: 10px 12px;
      vertical-align: top;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      text-align: left;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
    tr:last-child td {{ border-bottom: 0; }}
    .ok {{ color: var(--accent); }}
    .warn {{ color: var(--warn); }}
    .bad {{ color: var(--bad); }}
    .mono {{
      max-width: 150px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      display: inline-block;
      vertical-align: middle;
    }}
    .copyable {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      max-width: 220px;
    }}
    .copy-btn {{
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 4px 7px;
      color: var(--text);
      background: rgba(255,255,255,0.04);
      font-size: 11px;
      font-weight: 600;
      line-height: 1;
    }}
    .state-number {{
      color: var(--accent);
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .token-label {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
    }}
    .token-label strong {{
      color: var(--text);
      font-weight: 800;
    }}
  </style>
  <script>
    async function copyValue(button) {{
      const value = button.dataset.copy;
      try {{
        await navigator.clipboard.writeText(value);
        const label = button.textContent;
        button.textContent = "copied";
        window.setTimeout(() => button.textContent = label, 900);
      }} catch (_) {{
        window.prompt("Copy value", value);
      }}
    }}
    function togglePassword(button) {{
      const input = button.parentElement.querySelector("input");
      const visible = input.type === "text";
      input.type = visible ? "password" : "text";
      button.textContent = visible ? "show" : "hide";
    }}
    function promptPasswordSubmit(form, message) {{
      if (!window.confirm(message)) return false;
      const password = window.prompt("Monitor password");
      if (password === null || password.trim() === "") return false;
      form.querySelector('input[name="password"]').value = password;
      return true;
    }}
    function confirmTokenDefaultsUpdate(form) {{
      const changes = [];
      const changedTokens = new Set();
      form.querySelectorAll("input[data-old]").forEach((input) => {{
        const oldValue = input.dataset.old || "";
        const newValue = input.value.trim();
        if (oldValue !== newValue) {{
          changes.push(`${{input.dataset.symbol}} ${{input.dataset.kind}}: "${{oldValue || "-"}}" -> "${{newValue || "-"}}"`);
          changedTokens.add(input.dataset.token);
        }}
      }});
      if (changes.length === 0) {{
        return window.confirm("No token default changes detected. Submit anyway?");
      }}
      if (!window.confirm(`Update token configs?\n\n${{changes.join("\n")}}`)) {{
        return false;
      }}
      form.querySelectorAll("input[data-old]").forEach((input) => {{
        const oldValue = input.dataset.old || "";
        const newValue = input.value.trim();
        if (oldValue === newValue) {{
          input.disabled = true;
        }}
      }});
      form.querySelector('input[name="token_addresses"]').value = Array.from(changedTokens).join(",");
      return true;
    }}
  </script>
</head>
<body>
  <div class="brand">
    <span class="brand-logo">{logo}</span>
    <h1>{title}</h1>
  </div>
  <p>{subtitle}</p>
  <nav>
    <a href="{overview_href}">Overview</a>
    <a href="{registry_href}">Registry</a>
    <a href="{activity_href}">Activity</a>
    <a href="{execution_href}">Execution</a>
  </nav>
  {flash}
  <div class="stack">
    {content}
  </div>
</body>
</html>"#,
        subtitle = escape(subtitle),
        title = escape(title),
        logo = LOGO_SVG,
        overview_href = nav_href("/", auth_password),
        registry_href = nav_href("/registry", auth_password),
        activity_href = nav_href("/activity", auth_password),
        execution_href = nav_href("/execution", auth_password),
        flash = render_flash(flash),
        content = content,
    )
}

fn render_login() -> String {
    r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="/favicon.svg" type="image/svg+xml">
  <link rel="shortcut icon" href="/favicon.ico">
  <title>Base Arb Monitor Login</title>
  <style>
    :root { --bg: #0d1117; --panel: #161b22; --text: #e6edf3; --muted: #93a1b2; --line: #2d3742; --accent: #4ad295; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; background: radial-gradient(circle at top, #142033 0%, var(--bg) 48%); color: var(--text); }
    form { width: min(420px, calc(100vw - 32px)); padding: 22px; border: 1px solid var(--line); border-radius: 16px; background: var(--panel); box-shadow: 0 18px 50px rgba(0,0,0,0.26); }
    h1 { margin: 0; font-size: 22px; }
    p { margin: 0 0 18px; color: var(--muted); }
    .brand { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
    .brand-logo { width: 38px; height: 38px; flex: 0 0 auto; filter: drop-shadow(0 8px 18px rgba(74,210,149,0.18)); }
    label { display: grid; gap: 8px; color: var(--muted); font-size: 12px; }
    input { width: 100%; border: 1px solid var(--line); border-radius: 10px; background: #0d1117; color: var(--text); padding: 11px 12px; font: inherit; }
    button { width: 100%; margin-top: 14px; border: 0; border-radius: 10px; padding: 12px 14px; color: #07110c; background: var(--accent); font-weight: 700; cursor: pointer; }
    .password-wrap { display: flex; align-items: stretch; gap: 6px; }
    .password-wrap input { min-width: 0; }
    .toggle-password { width: auto; margin-top: 0; padding: 10px 11px; color: var(--text); background: rgba(255,255,255,0.06); border: 1px solid var(--line); }
  </style>
  <script>
    function togglePassword(button) {
      const input = button.parentElement.querySelector("input");
      const visible = input.type === "text";
      input.type = visible ? "password" : "text";
      button.textContent = visible ? "show" : "hide";
    }
  </script>
</head>
<body>
  <form method="get" action="/">
    <div class="brand">
      <span class="brand-logo">__LOGO__</span>
      <h1>Base Arb Monitor</h1>
    </div>
    <p>Enter `MONITOR_WEB_PASSWORD` from `.env`.</p>
    <label>Password
      __PASSWORD_INPUT__
    </label>
    <button type="submit">Open Monitor</button>
  </form>
</body>
</html>"#
        .replace("__LOGO__", LOGO_SVG)
        .replace("__PASSWORD_INPUT__", &password_input(Some(""), true))
}

fn render_events_table(rows: &[DexEventRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Block</th><th>DEX</th><th>Event</th><th>Pool</th><th>Tx</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.block_number,
            escape(&row.dex),
            escape(&row.event_type),
            copyable(&row.pool_address),
            copyable(&row.tx_hash),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"6\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_unknown_topics_table(rows: &[UnknownTopicRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Last Seen</th><th>Count</th><th>DEX</th><th>Pool</th><th>Topic 0</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.last_seen),
            row.event_count,
            escape(&row.dex),
            copyable(&row.pool_address),
            row.topic0
                .as_deref()
                .map(copyable)
                .unwrap_or_else(|| "-".to_string()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"5\">No unknown topics recorded.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_flash(value: Option<&str>) -> String {
    value
        .map(|message| format!("<div class=\"flash\">{}</div>", escape(message)))
        .unwrap_or_default()
}

fn nav_href(path: &str, auth_password: Option<&str>) -> String {
    match auth_password {
        Some(password) if !password.is_empty() => {
            format!("{}?password={}", path, url_query_escape(password))
        }
        _ => path.to_string(),
    }
}

fn render_token_pairs_table(rows: &[TokenPairRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table class=\"compact-table\"><thead><tr><th>Created</th><th>Pair</th><th>Enabled</th><th>Token 0</th><th>Token 1</th><th>T0 Active Amounts</th><th>T0 Active Min</th><th>T1 Active Amounts</th><th>T1 Active Min</th><th>Actions</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{created_at}</td><td>{symbol}<br><span class=\"muted\">chain {chain_id}</span></td><td>{enabled}</td><td>{token0}</td><td>{token1}</td><td>{token0_amounts}</td><td>{token0_profit}</td><td>{token1_amounts}</td><td>{token1_profit}</td><td>{actions}</td></tr>",
            symbol = escape(&row.symbol),
            chain_id = row.chain_id,
            created_at = fmt_ts(row.created_at),
            enabled = monitoring_status(row.enabled),
            token0 = copyable(&row.token0),
            token1 = copyable(&row.token1),
            token0_amounts = effective_config_value(
                row.token0_search_amounts.as_deref(),
                row.token0_default_search_amounts.as_deref(),
            ),
            token0_profit = effective_config_value(
                row.token0_search_amounts
                    .as_deref()
                    .and(row.token0_min_profit.as_deref()),
                row.token0_default_min_profit.as_deref(),
            ),
            token1_amounts = effective_config_value(
                row.token1_search_amounts.as_deref(),
                row.token1_default_search_amounts.as_deref(),
            ),
            token1_profit = effective_config_value(
                row.token1_search_amounts
                    .as_deref()
                    .and(row.token1_min_profit.as_deref()),
                row.token1_default_min_profit.as_deref(),
            ),
            actions = render_pair_actions(row),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"10\">No token pairs yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_token_search_defaults(rows: &[TokenSearchDefaultRow]) -> String {
    let token_addresses = rows
        .iter()
        .map(|row| row.token_address.to_lowercase())
        .collect::<Vec<_>>()
        .join(",");
    let mut html = format!(
        r#"<form method="post" action="/tokens/search-defaults" class="bulk-token-form" onsubmit="return confirmTokenDefaultsUpdate(this)">
  <div class="bulk-token-toolbar">
    <label>Password {password_input}</label>
    <button type="submit">Update Token Configs</button>
  </div>
  <input type="hidden" name="token_addresses" value="{token_addresses}">
  <div class="table-scroll"><table class="compact-table"><thead><tr><th>Token</th><th>Address</th><th>Enabled</th><th>Funding Anchor</th><th>All Amounts</th><th>All Min</th><th>2-Hop Amounts</th><th>2-Hop Min</th><th>Multi Amounts</th><th>Multi Min</th></tr></thead><tbody>"#,
        password_input = password_input(Some("password"), false),
        token_addresses = escape(&token_addresses),
    );
    for row in rows {
        let token_key = row.token_address.to_lowercase();
        let anchor = if row.search_amounts.is_some()
            || row.two_hop_search_amounts.is_some()
            || row.multihop_search_amounts.is_some()
        {
            "<span class=\"ok\">configured</span>"
        } else {
            "<span class=\"muted\">no</span>"
        };
        html.push_str(&format!(
            r#"<tr>
  <td><strong>{symbol}</strong><br><span class="muted">chain {chain_id}</span></td>
  <td>{token}</td>
  <td>{enabled}</td>
  <td>{anchor}</td>
  <td><input class="compact-input" name="search_amounts_{token_key}" value="{amounts_raw}" data-old="{amounts_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="Amounts Raw" placeholder="10000000,30000000"></td>
  <td><input class="compact-input" name="min_profit_{token_key}" value="{min_profit_raw}" data-old="{min_profit_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="Min Profit Raw" placeholder="500"></td>
  <td><input class="compact-input" name="two_hop_search_amounts_{token_key}" value="{two_hop_amounts_raw}" data-old="{two_hop_amounts_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="2-Hop Amounts Raw" placeholder="10000000,30000000"></td>
  <td><input class="compact-input" name="two_hop_min_profit_{token_key}" value="{two_hop_min_profit_raw}" data-old="{two_hop_min_profit_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="2-Hop Min Profit Raw" placeholder="500"></td>
  <td><input class="compact-input" name="multihop_search_amounts_{token_key}" value="{multihop_amounts_raw}" data-old="{multihop_amounts_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="Multi Amounts Raw" placeholder="10000000,30000000"></td>
  <td><input class="compact-input" name="multihop_min_profit_{token_key}" value="{multihop_min_profit_raw}" data-old="{multihop_min_profit_raw}" data-token="{token_key}" data-symbol="{symbol}" data-kind="Multi Min Profit Raw" placeholder="500"></td>
</tr>"#,
            symbol = escape(&row.symbol),
            chain_id = row.chain_id,
            token = copyable(&row.token_address),
            enabled = monitoring_status(row.enabled),
            anchor = anchor,
            token_key = escape(&token_key),
            amounts_raw = escape(row.search_amounts.as_deref().unwrap_or_default()),
            min_profit_raw = escape(row.min_profit.as_deref().unwrap_or_default()),
            two_hop_amounts_raw = escape(row.two_hop_search_amounts.as_deref().unwrap_or_default()),
            two_hop_min_profit_raw = escape(row.two_hop_min_profit.as_deref().unwrap_or_default()),
            multihop_amounts_raw = escape(row.multihop_search_amounts.as_deref().unwrap_or_default()),
            multihop_min_profit_raw = escape(row.multihop_min_profit.as_deref().unwrap_or_default()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"10\">No tokens yet.</td></tr>");
    }
    html.push_str("</tbody></table></div></form>");
    html
}

fn render_pair_actions(row: &TokenPairRow) -> String {
    format!(
        r#"<div class="pair-actions-row">{}{}{}</div>"#,
        render_rediscover_form(row),
        render_delete_pair_form(row),
        render_remove_pair_form(row)
    )
}

fn render_rediscover_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/rediscover" class="inline-form" onsubmit="return promptPasswordSubmit(this, 'Discover pools for {symbol}?')">
  <input name="password" type="hidden">
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button type="submit">Discover</button>
</form>"#,
        symbol = escape_js_string(&row.symbol),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_rediscover_all_form() -> String {
    format!(
        r#"<form method="post" action="/pairs/rediscover-all" onsubmit="return confirm('Rediscover all enabled token pairs? This will call pool factories and update registry rows.');">
  <label>Password
    {password_input}
  </label>
  <button type="submit">Rediscover All Enabled Pairs</button>
</form>"#,
        password_input = password_input(Some("password"), false),
    )
}

fn render_registry_nav(auth_password: Option<&str>, active: &str) -> String {
    let items = [
        ("home", "Home", "/registry"),
        ("tokens", "Tokens", "/registry/tokens"),
        ("pairs", "Pairs", "/registry/pairs"),
        ("factories", "Factories", "/registry/factories"),
        ("pools", "Pools", "/registry/pools"),
        ("observed", "Observed", "/registry/observed"),
    ];
    let links = items
        .iter()
        .map(|(key, label, path)| {
            let class = if *key == active {
                "registry-tab active"
            } else {
                "registry-tab"
            };
            format!(
                r#"<a class="{class}" href="{href}">{label}</a>"#,
                href = nav_href(path, auth_password),
                label = escape(label),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(r#"<section class="registry-tabs">{links}</section>"#)
}

fn render_registry_cards(auth_password: Option<&str>) -> String {
    [
        (
            "Tokens",
            "Funding anchors and token-level default amounts/min profit.",
            "/registry/tokens",
        ),
        (
            "Pairs",
            "Token pairs, pair-level config, discover/disable/delete actions.",
            "/registry/pairs",
        ),
        (
            "Factories",
            "Trusted factories and unknown factory candidates from discovery/import.",
            "/registry/factories",
        ),
        (
            "Pools",
            "Enabled pool registry rows with factory, variant, fee, and activity.",
            "/registry/pools",
        ),
        (
            "Observed",
            "Observed competitor pools and import status.",
            "/registry/observed",
        ),
    ]
    .iter()
    .map(|(title, description, href)| {
        format!(
            r#"<a class="registry-card-link" href="{href}">
  <strong>{title}</strong>
  <span>{description}</span>
</a>"#,
            href = nav_href(href, auth_password),
            title = escape(title),
            description = escape(description),
        )
    })
    .collect::<Vec<_>>()
    .join("")
}

fn render_reconcile_form() -> String {
    format!(
        r#"<form method="post" action="/registry/reconcile" onsubmit="return confirm('Reconcile all funding anchors against enabled tokens? This will call pool factories and update registry rows.');">
  <label>Password
    {password_input}
  </label>
  <button type="submit">Reconcile Existing Tokens</button>
</form>"#,
        password_input = password_input(Some("password"), false),
    )
}

fn render_import_observed_form() -> String {
    format!(
        r#"<form method="post" action="/registry/import-observed" onsubmit="return confirm('Import observed pools from cached competitor transactions? Supported pools will be enabled; unsupported pools are recorded as observed-only.');">
  <label>Password
    {password_input}
  </label>
  <label>Observed Address
    <input name="address" placeholder="0x collector or counterparty" required>
  </label>
  <label>Days
    <input name="days" value="30" inputmode="numeric">
  </label>
  <label>Min Pool Txs
    <input name="min_pool_txs" value="100" inputmode="numeric">
  </label>
  <label>Pool Limit
    <input name="pool_limit" value="200" inputmode="numeric">
  </label>
  <button type="submit">Import Observed Pools</button>
</form>"#,
        password_input = password_input(Some("password"), false),
    )
}

fn render_delete_pair_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/delete" class="inline-form" onsubmit="return promptPasswordSubmit(this, 'Disable pair {symbol}?')">
  <input name="password" type="hidden">
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button class="delete-btn" type="submit">Disable</button>
</form>"#,
        symbol = escape_js_string(&row.symbol),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_remove_pair_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/remove" class="inline-form" onsubmit="return promptPasswordSubmit(this, 'Delete pair {symbol} and associated pool registry rows? Historical events and state snapshots will be kept.')">
  <input name="password" type="hidden">
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button class="danger-btn" type="submit">Delete</button>
</form>"#,
        symbol = escape_js_string(&row.symbol),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_factory_registry_table(rows: &[FactoryRegistryRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Updated</th><th>Factory</th><th>DEX</th><th>Variant</th><th>Trusted</th><th>Enabled</th><th>Observed Pools</th><th>Seen Blocks</th><th>Source</th><th>Notes</th><th>Confirm / Update</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            copyable(&row.factory_address),
            escape(&row.dex),
            escape(&row.variant),
            bool_label(row.trusted),
            bool_label(row.enabled),
            row.observed_pools,
            seen_blocks(row.first_seen_block, row.latest_seen_block),
            escape(&row.source),
            row.notes.as_deref().map(escape).unwrap_or_else(|| "-".into()),
            render_factory_update_form(row),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"11\">No factory rows yet. Start market-data or import observed pools.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_factory_update_form(row: &FactoryRegistryRow) -> String {
    format!(
        r#"<form method="post" action="/registry/factory" class="factory-form" onsubmit="return promptPasswordSubmit(this, 'Update factory {factory}? Wrong DEX/variant can make bad pools executable.');">
  <input name="password" type="hidden">
  <input name="factory_address" type="hidden" value="{factory}">
  <select name="dex" aria-label="DEX">
    {dex_options}
  </select>
  <select name="variant" aria-label="Variant">
    {variant_options}
  </select>
  <label class="compact-check"><input type="checkbox" name="trusted" value="1" {trusted}> trusted</label>
  <label class="compact-check"><input type="checkbox" name="enabled" value="1" {enabled}> enabled</label>
  <input name="notes" value="{notes}" placeholder="notes">
  <button type="submit">Confirm</button>
</form>"#,
        factory = escape(&row.factory_address),
        dex_options = render_select_options(&["Aerodrome", "UniswapV3", "PancakeSwap"], &row.dex),
        variant_options = render_select_options(
            &[
                "AerodromeVolatile",
                "AerodromeSlipstream",
                "UniswapV3",
                "PancakeV3"
            ],
            &row.variant
        ),
        trusted = checked_attr(row.trusted),
        enabled = checked_attr(row.enabled),
        notes = row.notes.as_deref().map(escape).unwrap_or_default(),
    )
}

fn render_registry_pools_table(rows: &[PoolRegistryRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Created</th><th>Last Update Time</th><th>Activity</th><th>DEX</th><th>Variant</th><th>Pair</th><th>Pool</th><th>Factory</th><th>Token 0</th><th>Token 1</th><th>Fee BPS</th><th>Pool Mode</th><th>Monitoring</th><th>Source</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.last_update_time
                .map(fmt_ts)
                .unwrap_or_else(|| "-".into()),
            activity_status(row.last_update_time),
            escape(&row.dex),
            escape(&row.variant),
            row.pair_symbol
                .as_deref()
                .map(escape)
                .unwrap_or_else(|| "-".into()),
            copyable(&row.pool_address),
            copyable_optional(row.factory_address.as_deref()),
            token_label(row.token0_symbol.as_deref(), &row.token0),
            token_label(row.token1_symbol.as_deref(), &row.token1),
            row.fee_bps.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            pool_mode(row.stable),
            monitoring_status(row.enabled),
            escape(&row.source),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"14\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_observed_pools_table(rows: &[ObservedPoolRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Updated</th><th>Pair</th><th>Pool</th><th>Family</th><th>DEX</th><th>Variant</th><th>Factory</th><th>Txs 30d</th><th>Logs 30d</th><th>Status</th><th>Reason</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            row.symbol.as_deref().map(escape).unwrap_or_else(|| "-".into()),
            copyable(&row.pool_address),
            escape(&row.family),
            row.dex.as_deref().map(escape).unwrap_or_else(|| "-".into()),
            row.variant.as_deref().map(escape).unwrap_or_else(|| "-".into()),
            copyable_optional(row.factory_address.as_deref()),
            row.txs_30d,
            row.logs_30d,
            escape(&row.import_status),
            row.import_reason.as_deref().map(escape).unwrap_or_else(|| "-".into()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"11\">No observed pool imports yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_pool_states_table(
    rows: &[PoolStateRow],
    tick_coverage: &HashMap<String, TickCoverage>,
) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Updated</th><th>Block</th><th>Source</th><th>DEX</th><th>Variant</th><th>Tick</th><th>Tick Data</th><th>Pool</th><th>Token 0</th><th>Token 1</th><th>Fee</th><th>Reserve 0</th><th>Reserve 1</th><th>SqrtPriceX96</th><th>Liquidity</th></tr></thead><tbody>",
    );
    for row in rows {
        let tick_data = render_tick_coverage(row, tick_coverage);
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"state-number\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            row.block_number,
            state_source_label(&row.source),
            escape(&row.dex),
            row.variant
                .as_deref()
                .map(escape)
                .unwrap_or_else(|| "-".into()),
            row.tick.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            tick_data,
            copyable(&row.pool_address),
            token_label(row.token0_symbol.as_deref(), &row.token0),
            token_label(row.token1_symbol.as_deref(), &row.token1),
            row.fee.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            copyable_optional(row.reserve0.as_deref()),
            copyable_optional(row.reserve1.as_deref()),
            copyable_optional(row.sqrt_price_x96.as_deref()),
            copyable_optional(row.liquidity.as_deref()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"15\">No rows yet. Start market-data and make sure at least one pool is enabled.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_tick_coverage(
    row: &PoolStateRow,
    tick_coverage: &HashMap<String, TickCoverage>,
) -> String {
    if !is_v3_variant(row.variant.as_deref()) {
        return "-".to_string();
    }
    let coverage = tick_coverage
        .get(&row.pool_address.to_lowercase())
        .cloned()
        .unwrap_or_default();
    if coverage.count == 0 {
        return "<span class=\"warn\">0 ticks</span>".to_string();
    }
    let latest = coverage
        .latest_updated_at
        .map(fmt_ts)
        .unwrap_or_else(|| "-".to_string());
    format!("{} ticks<br><small>{}</small>", coverage.count, latest)
}

fn is_v3_variant(variant: Option<&str>) -> bool {
    matches!(
        variant,
        Some("UniswapV3") | Some("PancakeV3") | Some("AerodromeSlipstream")
    )
}

fn render_pool_state_warnings_table(rows: &[PoolStateWarningRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Pool</th><th>DEX</th><th>Variant</th><th>Block</th><th>Drift BPS</th><th>Message</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"warn\">{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            copyable(&row.pool_address),
            escape(&row.dex),
            escape(&row.variant),
            row.block_number,
            row.drift_bps,
            escape(&row.message),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"7\">No state warnings. This means calibration has not detected local-vs-onchain drift yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_pool_state_validations_table(rows: &[PoolStateValidationRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Pool</th><th>DEX</th><th>Variant</th><th>Block</th><th>Result</th><th>Drift BPS</th><th>Message</th></tr></thead><tbody>",
    );
    for row in rows {
        let result = if row.passed {
            "<span class=\"ok\">pass</span>"
        } else {
            "<span class=\"bad\">fail</span>"
        };
        let drift = if row.passed {
            row.drift_bps.to_string()
        } else {
            format!("<span class=\"warn\">{}</span>", row.drift_bps)
        };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            copyable(&row.pool_address),
            escape(&row.dex),
            escape(&row.variant),
            row.block_number,
            result,
            drift,
            escape(&row.message),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"8\">No delayed block validations yet. Start market-data and wait for local events plus the validation delay.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_opportunities_table(rows: &[OpportunityRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>UUID</th><th>Path</th><th>Block</th><th>Strategy</th><th>Amount In</th><th>Expected Profit</th><th>Quote Modes</th><th>Ticks Used</th><th>Crossed</th><th>Range Exhausted</th><th>Missing Ticks</th><th>Status</th></tr></thead><tbody>",
    );
    for row in rows {
        let range_exhausted = match row.tick_range_exhausted {
            Some(true) => "<span class=\"warn\">true</span>".to_string(),
            Some(false) => "false".to_string(),
            None => "-".to_string(),
        };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            copyable(&row.id),
            copyable(&row.path_name),
            row.block_number,
            escape(&row.strategy),
            escape(&row.amount_in),
            escape(&row.expected_profit),
            escape(row.quote_modes.as_deref().unwrap_or("-")),
            fmt_optional_i64(row.ticks_used),
            fmt_optional_i64(row.crossed_ticks),
            range_exhausted,
            fmt_warn_i64(row.v3_pools_without_ticks),
            escape(&row.status),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"13\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn fmt_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn fmt_warn_i64(value: Option<i64>) -> String {
    match value {
        Some(value) if value > 0 => format!("<span class=\"warn\">{value}</span>"),
        Some(value) => value.to_string(),
        None => "-".to_string(),
    }
}

fn render_simulations_table(rows: &[SimulationRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Block</th><th>Opportunity UUID</th><th>Path</th><th>Success</th><th>Gross Profit</th><th>Gas Est</th><th>Gas Expected</th><th>Gas Cap</th><th>Net Profit</th><th>Max Fee</th><th>Priority</th><th>Revert</th></tr></thead><tbody>",
    );
    for row in rows {
        let class = if row.success { "ok" } else { "bad" };
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.block_number
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            copyable(&row.opportunity_id),
            copyable(&row.path_name),
            class,
            row.success,
            escape(row.simulated_profit.as_deref().unwrap_or("-")),
            escape(row.gas_estimate.as_deref().unwrap_or("-")),
            escape(row.gas_cost_expected.as_deref().unwrap_or("-")),
            escape(row.gas_cost_cap.as_deref().unwrap_or("-")),
            escape(row.net_simulated_profit.as_deref().unwrap_or("-")),
            escape(row.max_fee_per_gas.as_deref().unwrap_or("-")),
            escape(row.max_priority_fee_per_gas.as_deref().unwrap_or("-")),
            escape(row.revert_reason.as_deref().unwrap_or("-")),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"13\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_transactions_table(rows: &[TransactionRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>EOA</th><th>Tx Hash</th><th>Nonce</th><th>Status</th><th>Profit</th><th>Revert</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            copyable(&row.eoa),
            row.tx_hash
                .as_deref()
                .map(copyable)
                .unwrap_or_else(|| "-".to_string()),
            row.nonce,
            escape(&row.status),
            escape(row.realized_profit.as_deref().unwrap_or("-")),
            escape(row.revert_reason.as_deref().unwrap_or("-")),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"7\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn escape_js_string(value: &str) -> String {
    escape(
        &value
            .replace('\\', "\\\\")
            .replace('\'', "\\'")
            .replace('\n', "\\n")
            .replace('\r', "\\r"),
    )
}

fn render_select_options(options: &[&str], selected: &str) -> String {
    options
        .iter()
        .map(|option| {
            format!(
                r#"<option value="{value}" {selected}>{label}</option>"#,
                value = escape(option),
                selected = selected_attr(*option == selected),
                label = escape(option),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn selected_attr(selected: bool) -> &'static str {
    if selected {
        "selected"
    } else {
        ""
    }
}

fn checked_attr(checked: bool) -> &'static str {
    if checked {
        "checked"
    } else {
        ""
    }
}

fn bool_label(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn seen_blocks(first: Option<i64>, latest: Option<i64>) -> String {
    match (first, latest) {
        (Some(first), Some(latest)) if first == latest => first.to_string(),
        (Some(first), Some(latest)) => format!("{first}..{latest}"),
        (Some(first), None) => first.to_string(),
        (None, Some(latest)) => latest.to_string(),
        (None, None) => "-".to_string(),
    }
}

fn factory_variant_is_consistent(dex: &str, variant: &str) -> bool {
    matches!(
        (dex.trim(), variant.trim()),
        ("Aerodrome", "AerodromeVolatile")
            | ("Aerodrome", "AerodromeSlipstream")
            | ("UniswapV3", "UniswapV3")
            | ("PancakeSwap", "PancakeV3")
    )
}

fn inferred_dex_for_topic(topic0: &str) -> &'static str {
    match topic0.to_ascii_lowercase().as_str() {
        CLASSIC_SWAP_TOPIC => "Aerodrome",
        _ => "UniswapV3",
    }
}

fn inferred_variant_for_topic(topic0: &str) -> &'static str {
    match topic0.to_ascii_lowercase().as_str() {
        CLASSIC_SWAP_TOPIC => "AerodromeVolatile",
        _ => "UniswapV3",
    }
}

fn address_to_string(address: Address) -> String {
    format!("{address:#x}")
}

fn option_display(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
        .to_string()
}

fn summarize_token_changes(changes: &[TokenUpdateChange]) -> String {
    if changes.is_empty() {
        return String::new();
    }
    let mut parts = changes
        .iter()
        .take(6)
        .map(|change| {
            let anchor_note = if change.became_anchor {
                "; became funding anchor"
            } else {
                ""
            };
            format!(
                "{} {} [{}] amounts {} -> {}, min profit {} -> {}{}",
                change.symbol,
                change.token_address,
                change.scope,
                option_display(change.old_search_amounts.as_deref()),
                option_display(change.new_search_amounts.as_deref()),
                option_display(change.old_min_profit.as_deref()),
                option_display(change.new_min_profit.as_deref()),
                anchor_note
            )
        })
        .collect::<Vec<_>>();
    if changes.len() > 6 {
        parts.push(format!("+{} more token changes", changes.len() - 6));
    }
    format!("updated token defaults: {}", parts.join("; "))
}

fn url_query_escape(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn copyable(value: &str) -> String {
    let escaped = escape(value);
    format!(
        "<span class=\"copyable\"><span class=\"mono\" title=\"{0}\">{0}</span><button class=\"copy-btn\" type=\"button\" data-copy=\"{0}\" onclick=\"copyValue(this)\">copy</button></span>",
        escaped
    )
}

fn copyable_optional(value: Option<&str>) -> String {
    value.map(copyable).unwrap_or_else(|| "-".into())
}

fn token_label(symbol: Option<&str>, address: &str) -> String {
    let symbol = symbol
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let escaped_symbol = escape(symbol);
    let escaped_address = escape(address);
    format!(
        "<span class=\"token-label\" title=\"{escaped_address}\"><strong>{escaped_symbol}</strong><button class=\"copy-btn\" type=\"button\" data-copy=\"{escaped_address}\" onclick=\"copyValue(this)\">copy addr</button></span>"
    )
}

fn pool_mode(stable: Option<bool>) -> String {
    match stable {
        Some(true) => "stable".into(),
        Some(false) => "volatile".into(),
        None => "-".into(),
    }
}

fn monitoring_status(enabled: bool) -> String {
    if enabled {
        "<span class=\"ok\">enabled</span>".into()
    } else {
        "<span class=\"bad\">disabled</span>".into()
    }
}

fn effective_config_value(override_value: Option<&str>, default_value: Option<&str>) -> String {
    if let Some(value) = override_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return escape(value);
    }
    default_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| format!("<span class=\"muted\">inherited: {}</span>", escape(value)))
        .unwrap_or_else(|| "<span class=\"muted\">disabled</span>".into())
}

fn state_source_label(source: &str) -> String {
    match source {
        "local_event" => "<span class=\"ok\">local_event</span>".into(),
        "onchain_init" | "registry_reload" => {
            format!("<span class=\"warn\">{}</span>", escape(source))
        }
        "calibration_correction" => "<span class=\"bad\">calibration_correction</span>".into(),
        value => escape(value),
    }
}

fn activity_status(last_update_time: Option<DateTime<Utc>>) -> String {
    let Some(last_update_time) = last_update_time else {
        return "<span class=\"bad\">never seen</span>".into();
    };
    let age = Utc::now().signed_duration_since(last_update_time);
    if age <= Duration::minutes(10) {
        "<span class=\"ok\">active</span>".into()
    } else if age <= Duration::minutes(30) {
        "<span class=\"warn\">stale</span>".into()
    } else {
        "<span class=\"bad\">inactive</span>".into()
    }
}

fn password_input(placeholder: Option<&str>, autofocus: bool) -> String {
    format!(
        r#"<span class="password-wrap"><input name="password" type="password" {placeholder} autocomplete="current-password" required {autofocus}><button class="toggle-password" type="button" onclick="togglePassword(this)">show</button></span>"#,
        placeholder = placeholder
            .map(|value| format!(r#"placeholder="{}""#, escape(value)))
            .unwrap_or_default(),
        autofocus = if autofocus { "autofocus" } else { "" },
    )
}

fn password_matches(expected: Option<&str>, actual: &str) -> bool {
    expected
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|value| value == actual.trim())
}

fn password_matches_query(expected: Option<&str>, actual: Option<&str>) -> bool {
    let Some(expected) = expected.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    actual.map(str::trim).is_some_and(|value| value == expected)
}

fn canonical_pair(token_a: Address, token_b: Address) -> (Address, Address) {
    let token_a_key = format!("{token_a:#x}");
    let token_b_key = format!("{token_b:#x}");
    if token_a_key <= token_b_key {
        (token_a, token_b)
    } else {
        (token_b, token_a)
    }
}

fn normalize_raw_amount_list(raw: &str) -> std::result::Result<Option<String>, ()> {
    let values = raw
        .split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(normalize_raw_amount(trimmed).and_then(|value| value.ok_or(())))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values.join(",")))
    }
}

fn normalize_raw_amount(raw: &str) -> std::result::Result<Option<String>, ()> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    if !raw.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(());
    }
    let normalized = raw.trim_start_matches('0');
    if normalized.is_empty() {
        return Err(());
    }
    Ok(Some(normalized.to_string()))
}

async fn pair_symbol(provider: &ChainProvider, token0: Address, token1: Address) -> String {
    let symbol0 = provider
        .fetch_erc20_symbol(token0)
        .await
        .unwrap_or_else(|_| short_address(token0));
    let symbol1 = provider
        .fetch_erc20_symbol(token1)
        .await
        .unwrap_or_else(|_| short_address(token1));
    format!("{symbol0}/{symbol1}")
}

async fn token_symbol(provider: &ChainProvider, token: Address) -> String {
    provider
        .fetch_erc20_symbol(token)
        .await
        .unwrap_or_else(|_| short_address(token))
}

fn short_address(address: Address) -> String {
    let value = format!("{address:#x}");
    value.chars().take(8).collect()
}

fn dex_to_string(dex: DexKind) -> &'static str {
    match dex {
        DexKind::Aerodrome => "Aerodrome",
        DexKind::UniswapV3 => "UniswapV3",
        DexKind::PancakeSwap => "PancakeSwap",
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
        PoolVariant::PancakeV3 => "PancakeV3",
    }
}
