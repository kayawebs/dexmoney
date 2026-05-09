use std::sync::Arc;

use alloy_primitives::Address;
use anyhow::Result;
use axum::{
    extract::{Form, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use sqlx::{FromRow, PgPool};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    pool: Arc<PgPool>,
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
struct OpportunityRow {
    created_at: DateTime<Utc>,
    block_number: i64,
    strategy: String,
    amount_in: String,
    expected_profit: String,
    status: String,
}

#[derive(Debug, FromRow)]
struct SimulationRow {
    created_at: DateTime<Utc>,
    success: bool,
    simulated_profit: Option<String>,
    gas_estimate: Option<String>,
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
}

#[derive(Debug, FromRow)]
struct PoolRegistryRow {
    created_at: DateTime<Utc>,
    last_update_time: Option<DateTime<Utc>>,
    dex: String,
    variant: String,
    pair_symbol: Option<String>,
    pool_address: String,
    token0: String,
    token1: String,
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    fee_bps: Option<i64>,
    stable: Option<bool>,
    enabled: bool,
    source: String,
}

#[derive(Debug, Deserialize)]
struct AddPairForm {
    password: String,
    token0: String,
    token1: String,
}

#[derive(Debug, Deserialize)]
struct RediscoverPairForm {
    password: String,
    token0: String,
    token1: String,
}

#[derive(Debug, Deserialize)]
struct DeletePairForm {
    password: String,
    token0: String,
    token1: String,
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
    ensure_registry_schema(&pool).await?;
    let state = AppState {
        pool: Arc::new(pool),
        provider: ChainProvider::from_settings(&settings),
        admin_password: settings.monitor_web_password.clone(),
        settings,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/registry", get(registry_page))
        .route("/activity", get(activity_page))
        .route("/execution", get(execution_page))
        .route("/pairs", post(add_pair))
        .route("/pairs/rediscover", post(rediscover_pair))
        .route("/pairs/delete", post(delete_pair))
        .route("/pairs/remove", post(remove_pair))
        .route("/healthz", get(healthz))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8085").await?;
    info!("monitor-web listening on 0.0.0.0:8085");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    "ok"
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
    let warnings = fetch_pool_state_warnings(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let opportunities = fetch_opportunities(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
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
        render_pool_state_warnings_table(&warnings),
        render_pool_states_table(&pool_states),
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
        render_pool_states_table(&pool_states),
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

async fn add_pair(
    State(state): State<AppState>,
    Form(form): Form<AddPairForm>,
) -> Result<Html<String>, StatusCode> {
    if !password_matches(state.admin_password.as_deref(), &form.password) {
        return render_registry_response(
            &state.pool,
            None,
            Some("unauthorized: invalid monitor password"),
        )
        .await;
    }

    let result = discover_and_upsert_pair(&state, &form.token0, &form.token1).await?;
    let message = format!(
        "added pair {}; discovered {} pools",
        result.symbol, result.discovered_count
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

    let result = discover_and_upsert_pair(&state, &form.token0, &form.token1).await?;
    let message = format!(
        "rediscovered pair {symbol}; discovered {} pools",
        result.discovered_count,
        symbol = result.symbol,
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

struct PairDiscoveryResult {
    symbol: String,
    discovered_count: usize,
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

    Ok(PairDiscoveryResult {
        symbol,
        discovered_count: discovered.len(),
    })
}

async fn render_registry_response(
    pool: &PgPool,
    auth_password: Option<&str>,
    flash: Option<&str>,
) -> Result<Html<String>, StatusCode> {
    let token_pairs = fetch_token_pairs(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let registry_pools = fetch_registry_pools(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
        <section class="admin">
          <form method="post" action="/pairs">
            <label>Password
              {password_input}
            </label>
            <label>Token A
              <input name="token0" placeholder="0x..." required>
            </label>
            <label>Token B
              <input name="token1" placeholder="0x..." required>
            </label>
            <button type="submit">Discover Pools</button>
          </form>
        </section>
        <section class="card">
          <h2>Token Pairs</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Pool Registry</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
        render_token_pairs_table(&token_pairs),
        render_registry_pools_table(&registry_pools),
        password_input = password_input(None, true),
    );

    Ok(Html(render_page(
        "Registry",
        "Add token pairs and inspect discovered pools.",
        auth_password,
        flash,
        &content,
    )))
}

async fn fetch_token_pairs(pool: &PgPool) -> Result<Vec<TokenPairRow>> {
    Ok(sqlx::query_as::<_, TokenPairRow>(
        r#"
        SELECT created_at, chain_id, symbol, token0, token1, enabled
        FROM token_pairs
        ORDER BY created_at DESC
        LIMIT 50
        "#,
    )
    .fetch_all(pool)
    .await?)
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
        SELECT DISTINCT ON (ps.pool_address)
            ps.updated_at, ps.block_number, ps.dex, p.variant, ps.pool_address,
            ps.token0, ps.token1,
            split_part(tp.symbol, '/', 1) AS token0_symbol,
            split_part(tp.symbol, '/', 2) AS token1_symbol,
            ps.fee, ps.reserve0, ps.reserve1,
            ps.sqrt_price_x96, ps.liquidity, ps.tick
        FROM pool_states ps
        LEFT JOIN pools p ON lower(p.pool_address) = lower(ps.pool_address)
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        ORDER BY ps.pool_address, ps.updated_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
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

async fn fetch_opportunities(pool: &PgPool) -> Result<Vec<OpportunityRow>> {
    Ok(sqlx::query_as::<_, OpportunityRow>(
        r#"
        SELECT created_at, block_number, strategy, amount_in, expected_profit, status
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
        SELECT created_at, success, simulated_profit, gas_estimate, revert_reason
        FROM simulations
        ORDER BY created_at DESC
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
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    p {{ margin: 0 0 18px; color: var(--muted); }}
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
      max-height: 450px;
      overflow-y: auto;
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
  </script>
</head>
<body>
  <h1>{title}</h1>
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
  <title>Base Arb Monitor Login</title>
  <style>
    :root { --bg: #0d1117; --panel: #161b22; --text: #e6edf3; --muted: #93a1b2; --line: #2d3742; --accent: #4ad295; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; background: radial-gradient(circle at top, #142033 0%, var(--bg) 48%); color: var(--text); }
    form { width: min(420px, calc(100vw - 32px)); padding: 22px; border: 1px solid var(--line); border-radius: 16px; background: var(--panel); box-shadow: 0 18px 50px rgba(0,0,0,0.26); }
    h1 { margin: 0 0 8px; font-size: 22px; }
    p { margin: 0 0 18px; color: var(--muted); }
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
    <h1>Base Arb Monitor</h1>
    <p>Enter `MONITOR_WEB_PASSWORD` from `.env`.</p>
    <label>Password
      __PASSWORD_INPUT__
    </label>
    <button type="submit">Open Monitor</button>
  </form>
</body>
</html>"#
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
        "<div class=\"table-scroll\"><table><thead><tr><th>Created</th><th>Chain</th><th>Symbol</th><th>Token 0</th><th>Token 1</th><th>Enabled</th><th>Actions</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.chain_id,
            escape(&row.symbol),
            copyable(&row.token0),
            copyable(&row.token1),
            row.enabled,
            render_pair_actions(row),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"7\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_pair_actions(row: &TokenPairRow) -> String {
    format!(
        "{}{}{}",
        render_rediscover_form(row),
        render_delete_pair_form(row),
        render_remove_pair_form(row)
    )
}

fn render_rediscover_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/rediscover" class="inline-form">
  {password_input}
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button type="submit">Discover Pools</button>
</form>"#,
        password_input = password_input(Some("password"), false),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_delete_pair_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/delete" class="inline-form">
  {password_input}
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button class="delete-btn" type="submit">Disable Pair</button>
</form>"#,
        password_input = password_input(Some("password"), false),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_remove_pair_form(row: &TokenPairRow) -> String {
    format!(
        r#"<form method="post" action="/pairs/remove" class="inline-form" onsubmit="return confirm('Delete this token pair and associated pool registry rows? Historical events and state snapshots will be kept.');">
  {password_input}
  <input name="token0" type="hidden" value="{token0}">
  <input name="token1" type="hidden" value="{token1}">
  <button class="danger-btn" type="submit">Delete Pair</button>
</form>"#,
        password_input = password_input(Some("password"), false),
        token0 = escape(&row.token0),
        token1 = escape(&row.token1),
    )
}

fn render_registry_pools_table(rows: &[PoolRegistryRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Created</th><th>Last Update Time</th><th>Activity</th><th>DEX</th><th>Variant</th><th>Pair</th><th>Pool</th><th>Token 0</th><th>Token 1</th><th>Fee BPS</th><th>Pool Mode</th><th>Monitoring</th><th>Source</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
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
            token_label(row.token0_symbol.as_deref(), &row.token0),
            token_label(row.token1_symbol.as_deref(), &row.token1),
            row.fee_bps.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            pool_mode(row.stable),
            monitoring_status(row.enabled),
            escape(&row.source),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"13\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_pool_states_table(rows: &[PoolStateRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Updated</th><th>Block</th><th>DEX</th><th>Variant</th><th>Tick</th><th>Pool</th><th>Token 0</th><th>Token 1</th><th>Fee</th><th>Reserve 0</th><th>Reserve 1</th><th>SqrtPriceX96</th><th>Liquidity</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"state-number\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            row.block_number,
            escape(&row.dex),
            row.variant
                .as_deref()
                .map(escape)
                .unwrap_or_else(|| "-".into()),
            row.tick.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
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
        html.push_str("<tr><td colspan=\"13\">No rows yet. Start market-data and make sure at least one pool is enabled.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
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
        html.push_str("<tr><td colspan=\"7\">No state warnings.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_opportunities_table(rows: &[OpportunityRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Block</th><th>Strategy</th><th>Amount In</th><th>Expected Profit</th><th>Status</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.block_number,
            escape(&row.strategy),
            escape(&row.amount_in),
            escape(&row.expected_profit),
            escape(&row.status),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"6\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_simulations_table(rows: &[SimulationRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Time</th><th>Success</th><th>Profit</th><th>Gas</th><th>Revert</th></tr></thead><tbody>",
    );
    for row in rows {
        let class = if row.success { "ok" } else { "bad" };
        html.push_str(&format!(
            "<tr><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            class,
            row.success,
            escape(row.simulated_profit.as_deref().unwrap_or("-")),
            escape(row.gas_estimate.as_deref().unwrap_or("-")),
            escape(row.revert_reason.as_deref().unwrap_or("-")),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"5\">No rows yet.</td></tr>");
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

fn short_address(address: Address) -> String {
    let value = format!("{address:#x}");
    value.chars().take(8).collect()
}
