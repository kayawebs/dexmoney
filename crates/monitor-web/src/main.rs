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
use chrono::{DateTime, Utc};
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
struct PoolStateRow {
    updated_at: DateTime<Utc>,
    block_number: i64,
    dex: String,
    pool_address: String,
    token0: String,
    token1: String,
    fee: Option<i64>,
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
    dex: String,
    variant: String,
    pool_address: String,
    token0: String,
    token1: String,
    fee_bps: Option<i64>,
    tick_spacing: Option<i64>,
    stable: Option<bool>,
    enabled: bool,
    source: String,
}

#[derive(Debug, Deserialize)]
struct AddPairForm {
    password: String,
    symbol: String,
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
    let opportunities = fetch_opportunities(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let content = format!(
        r#"
        <section class="card">
          <h2>Pool States</h2>
          <div class="card-body">{}</div>
        </section>
        <section class="card">
          <h2>Opportunities</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
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
          <h2>Pool States</h2>
          <div class="card-body">{}</div>
        </section>
        "#,
        render_events_table(&events),
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
    let symbol = normalized_symbol(&form.symbol, &form.token0, &form.token1);

    let discovered = state
        .provider
        .discover_pools_for_pair(&state.settings, token0, token1)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    if discovered.is_empty() {
        return render_registry_response(&state.pool, None, Some("no pools found for this pair"))
            .await;
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

    let message = format!("added pair {symbol}; discovered {} pools", discovered.len());
    render_registry_response(&state.pool, None, Some(&message)).await
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
              <input name="password" type="password" autocomplete="current-password" required>
            </label>
            <label>Symbol
              <input name="symbol" placeholder="USDC/WETH" required>
            </label>
            <label>Token 0
              <input name="token0" placeholder="0x..." required>
            </label>
            <label>Token 1
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
        SELECT created_at, dex, variant, pool_address, token0, token1, fee_bps,
            tick_spacing, stable, enabled, source
        FROM pools
        ORDER BY created_at DESC
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
        ORDER BY created_at DESC
        LIMIT 25
        "#,
    )
    .fetch_all(pool)
    .await?)
}

async fn fetch_pool_states(pool: &PgPool) -> Result<Vec<PoolStateRow>> {
    Ok(sqlx::query_as::<_, PoolStateRow>(
        r#"
        SELECT DISTINCT ON (pool_address)
            updated_at, block_number, dex, pool_address, token0, token1, fee
        FROM pool_states
        ORDER BY pool_address, updated_at DESC
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
    .admin form {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      align-items: end;
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
  </style>
</head>
<body>
  <form method="get" action="/">
    <h1>Base Arb Monitor</h1>
    <p>Enter `MONITOR_WEB_PASSWORD` from `.env`.</p>
    <label>Password
      <input name="password" type="password" autocomplete="current-password" required autofocus>
    </label>
    <button type="submit">Open Monitor</button>
  </form>
</body>
</html>"#
        .to_string()
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
        "<div class=\"table-scroll\"><table><thead><tr><th>Created</th><th>Chain</th><th>Symbol</th><th>Token 0</th><th>Token 1</th><th>Enabled</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            row.chain_id,
            escape(&row.symbol),
            copyable(&row.token0),
            copyable(&row.token1),
            row.enabled,
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"6\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_registry_pools_table(rows: &[PoolRegistryRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Created</th><th>DEX</th><th>Variant</th><th>Pool</th><th>Token 0</th><th>Token 1</th><th>Fee</th><th>Tick</th><th>Stable</th><th>Enabled</th><th>Source</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            escape(&row.dex),
            escape(&row.variant),
            copyable(&row.pool_address),
            copyable(&row.token0),
            copyable(&row.token1),
            row.fee_bps.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            row.tick_spacing.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            row.stable.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            row.enabled,
            escape(&row.source),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"11\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table></div>");
    html
}

fn render_pool_states_table(rows: &[PoolStateRow]) -> String {
    let mut html = String::from(
        "<div class=\"table-scroll\"><table><thead><tr><th>Updated</th><th>Block</th><th>DEX</th><th>Pool</th><th>Token 0</th><th>Token 1</th><th>Fee</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            row.block_number,
            escape(&row.dex),
            copyable(&row.pool_address),
            copyable(&row.token0),
            copyable(&row.token1),
            row.fee.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"7\">No rows yet.</td></tr>");
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

fn normalized_symbol(symbol: &str, token0: &str, token1: &str) -> String {
    let symbol = symbol.trim();
    if !symbol.is_empty() {
        return symbol.to_string();
    }
    format!(
        "{}/{}",
        &token0[..token0.len().min(6)],
        &token1[..token1.len().min(6)]
    )
}
