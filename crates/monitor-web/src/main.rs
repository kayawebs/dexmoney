use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use base_arb_common::config::Settings;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    pool: Arc<PgPool>,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let settings = Settings::load()?;
    let pool = PgPool::connect(&settings.postgres_url).await?;
    let state = AppState {
        pool: Arc::new(pool),
    };

    let app = Router::new()
        .route("/", get(index))
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

async fn index(State(state): State<AppState>) -> Result<Html<String>, axum::http::StatusCode> {
    let events = fetch_dex_events(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let pool_states = fetch_pool_states(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let opportunities = fetch_opportunities(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let simulations = fetch_simulations(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let transactions = fetch_transactions(&state.pool)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Html(render_dashboard(
        &events,
        &pool_states,
        &opportunities,
        &simulations,
        &transactions,
    )))
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

fn render_dashboard(
    events: &[DexEventRow],
    pool_states: &[PoolStateRow],
    opportunities: &[OpportunityRow],
    simulations: &[SimulationRow],
    transactions: &[TransactionRow],
) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Base Arb Monitor</title>
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
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      gap: 16px;
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
      max-width: 180px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      display: inline-block;
    }}
  </style>
</head>
<body>
  <h1>Base Arb Monitor</h1>
  <p>Read-only Postgres dashboard for recent system activity.</p>
  <div class="grid">
    <section class="card">
      <h2>DEX Events</h2>
      {events}
    </section>
    <section class="card">
      <h2>Pool States</h2>
      {pool_states}
    </section>
    <section class="card">
      <h2>Opportunities</h2>
      {opportunities}
    </section>
    <section class="card">
      <h2>Simulations</h2>
      {simulations}
    </section>
    <section class="card">
      <h2>Transactions</h2>
      {transactions}
    </section>
  </div>
</body>
</html>"#,
        events = render_events_table(events),
        pool_states = render_pool_states_table(pool_states),
        opportunities = render_opportunities_table(opportunities),
        simulations = render_simulations_table(simulations),
        transactions = render_transactions_table(transactions),
    )
}

fn render_events_table(rows: &[DexEventRow]) -> String {
    let mut html = String::from(
        "<table><thead><tr><th>Time</th><th>Block</th><th>DEX</th><th>Event</th><th>Pool</th><th>Tx</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><span class=\"mono\">{}</span></td><td><span class=\"mono\">{}</span></td></tr>",
            fmt_ts(row.created_at),
            row.block_number,
            escape(&row.dex),
            escape(&row.event_type),
            escape(&row.pool_address),
            escape(&row.tx_hash),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"6\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table>");
    html
}

fn render_pool_states_table(rows: &[PoolStateRow]) -> String {
    let mut html = String::from(
        "<table><thead><tr><th>Updated</th><th>Block</th><th>DEX</th><th>Pool</th><th>Pair</th><th>Fee</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td><span class=\"mono\">{}</span></td><td><span class=\"mono\">{}/{}\
            </span></td><td>{}</td></tr>",
            fmt_ts(row.updated_at),
            row.block_number,
            escape(&row.dex),
            escape(&row.pool_address),
            escape(&row.token0),
            escape(&row.token1),
            row.fee.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"6\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table>");
    html
}

fn render_opportunities_table(rows: &[OpportunityRow]) -> String {
    let mut html = String::from(
        "<table><thead><tr><th>Time</th><th>Block</th><th>Strategy</th><th>Amount In</th><th>Expected Profit</th><th>Status</th></tr></thead><tbody>",
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
    html.push_str("</tbody></table>");
    html
}

fn render_simulations_table(rows: &[SimulationRow]) -> String {
    let mut html = String::from(
        "<table><thead><tr><th>Time</th><th>Success</th><th>Profit</th><th>Gas</th><th>Revert</th></tr></thead><tbody>",
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
    html.push_str("</tbody></table>");
    html
}

fn render_transactions_table(rows: &[TransactionRow]) -> String {
    let mut html = String::from(
        "<table><thead><tr><th>Time</th><th>EOA</th><th>Tx Hash</th><th>Nonce</th><th>Status</th><th>Profit</th><th>Revert</th></tr></thead><tbody>",
    );
    for row in rows {
        html.push_str(&format!(
            "<tr><td>{}</td><td><span class=\"mono\">{}</span></td><td><span class=\"mono\">{}</span></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            fmt_ts(row.created_at),
            escape(&row.eoa),
            escape(row.tx_hash.as_deref().unwrap_or("-")),
            row.nonce,
            escape(&row.status),
            escape(row.realized_profit.as_deref().unwrap_or("-")),
            escape(row.revert_reason.as_deref().unwrap_or("-")),
        ));
    }
    if rows.is_empty() {
        html.push_str("<tr><td colspan=\"7\">No rows yet.</td></tr>");
    }
    html.push_str("</tbody></table>");
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
