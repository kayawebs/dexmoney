use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::Candidate;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use sqlx::{PgPool, Row};
use tokio::time::{sleep, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct MonitorConfig {
    interval_secs: u64,
    once: bool,
    block_lag_warn: u64,
    block_lag_critical: u64,
    pool_state_stale_warn_secs: i64,
    pool_state_stale_critical_secs: i64,
    no_opportunity_warn_secs: i64,
    no_opportunity_critical_secs: i64,
    opportunity_flow_window_secs: i64,
    opportunity_flow_min_pool_states: i64,
    opportunity_flow_warn_opportunities: i64,
    opportunity_flow_critical_opportunities: i64,
    funded_path_min_pairs: i64,
    funded_path_min_ready_pairs: i64,
    candidate_sample: usize,
    missing_tick_sample: i64,
    telegram_token: Option<String>,
    telegram_chat_id: Option<String>,
    telegram_min_severity: Severity,
    telegram_throttle_secs: u64,
}

#[derive(Debug, Clone)]
struct HealthCheck {
    name: &'static str,
    severity: Severity,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Severity {
    Ok,
    Warn,
    Critical,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let config = MonitorConfig::from_env()?;
    let provider = ChainProvider::from_settings(&settings);
    let pg = PgPool::connect(&settings.postgres_url)
        .await
        .context("connect postgres")?;
    let redis_client = redis::Client::open(settings.redis_url.clone())?;
    let redis = ConnectionManager::new(redis_client)
        .await
        .context("connect redis")?;
    let telegram = TelegramClient::from_config(&config);
    let mut last_alert: Option<Instant> = None;

    loop {
        let mut redis_conn = redis.clone();
        let checks = collect_checks(&config, &provider, &pg, &mut redis_conn).await;
        emit_checks(&checks);
        if let Some(client) = &telegram {
            maybe_send_telegram(&config, client, &checks, &mut last_alert).await;
        }
        if config.once {
            break;
        }
        sleep(Duration::from_secs(config.interval_secs)).await;
    }
    Ok(())
}

impl MonitorConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            interval_secs: env_u64("HEALTH_MONITOR_INTERVAL_SECS", 30),
            once: env::args().any(|arg| arg == "--once"),
            block_lag_warn: env_u64("HEALTH_BLOCK_LAG_WARN", 3),
            block_lag_critical: env_u64("HEALTH_BLOCK_LAG_CRITICAL", 10),
            pool_state_stale_warn_secs: env_i64("HEALTH_POOL_STATE_STALE_WARN_SECS", 15),
            pool_state_stale_critical_secs: env_i64("HEALTH_POOL_STATE_STALE_CRITICAL_SECS", 60),
            no_opportunity_warn_secs: env_i64("HEALTH_NO_OPPORTUNITY_WARN_SECS", 600),
            no_opportunity_critical_secs: env_i64("HEALTH_NO_OPPORTUNITY_CRITICAL_SECS", 1800),
            opportunity_flow_window_secs: env_i64("HEALTH_OPPORTUNITY_FLOW_WINDOW_SECS", 600),
            opportunity_flow_min_pool_states: env_i64(
                "HEALTH_OPPORTUNITY_FLOW_MIN_POOL_STATES",
                500,
            ),
            opportunity_flow_warn_opportunities: env_i64(
                "HEALTH_OPPORTUNITY_FLOW_WARN_OPPORTUNITIES",
                10,
            ),
            opportunity_flow_critical_opportunities: env_i64(
                "HEALTH_OPPORTUNITY_FLOW_CRITICAL_OPPORTUNITIES",
                1,
            ),
            funded_path_min_pairs: env_i64("HEALTH_FUNDED_PATH_MIN_PAIRS", 1),
            funded_path_min_ready_pairs: env_i64("HEALTH_FUNDED_PATH_MIN_READY_PAIRS", 1),
            candidate_sample: env_usize("HEALTH_CANDIDATE_SAMPLE", 50),
            missing_tick_sample: env_i64("HEALTH_MISSING_TICK_SAMPLE", 100),
            telegram_token: env::var("HEALTH_TELEGRAM_BOT_TOKEN")
                .ok()
                .or_else(|| env::var("TELEGRAM_BOT_TOKEN").ok()),
            telegram_chat_id: env::var("HEALTH_TELEGRAM_CHAT_ID")
                .ok()
                .or_else(|| env::var("TELEGRAM_CHAT_ID").ok()),
            telegram_min_severity: env_severity("HEALTH_TELEGRAM_MIN_SEVERITY", Severity::Warn),
            telegram_throttle_secs: env_u64("HEALTH_TELEGRAM_THROTTLE_SECS", 300),
        })
    }
}

async fn collect_checks(
    config: &MonitorConfig,
    provider: &ChainProvider,
    pg: &PgPool,
    redis: &mut ConnectionManager,
) -> Vec<HealthCheck> {
    let mut checks = Vec::new();
    checks.push(check_block_lag(config, provider, redis).await);
    checks.push(
        check_table_freshness(
            pg,
            "pool_states",
            "updated_at",
            config.pool_state_stale_warn_secs,
            config.pool_state_stale_critical_secs,
        )
        .await,
    );
    checks.push(
        check_table_freshness(
            pg,
            "opportunities",
            "created_at",
            config.no_opportunity_warn_secs,
            config.no_opportunity_critical_secs,
        )
        .await,
    );
    checks.push(check_table_freshness(pg, "simulations", "created_at", 900, 3600).await);
    checks.push(check_opportunity_flow(config, pg).await);
    checks.push(check_funded_path_readiness(config, pg).await);
    checks.push(check_candidate_queue(config, provider, redis).await);
    checks.push(check_missing_ticks(config, pg).await);
    checks
}

async fn check_block_lag(
    config: &MonitorConfig,
    provider: &ChainProvider,
    redis: &mut ConnectionManager,
) -> HealthCheck {
    let latest = match provider.get_block_number().await {
        Ok(block) => block,
        Err(err) => {
            return HealthCheck {
                name: "block_lag",
                severity: Severity::Critical,
                message: format!("rpc latest block failed: {err:#}"),
            };
        }
    };
    let redis_block = match redis.get::<_, Option<String>>("chain:current_block").await {
        Ok(Some(value)) => value.parse::<u64>().ok(),
        Ok(None) => None,
        Err(err) => {
            return HealthCheck {
                name: "block_lag",
                severity: Severity::Critical,
                message: format!("redis current block read failed: {err}"),
            };
        }
    };
    let Some(redis_block) = redis_block else {
        return HealthCheck {
            name: "block_lag",
            severity: Severity::Critical,
            message: format!("missing chain:current_block rpc_latest={latest}"),
        };
    };
    let lag = latest.saturating_sub(redis_block);
    let severity = if lag >= config.block_lag_critical {
        Severity::Critical
    } else if lag >= config.block_lag_warn {
        Severity::Warn
    } else {
        Severity::Ok
    };
    HealthCheck {
        name: "block_lag",
        severity,
        message: format!("rpc_latest={latest} redis_current={redis_block} lag_blocks={lag}"),
    }
}

async fn check_table_freshness(
    pg: &PgPool,
    table: &'static str,
    time_column: &'static str,
    warn_secs: i64,
    critical_secs: i64,
) -> HealthCheck {
    let sql = format!("SELECT max({time_column}) AS latest FROM {table}");
    let latest = match sqlx::query_scalar::<_, Option<DateTime<Utc>>>(&sql)
        .fetch_one(pg)
        .await
    {
        Ok(latest) => latest,
        Err(err) => {
            return HealthCheck {
                name: table,
                severity: Severity::Critical,
                message: format!("freshness query failed: {err:#}"),
            };
        }
    };
    let Some(latest) = latest else {
        return HealthCheck {
            name: table,
            severity: Severity::Warn,
            message: "no rows".to_string(),
        };
    };
    let age_secs = Utc::now().signed_duration_since(latest).num_seconds();
    let severity = if age_secs >= critical_secs {
        Severity::Critical
    } else if age_secs >= warn_secs {
        Severity::Warn
    } else {
        Severity::Ok
    };
    HealthCheck {
        name: table,
        severity,
        message: format!("latest={latest} age_secs={age_secs}"),
    }
}

async fn check_candidate_queue(
    config: &MonitorConfig,
    provider: &ChainProvider,
    redis: &mut ConnectionManager,
) -> HealthCheck {
    let latest = match provider.get_block_number().await {
        Ok(block) => block,
        Err(err) => {
            return HealthCheck {
                name: "candidate_queue",
                severity: Severity::Warn,
                message: format!("skip candidate freshness because rpc failed: {err:#}"),
            };
        }
    };
    let depth = match redis.zcard::<_, u64>("candidates:priority").await {
        Ok(depth) => depth,
        Err(err) => {
            return HealthCheck {
                name: "candidate_queue",
                severity: Severity::Critical,
                message: format!("candidate zcard failed: {err}"),
            };
        }
    };
    if depth == 0 {
        return HealthCheck {
            name: "candidate_queue",
            severity: Severity::Ok,
            message: "depth=0".to_string(),
        };
    }

    let payloads: Vec<String> = match redis::cmd("ZREVRANGE")
        .arg("candidates:priority")
        .arg(0)
        .arg(config.candidate_sample.saturating_sub(1))
        .query_async(redis)
        .await
    {
        Ok(payloads) => payloads,
        Err(err) => {
            return HealthCheck {
                name: "candidate_queue",
                severity: Severity::Critical,
                message: format!("candidate sample read failed: {err}"),
            };
        }
    };

    let mut fresh = 0u64;
    let mut stale = 0u64;
    let mut max_lag = 0u64;
    for payload in payloads {
        let Ok(candidate) = serde_json::from_str::<Candidate>(&payload) else {
            continue;
        };
        let lag = latest.saturating_sub(candidate.block_number);
        max_lag = max_lag.max(lag);
        if lag <= 1 {
            fresh += 1;
        } else {
            stale += 1;
        }
    }
    let severity = if stale > 0 && fresh == 0 {
        Severity::Critical
    } else if stale > fresh {
        Severity::Warn
    } else {
        Severity::Ok
    };
    HealthCheck {
        name: "candidate_queue",
        severity,
        message: format!(
            "depth={depth} sample_fresh={fresh} sample_stale={stale} max_lag_blocks={max_lag}"
        ),
    }
}

async fn check_opportunity_flow(config: &MonitorConfig, pg: &PgPool) -> HealthCheck {
    let row = match sqlx::query(
        r#"
        SELECT
          (SELECT count(*) FROM pool_states WHERE updated_at >= NOW() - ($1::double precision * INTERVAL '1 second')) AS pool_states,
          (SELECT count(*) FROM opportunities WHERE created_at >= NOW() - ($1::double precision * INTERVAL '1 second')) AS opportunities,
          (SELECT count(*) FROM simulations WHERE created_at >= NOW() - ($1::double precision * INTERVAL '1 second')) AS simulations,
          (SELECT count(*) FROM transactions WHERE created_at >= NOW() - ($1::double precision * INTERVAL '1 second')) AS transactions
        "#,
    )
    .bind(config.opportunity_flow_window_secs)
    .fetch_one(pg)
    .await
    {
        Ok(row) => row,
        Err(err) => {
            return HealthCheck {
                name: "opportunity_flow",
                severity: Severity::Warn,
                message: format!("flow query failed: {err:#}"),
            };
        }
    };

    let pool_states: i64 = row.try_get("pool_states").unwrap_or_default();
    let opportunities: i64 = row.try_get("opportunities").unwrap_or_default();
    let simulations: i64 = row.try_get("simulations").unwrap_or_default();
    let transactions: i64 = row.try_get("transactions").unwrap_or_default();

    let severity = if pool_states < config.opportunity_flow_min_pool_states {
        Severity::Ok
    } else if opportunities < config.opportunity_flow_critical_opportunities {
        Severity::Critical
    } else if opportunities < config.opportunity_flow_warn_opportunities {
        Severity::Warn
    } else {
        Severity::Ok
    };
    HealthCheck {
        name: "opportunity_flow",
        severity,
        message: format!(
            "window_secs={} pool_states={} opportunities={} simulations={} transactions={} min_pool_states={} warn_opportunities={} critical_opportunities={}",
            config.opportunity_flow_window_secs,
            pool_states,
            opportunities,
            simulations,
            transactions,
            config.opportunity_flow_min_pool_states,
            config.opportunity_flow_warn_opportunities,
            config.opportunity_flow_critical_opportunities
        ),
    }
}

async fn check_funded_path_readiness(config: &MonitorConfig, pg: &PgPool) -> HealthCheck {
    let row = match sqlx::query(
        r#"
        WITH defaults AS (
          SELECT chain_id, lower(token_address) AS token, executor_scope, NULLIF(BTRIM(search_amounts), '') AS search_amounts
          FROM token_search_defaults
        ),
        funded_pairs AS (
          SELECT
            tp.id,
            tp.symbol,
            COALESCE(NULLIF(BTRIM(tp.token0_search_amounts), ''), d0_two.search_amounts, d0_multi.search_amounts, d0_all.search_amounts) AS token0_amounts,
            COALESCE(NULLIF(BTRIM(tp.token1_search_amounts), ''), d1_two.search_amounts, d1_multi.search_amounts, d1_all.search_amounts) AS token1_amounts
          FROM token_pairs tp
          LEFT JOIN defaults d0_all ON d0_all.chain_id = tp.chain_id AND d0_all.token = lower(tp.token0) AND d0_all.executor_scope = 'all'
          LEFT JOIN defaults d0_two ON d0_two.chain_id = tp.chain_id AND d0_two.token = lower(tp.token0) AND d0_two.executor_scope = 'two_hop'
          LEFT JOIN defaults d0_multi ON d0_multi.chain_id = tp.chain_id AND d0_multi.token = lower(tp.token0) AND d0_multi.executor_scope = 'multihop'
          LEFT JOIN defaults d1_all ON d1_all.chain_id = tp.chain_id AND d1_all.token = lower(tp.token1) AND d1_all.executor_scope = 'all'
          LEFT JOIN defaults d1_two ON d1_two.chain_id = tp.chain_id AND d1_two.token = lower(tp.token1) AND d1_two.executor_scope = 'two_hop'
          LEFT JOIN defaults d1_multi ON d1_multi.chain_id = tp.chain_id AND d1_multi.token = lower(tp.token1) AND d1_multi.executor_scope = 'multihop'
          WHERE tp.enabled
        ),
        pair_pools AS (
          SELECT
            fp.id,
            fp.symbol,
            count(p.id) FILTER (WHERE p.enabled) AS enabled_pools
          FROM funded_pairs fp
          LEFT JOIN pools p ON p.token_pair_id = fp.id
          WHERE fp.token0_amounts IS NOT NULL OR fp.token1_amounts IS NOT NULL
          GROUP BY fp.id, fp.symbol
        )
        SELECT
          count(*) AS funded_pairs,
          count(*) FILTER (WHERE enabled_pools >= 2) AS ready_pairs,
          COALESCE(sum(enabled_pools), 0) AS enabled_pools,
          COALESCE(sum(GREATEST(enabled_pools * (enabled_pools - 1), 0)), 0) AS ordered_two_pool_paths,
          string_agg(symbol || ':' || enabled_pools::text, ', ' ORDER BY enabled_pools DESC, symbol) FILTER (WHERE enabled_pools < 2) AS weak_examples
        FROM pair_pools
        "#,
    )
    .fetch_one(pg)
    .await
    {
        Ok(row) => row,
        Err(err) => {
            return HealthCheck {
                name: "funded_path_readiness",
                severity: Severity::Warn,
                message: format!("funded path query failed: {err:#}"),
            };
        }
    };

    let funded_pairs: i64 = row.try_get("funded_pairs").unwrap_or_default();
    let ready_pairs: i64 = row.try_get("ready_pairs").unwrap_or_default();
    let enabled_pools: i64 = row.try_get("enabled_pools").unwrap_or_default();
    let ordered_two_pool_paths: i64 = row.try_get("ordered_two_pool_paths").unwrap_or_default();
    let weak_examples: Option<String> = row.try_get("weak_examples").ok().flatten();
    let weak_examples = weak_examples
        .as_deref()
        .map(|examples| examples.chars().take(240).collect::<String>())
        .unwrap_or_else(|| "-".to_string());

    let severity = if funded_pairs < config.funded_path_min_pairs {
        Severity::Critical
    } else if ready_pairs < config.funded_path_min_ready_pairs {
        Severity::Warn
    } else {
        Severity::Ok
    };
    HealthCheck {
        name: "funded_path_readiness",
        severity,
        message: format!(
            "funded_pairs={} ready_pairs={} enabled_pools={} ordered_two_pool_paths={} min_pairs={} min_ready_pairs={} weak_examples={}",
            funded_pairs,
            ready_pairs,
            enabled_pools,
            ordered_two_pool_paths,
            config.funded_path_min_pairs,
            config.funded_path_min_ready_pairs,
            weak_examples
        ),
    }
}

async fn check_missing_ticks(config: &MonitorConfig, pg: &PgPool) -> HealthCheck {
    let rows = match sqlx::query(
        r#"
        WITH latest_state AS (
          SELECT DISTINCT ON (lower(pool_address))
            lower(pool_address) AS pool,
            updated_at
          FROM pool_states
          WHERE updated_at >= NOW() - INTERVAL '1 hour'
          ORDER BY lower(pool_address), updated_at DESC
        )
        SELECT
          p.pool_address,
          p.variant,
          tc.status,
          tc.tick_count,
          tc.updated_at AS coverage_updated_at,
          ls.updated_at AS state_updated_at
        FROM latest_state ls
        JOIN pools p ON lower(p.pool_address) = ls.pool
        LEFT JOIN pool_tick_coverage tc
          ON tc.chain_id = p.chain_id
         AND lower(tc.pool_address) = lower(p.pool_address)
        WHERE p.enabled
          AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3', 'UniswapV4')
        ORDER BY ls.updated_at DESC
        LIMIT $1
        "#,
    )
    .bind(config.missing_tick_sample)
    .fetch_all(pg)
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return HealthCheck {
                name: "missing_ticks",
                severity: Severity::Warn,
                message: format!("missing tick sample query failed: {err:#}"),
            };
        }
    };

    if rows.is_empty() {
        return HealthCheck {
            name: "missing_ticks",
            severity: Severity::Ok,
            message: "sample=0".to_string(),
        };
    }

    let mut missing = 0usize;
    let mut failed = 0usize;
    let mut examples = Vec::new();
    for row in &rows {
        let pool: String = row.try_get("pool_address").unwrap_or_default();
        let status: Option<String> = row.try_get("status").ok().flatten();
        let tick_count: Option<i64> = row.try_get("tick_count").ok().flatten();
        let is_missing = match status.as_deref() {
            Some("ready") => tick_count.unwrap_or_default() <= 0,
            Some("zero_ticks") => false,
            Some("refresh_failed") => {
                failed += 1;
                true
            }
            Some(_) => false,
            None => true,
        };
        if is_missing {
            missing += 1;
            if examples.len() < 5 {
                examples.push(format!(
                    "{}:{}",
                    pool,
                    status.as_deref().unwrap_or("unscanned")
                ));
            }
        }
    }
    let severity = if missing * 2 >= rows.len() {
        Severity::Critical
    } else if missing > 0 {
        Severity::Warn
    } else {
        Severity::Ok
    };
    let examples = examples.join(",");
    HealthCheck {
        name: "missing_ticks",
        severity,
        message: format!(
            "sample={} missing={} failed={} examples={}",
            rows.len(),
            missing,
            failed,
            if examples.is_empty() { "-" } else { &examples }
        ),
    }
}

fn emit_checks(checks: &[HealthCheck]) {
    let overall = checks
        .iter()
        .map(|check| check.severity)
        .max()
        .unwrap_or(Severity::Ok);
    match overall {
        Severity::Ok => info!(status = "ok", "health monitor summary"),
        Severity::Warn => warn!(status = "warn", "health monitor summary"),
        Severity::Critical => error!(status = "critical", "health monitor summary"),
    }
    for check in checks {
        match check.severity {
            Severity::Ok => info!(check = check.name, message = %check.message, "health check ok"),
            Severity::Warn => {
                warn!(check = check.name, message = %check.message, "health check warn")
            }
            Severity::Critical => {
                error!(check = check.name, message = %check.message, "health check critical")
            }
        }
    }
}

#[derive(Clone)]
struct TelegramClient {
    token: String,
    chat_id: String,
    client: reqwest::Client,
}

impl TelegramClient {
    fn from_config(config: &MonitorConfig) -> Option<Self> {
        Some(Self {
            token: config.telegram_token.clone()?,
            chat_id: config.telegram_chat_id.clone()?,
            client: reqwest::Client::new(),
        })
    }
}

async fn maybe_send_telegram(
    config: &MonitorConfig,
    client: &TelegramClient,
    checks: &[HealthCheck],
    last_alert: &mut Option<Instant>,
) {
    let max_severity = checks
        .iter()
        .map(|check| check.severity)
        .max()
        .unwrap_or(Severity::Ok);
    if max_severity < config.telegram_min_severity {
        return;
    }
    if last_alert
        .is_some_and(|last| last.elapsed() < Duration::from_secs(config.telegram_throttle_secs))
    {
        return;
    }

    let lines = checks
        .iter()
        .filter(|check| check.severity >= config.telegram_min_severity)
        .map(|check| format!("{:?} {}: {}", check.severity, check.name, check.message))
        .collect::<Vec<_>>()
        .join("\n");
    let text = format!("dexmoney health {:?}\n{}", max_severity, lines);
    let url = format!("https://api.telegram.org/bot{}/sendMessage", client.token);
    let result = client
        .client
        .post(url)
        .json(&serde_json::json!({
            "chat_id": client.chat_id,
            "text": text,
            "disable_web_page_preview": true
        }))
        .send()
        .await;
    match result {
        Ok(response) if response.status().is_success() => {
            *last_alert = Some(Instant::now());
            info!("telegram health alert sent");
        }
        Ok(response) => warn!(status = %response.status(), "telegram health alert failed"),
        Err(err) => warn!(error = %err, "telegram health alert failed"),
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_severity(name: &str, default: Severity) -> Severity {
    match env::var(name).ok().as_deref() {
        Some("ok") | Some("OK") => Severity::Ok,
        Some("warn") | Some("WARN") | Some("warning") | Some("WARNING") => Severity::Warn,
        Some("critical") | Some("CRITICAL") | Some("error") | Some("ERROR") => Severity::Critical,
        _ => default,
    }
}
