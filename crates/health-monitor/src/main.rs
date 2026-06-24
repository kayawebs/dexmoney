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
