use std::env;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use base_arb_common::config::Settings;
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, TickChangeStore, TickStateStore,
};
use sqlx::{Postgres, QueryBuilder, Row};

#[derive(Debug, Clone)]
struct Args {
    apply: bool,
    limit: i64,
    min_ticks: i64,
    variant: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    let selected = select_pools(&postgres, &args).await?;
    println!("== Sync pool_ticks_current to Redis ==");
    println!(
        "mode={} pools={} limit={} min_ticks={} variant={}",
        if args.apply { "apply" } else { "dry-run" },
        selected.len(),
        args.limit,
        args.min_ticks,
        args.variant.as_deref().unwrap_or("all")
    );

    let ticks_by_pool = postgres.get_pool_ticks_current_many(&selected).await?;
    let mut synced = 0usize;
    let mut skipped_empty = 0usize;
    let mut ticks_written = 0usize;

    for pool in selected {
        let ticks = ticks_by_pool.get(&pool).cloned().unwrap_or_default();
        if ticks.is_empty() {
            skipped_empty += 1;
            continue;
        }
        println!("pool={pool:#x} ticks={}", ticks.len());
        ticks_written += ticks.len();
        if args.apply {
            redis.replace_pool_ticks(pool, ticks).await?;
            redis.mark_tick_changed_pools(vec![pool]).await?;
        }
        synced += 1;
    }

    println!("synced={synced} skipped_empty={skipped_empty} ticks_written={ticks_written}");
    if !args.apply {
        println!("dry-run only. rerun with --apply to write Redis.");
    }
    Ok(())
}

async fn select_pools(store: &PostgresStore, args: &Args) -> Result<Vec<Address>> {
    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        SELECT pt.pool_address, count(*) AS tick_count
        FROM pool_ticks_current pt
        LEFT JOIN pools p
          ON p.chain_id = pt.chain_id
         AND lower(p.pool_address) = lower(pt.pool_address)
        "#,
    );

    if args.variant.is_some() {
        query.push(" WHERE p.variant = ");
        query.push_bind(args.variant.as_deref());
    }

    query.push(" GROUP BY pt.pool_address HAVING count(*) >= ");
    query.push_bind(args.min_ticks);
    query.push(" ORDER BY count(*) DESC, lower(pt.pool_address)");
    if args.limit > 0 {
        query.push(" LIMIT ");
        query.push_bind(args.limit);
    }

    let rows = query.build().fetch_all(&store.pool).await?;
    rows.into_iter()
        .map(|row| {
            let pool: String = row.try_get("pool_address")?;
            pool.parse::<Address>()
                .with_context(|| format!("invalid pool_address {pool}"))
        })
        .collect()
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        apply: false,
        limit: 1_000,
        min_ticks: 1,
        variant: None,
    };
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--apply" => args.apply = true,
            "--limit" => {
                let value = iter.next().context("--limit requires a value")?;
                args.limit = value.parse().context("invalid --limit")?;
            }
            "--min-ticks" => {
                let value = iter.next().context("--min-ticks requires a value")?;
                args.min_ticks = value.parse().context("invalid --min-ticks")?;
            }
            "--variant" => {
                args.variant = Some(iter.next().context("--variant requires a value")?);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    if args.limit < 0 {
        anyhow::bail!("--limit must be >= 0");
    }
    if args.min_ticks < 1 {
        anyhow::bail!("--min-ticks must be >= 1");
    }
    Ok(args)
}

fn print_usage() {
    println!(
        "Usage: sync_ticks_current_to_redis [--variant UniswapV4] [--limit 1000] [--min-ticks 1] [--apply]\n\nUse --limit 0 for all matching pools. Without --apply this is a dry-run."
    );
}
