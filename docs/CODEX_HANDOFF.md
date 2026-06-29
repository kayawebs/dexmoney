# Codex Handoff

这份文档给在服务器或本地新启动的 Codex 使用。目标是让接手者不依赖聊天记录，也能理解 Dexmoney 的系统边界、当前阶段、排查工作流和下一步优先级。

## 必读顺序

1. `docs/OPERATING_PRIORITIES.md`。这里定义最高优先级：性能、真实套利结果、避免热路径 RPC。
2. `docs/ARCHITECTURE.md`。这里定义 market-data、pool-discovery、searcher、execution-manager、Redis/Postgres 边界。
3. `docs/DEBUG_WORKFLOW.md`。非平凡生产问题必须按这个流程留痕：symptom、hypotheses、evidence、root cause、smallest fix、verification、regression guard。
4. `docs/COMPETITOR_DRIVEN_LOOP.md`。机会少时优先用大哥报告驱动排查，不要先假设市场没机会。
5. `docs/TODO.md`。当前 P0/P1 排序。
6. `docs/INCIDENTS.md`。历史问题和已验证修复。

## 项目目标

Dexmoney 是 Base 链套利机器人。核心目标不是工程完备性，而是低延迟、数据准确、真实套利结果。

原则：

- `market-data` 必须快速、准确处理链上 state-changing events。
- `searcher` 热路径不得调用 RPC。
- `execution-manager` 只处理 fresh candidate，过期 candidate 是诊断数据，不是执行机会。
- Redis 是热缓存，不是全量历史库。
- Postgres 是持久状态、覆盖率、诊断和回放数据源。
- 不要用 fallback 掩盖坏数据或坏模型；要暴露并修掉根因。
- 不要把 gas/USD 价格计算塞进模拟热路径；盈利由配置的 token min_profit 控制。

## 运行环境

本地开发目录：

```bash
/Users/peter/claude/dexmoney
```

云端生产目录：

```bash
/home/ubuntu/dexmoney
```

远端 SSH target：

```bash
ssh base
```

远端常用环境：

```bash
cd /home/ubuntu/dexmoney
set -a
source .env
source .env.docker
set +a
source ~/.cargo/env
export PATH="$HOME/.foundry/bin:$HOME/.cargo/bin:$PATH"
```

Docker compose：

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml ps
```

重启：

```bash
./restart.sh market-data
./restart.sh searcher
./restart.sh execution-manager
./restart.sh all
```

注意：`restart.sh all` 可能包含 execution-manager。正式资金运行前必须确认 `.env.docker` 里的 submit/auto-approve 配置。

## 当前系统分层

### market-data

职责：

- 读取 sealed block / flashblock / logs。
- 应用 pool state change。
- 写 Redis 当前 hot state。
- 写 Postgres 事件、pool_states、warnings、coverage。
- 发布 `pools:changed` / `ticks:changed`。

不能做：

- 历史全量 backfill。
- 重型 discovery。
- searcher 的机会判断。

关键日志：

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since 10m market-data \
  | grep "market-data sealed block summary"
```

健康判断：

- `catchup_lag_blocks=0` 或很小。
- `total_ms` 正常低于一个 block 时间。
- `changed_pools` 非零说明正在发布状态。
- `fee_refresh_failed_pools` 可以非零，但不能导致状态不发布。

### pool-discovery

职责：

- 发现新池。
- 写 `observed_pools` / `protocol_pool_observations`。
- 分类、promote quoteable/executable pools。
- 新 quoteable hot pool 应该写 Postgres，并在必要时发布到 Redis。

### searcher

职责：

- 从 Redis 读取 hot state/ticks。
- 从 Postgres 读取配置。
- 不调用 RPC。
- 用 changed pools 约束路径搜索。
- 生成 next-block candidates 和 opportunities。

关键日志：

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since 10m searcher \
  | grep "searcher cycle summary"
```

重点字段：

- `latest_chain_block` / `latest_pool_state_block`
- `changed_pools`
- `path_build_ms`
- `quote_successes`
- `quote_skipped_tick_range_exhausted`
- `price_impact_rejected`
- `price_impact_shadow_pass_*`
- `min_profit_rejected`
- `opportunities_created`

### execution-manager

职责：

- 从 Redis candidate queue 消费 fresh candidates。
- 模拟。
- 必要时自动 approve。
- 按配置提交交易。
- 记录 simulations / transactions。

关键日志：

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml --profile executor logs --since 10m execution-manager \
  | grep -E "candidate queue drain summary|execution candidate batch summary|simulation success/fail|tx submitted|revert"
```

## 诊断入口

机会/模拟/链上提交失败，优先用 doctor：

```bash
doctor/arb_doctor.sh --opportunity-id <uuid>
doctor/arb_doctor.sh --simulation-id <uuid>
doctor/arb_doctor.sh --tx-hash <0x...>
```

底层二进制：

```bash
cargo run -p base-arb-recorder --bin arb_doctor -- \
  --simulation-id <uuid> \
  --out reports/doctor-<name>.txt
```

当前 doctor 已支持：

- opportunity/simulation/path JSON 读取。
- V2/classic recorded quote vs onchain state 对比。
- source block / opportunity block 状态差异。
- Redis current state / tick count。
- Postgres tick count。
- V2/classic 本地公式和池子 `getAmountOut` 对比。

当前 doctor verdict 重点：

- `classic_state_drift`
- `classic_state_stale_by_opportunity_block`
- `classic_formula_mismatch`
- `classic_pool_formula_mismatch`
- `classic_k_not_state_or_formula`
- `v3_state_or_tick_needs_review`
- `unsupported_deep_check`

后续扩展方向：

- V3-style historical tick replay。
- Uniswap V4 PoolManager state/tick deep analyzer。
- Balancer V3 vault/router quote analyzer。
- submitted tx block-order / same-block race analyzer。

## 当前已修复的问题

### Aerodrome Classic fee refresh 失败导致 reserve 更新被丢弃

相关 commit：

```text
030731c Publish state when fee refresh fails
aa3b111 Classify stale opportunity state in arb doctor
2a00600 Add arb doctor state diff diagnostics
```

症状：

- `UniswapV2: K` 和 `MinProfitNotMet` 集中出现在带 Aerodrome classic 的路径。
- doctor 发现 recorded quote 在 source block 正确，但 opportunity block 的同一个 pool reserve 已经变化。
- 链上同 block 有 `Sync` / `Swap`。
- market-data 旧日志出现 `Classic state update withheld because factory fee refresh failed`。

根因：

- market-data 已经从 event 里拿到新的 reserve。
- 后续 Aerodrome fee refresh 失败。
- 旧逻辑把该 pool 从 `changed_pools` 和 `validation_snapshots` 移除。
- searcher 没收到这个 changed pool 的新 reserve，于是用旧 reserve 计算机会。

修复：

- fee refresh 失败时，不再丢弃 state update。
- 保留上一次 fee，继续发布新的 reserve/state。
- 日志改为：

```text
Classic fee refresh failed; retaining previous fee and publishing updated reserves
Slipstream dynamic fee refresh failed; retaining previous fee and publishing updated state
```

sealed block summary 增加：

```text
fee_refresh_failed_pools=<n>
```

验证命令：

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since 10m market-data \
  | grep -E "Classic fee refresh failed; retaining previous fee|state update withheld|fee_refresh_failed_pools|market-data sealed block summary"
```

期待：

- 新日志里不再出现旧的 `state update withheld`。
- 即使 `fee_refresh_failed_pools > 0`，`changed_pools` 仍然发布。
- 后续新 simulation 中，同类 `UniswapV2: K` 应明显下降；如果仍有新样本，用 doctor 继续拆。

## fee refresh 是什么，为什么会失败

Aerodrome Classic / Slipstream 的 fee 不是所有协议里都固定写死在 pool metadata 里的。

### 什么时候刷新

不是每个 pool 每个 block 刷新。

当前 sealed-block 逻辑是：

- 先处理当前 block range 的 pool events。
- 对本轮发生相关事件的 Aerodrome Classic / Slipstream pools 生成 `fee_refresh_jobs`。
- Classic 通过 factory `getFee(address,bool)` 查询当时 block hash 的 fee。
- Slipstream 通过对应 dynamic fee 查询。
- 同一批 block 内按 `FEE_REFRESH_CONCURRENCY` 并发执行。
- 成功则更新 fee 并额外发布 `fee_refresh`。
- 失败则保留旧 fee，但现在仍发布已经应用的新 reserve/state。

代码入口：

- `crates/market-data/src/listener.rs` sealed block fee refresh loop。
- `crates/chain/src/provider.rs` 的 `fetch_aerodrome_classic_fee_bps_at_block_hash`。

另外存在 `refresh_aerodrome_fees()` 这种批量 drift 校准函数，但当前标注 `#[allow(dead_code)]`，不是主热路径。

### 为什么失败

常见原因：

- factory 不支持我们调用的 `getFee(address,bool)` 签名。
- pool 的 `factory_address` 是 fork/非标准 factory。
- pool 本身不支持 `fee()` fallback，或者 selector 撞到其他函数，返回巨大 U256，日志里会出现 `classic pool fee too large`。
- 历史 block hash eth_call 在本地节点或目标合约上失败。
- metadata 里 stable/factory 分类错误。
- RPC 节点短暂异常。

关键判断：

- fee refresh 失败不等于 reserve/state 错。
- reserve/state 是由 `Sync`/`Swap` 等事件给出的，应该优先发布。
- fee 失败只说明本次 fee 可能沿用旧值；如果某类 factory 长期失败，应修 factory/fee classification，而不是阻断 state。

后续改进建议：

- 对长期失败的 factory 建立 `fee_model_coverage` 或 registry 标记，避免热路径重复打无效 RPC。
- 对稳定已知 fee 的 classic fork 使用 registry fee。
- 对真正 dynamic fee 的协议，单独实现正确查询方式。
- 把 fee refresh failure 的 factory/pool top bucket 加进 health/report。

## 当前阶段

当前阶段不是“系统完全好了”，而是：

- market-data 的一个明确 stale reserve 根因已修复并部署。
- 需要观察修复后新窗口，确认 `UniswapV2: K` 是否消失或下降。
- searcher 当前仍有机会偏少问题，主要拒绝桶包括：
  - `min_profit_rejected`
  - `price_impact_rejected`
  - `TickRangeExhausted`
  - V4/Balancer 深度模型不足
  - competitor covered path but no local opportunity

最新优先级仍以 `docs/TODO.md` 为准。

## 推荐接手流程

### 1. 先看 runtime 是否健康

```bash
cd /home/ubuntu/dexmoney
set -a
source .env
source .env.docker
set +a

sudo docker compose --env-file .env.docker -f docker-compose.apps.yml ps
redis-cli -u "$REDIS_URL" GET chain:current_block
redis-cli -u "$REDIS_URL" SCARD pools:changed
redis-cli -u "$REDIS_URL" SCARD ticks:changed
```

### 2. 看 market-data 是否跟上

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since 10m market-data \
  | grep "market-data sealed block summary" \
  | tail -n 20
```

判断：

- `catchup_lag_blocks=0` 最好。
- `total_ms` 不应持续大于一个 block。
- `changed_pools` 正常非零。

### 3. 看 searcher 是否产机会

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since 10m searcher \
  | grep "searcher cycle summary" \
  | tail -n 5
```

判断：

- `latest_chain_block == latest_pool_state_block`。
- `opportunities_created` 是否长期为 0。
- 如果长期为 0，优先看 `min_profit_rejected`、`price_impact_rejected`、`quote_skipped_tick_range_exhausted`。

### 4. 看 simulation / transaction 分布

```bash
psql "$POSTGRES_URL" -X -q <<'SQL'
SELECT 'opportunities_10m' AS name, count(*) AS n, max(created_at) AS latest
FROM opportunities
WHERE created_at >= now() - interval '10 minutes'
UNION ALL
SELECT 'simulations_10m', count(*), max(created_at)
FROM simulations
WHERE created_at >= now() - interval '10 minutes'
UNION ALL
SELECT 'transactions_10m', count(*), max(created_at)
FROM transactions
WHERE created_at >= now() - interval '10 minutes';

SELECT COALESCE(revert_reason, 'success') AS reason, count(*) AS n, max(created_at) AS latest
FROM simulations
WHERE created_at >= now() - interval '10 minutes'
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
SQL
```

### 5. 如果有失败样本，先跑 doctor

```bash
doctor/arb_doctor.sh --simulation-id <uuid>
```

不要直接改代码。先写清：

- 症状窗口。
- 代表性 path/pool/simulation。
- doctor verdict。
- 可证伪假设。
- 最小修复。
- 验证指标。

## 远端同步习惯

本地改代码后：

```bash
cargo check -p <crate>
cargo test -p <crate>
git diff --check
git status --short
git add <files>
git commit -m "<message>"
git push
```

云端：

```bash
ssh base
cd /home/ubuntu/dexmoney
git pull --ff-only
./restart.sh <service>
```

不要在云端直接编辑未提交代码，除非是紧急诊断脚本；紧急脚本也要后续回收到 repo。

## 当前常见坑

- `searcher` 没机会不等于市场没机会；要对比 target competitor。
- `latest pool state block` 不能简单理解为“池子旧就是错”。如果池子没有事件，旧 block 的 state 可以是准确的；只有监听缺失、事件处理失败、或 same-block 更新没发布才是问题。
- tick 缺失要区分 Redis hot cache 缺失和 Postgres durable coverage 缺失。
- V4/Balancer 不是只看 discovery；还要 metadata/state/ticks/model/execution 全链路 ready。
- `MinProfitNotMet` 不是一个根因，它是结果桶。可能来自 stale state、tick 不全、公式错误、same-block 抢跑、资金点不连续、impact 模型、协议适配缺失。
- `router/no-revert-data` 要区分模拟没 decode、adapter calldata 错、真实协议 reject。
- 大哥用了某个池，不代表我们可以自动 trust；competitor usage 只能提高排查优先级。

## 下一步建议

1. 观察 `030731c` 部署后的新窗口，确认新 `UniswapV2: K` 是否下降。
2. 如果 `UniswapV2: K` 仍出现，用 `arb_doctor` 对修复后的新 sample 分类，不要复用旧 sample。
3. 如果 K 下降，转向最高产出的失败桶：
   - `MinProfitNotMet`：用 doctor 判断 V2/classic 是否还有 stale state；V3/V4/Balancer 则补深度 analyzer。
   - `TickRangeExhausted`：检查 hot tick repair / durable tick coverage。
   - `price_impact_rejected`：结合 shadow pass 数据决定是否调整模型或阈值，不要盲目放宽。
4. 每次有代码修复，都更新 `docs/INCIDENTS.md` 和必要的诊断脚本。
