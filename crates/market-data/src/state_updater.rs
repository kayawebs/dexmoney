use base_arb_common::types::PoolState;
use tracing::info;

pub fn log_pool_state_update(pool_state: &PoolState) {
    info!(
        pool = %pool_state.pool_id.address,
        block_number = pool_state.block_number,
        "pool state updated"
    );
}
