use alloy_primitives::{Address, B256, U256};
use uuid::Uuid;

use base_arb_common::types::{EoaLaneState, EoaLaneStatus};

#[derive(Debug, Clone)]
pub struct EoaLane {
    pub state: EoaLaneState,
}

impl EoaLane {
    pub fn new(address: Address) -> Self {
        Self {
            state: EoaLaneState {
                address,
                local_nonce: 0,
                confirmed_nonce: 0,
                pending_tx: None,
                pending_opportunity_id: None,
                pending_simulation_id: None,
                pending_nonce: None,
                pending_submitted_block: None,
                pending_replacement_count: 0,
                pending_gas_limit: None,
                pending_max_fee_per_gas: None,
                pending_max_priority_fee_per_gas: None,
                eth_balance: U256::ZERO,
                status: EoaLaneStatus::Idle,
            },
        }
    }

    pub fn mark_submitted(
        &mut self,
        opportunity_id: Uuid,
        simulation_id: Option<Uuid>,
        tx_hash: B256,
        nonce: u64,
        submitted_block: u64,
        gas_limit: U256,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    ) {
        self.state.pending_tx = Some(tx_hash);
        self.state.pending_opportunity_id = Some(opportunity_id);
        self.state.pending_simulation_id = simulation_id;
        self.state.pending_nonce = Some(nonce);
        self.state.pending_submitted_block = Some(submitted_block);
        self.state.pending_replacement_count = 0;
        self.state.pending_gas_limit = Some(gas_limit);
        self.state.pending_max_fee_per_gas = Some(max_fee_per_gas);
        self.state.pending_max_priority_fee_per_gas = Some(max_priority_fee_per_gas);
        self.state.local_nonce = nonce.saturating_add(1);
        self.state.status = EoaLaneStatus::Pending;
    }

    pub fn mark_replaced(
        &mut self,
        tx_hash: B256,
        submitted_block: u64,
        gas_limit: U256,
        max_fee_per_gas: U256,
        max_priority_fee_per_gas: U256,
    ) {
        self.state.pending_tx = Some(tx_hash);
        self.state.pending_submitted_block = Some(submitted_block);
        self.state.pending_replacement_count =
            self.state.pending_replacement_count.saturating_add(1);
        self.state.pending_gas_limit = Some(gas_limit);
        self.state.pending_max_fee_per_gas = Some(max_fee_per_gas);
        self.state.pending_max_priority_fee_per_gas = Some(max_priority_fee_per_gas);
        self.state.status = EoaLaneStatus::Pending;
    }

    pub fn mark_confirmed(&mut self, confirmed_nonce: u64) {
        self.state.confirmed_nonce = confirmed_nonce;
        self.state.pending_tx = None;
        self.state.pending_opportunity_id = None;
        self.state.pending_simulation_id = None;
        self.state.pending_nonce = None;
        self.state.pending_submitted_block = None;
        self.state.pending_replacement_count = 0;
        self.state.pending_gas_limit = None;
        self.state.pending_max_fee_per_gas = None;
        self.state.pending_max_priority_fee_per_gas = None;
        self.state.status = EoaLaneStatus::Idle;
    }

    pub fn clear_consumed_pending(
        &mut self,
        confirmed_nonce: u64,
        local_nonce: u64,
        eth_balance: U256,
    ) {
        self.mark_confirmed(confirmed_nonce);
        self.state.local_nonce = local_nonce;
        self.state.eth_balance = eth_balance;
    }

    pub fn mark_blocked(&mut self) {
        self.state.status = EoaLaneStatus::Blocked;
    }

    pub fn mark_cooldown(&mut self) {
        self.state.status = EoaLaneStatus::Cooldown;
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, b256};

    use base_arb_common::types::EoaLaneStatus;

    use super::EoaLane;

    #[test]
    fn lane_state_machine_transitions() {
        let address = address!("4200000000000000000000000000000000000006");
        let mut lane = EoaLane::new(address);

        assert_eq!(lane.state.status, EoaLaneStatus::Idle);

        lane.mark_submitted(
            uuid::Uuid::new_v4(),
            Some(uuid::Uuid::new_v4()),
            b256!("0101010101010101010101010101010101010101010101010101010101010101"),
            7,
            100,
            alloy_primitives::U256::from(350_000u64),
            alloy_primitives::U256::from(10u64),
            alloy_primitives::U256::from(2u64),
        );
        assert_eq!(lane.state.status, EoaLaneStatus::Pending);
        assert_eq!(lane.state.local_nonce, 8);
        assert_eq!(lane.state.pending_nonce, Some(7));
        assert_eq!(lane.state.pending_submitted_block, Some(100));
        assert_eq!(lane.state.pending_replacement_count, 0);

        lane.mark_confirmed(1);
        assert_eq!(lane.state.status, EoaLaneStatus::Idle);
        assert_eq!(lane.state.confirmed_nonce, 1);
        assert!(lane.state.pending_tx.is_none());
        assert!(lane.state.pending_opportunity_id.is_none());
        assert!(lane.state.pending_simulation_id.is_none());
        assert!(lane.state.pending_nonce.is_none());
        assert!(lane.state.pending_submitted_block.is_none());

        lane.mark_blocked();
        assert_eq!(lane.state.status, EoaLaneStatus::Blocked);

        lane.mark_cooldown();
        assert_eq!(lane.state.status, EoaLaneStatus::Cooldown);
    }

    #[test]
    fn clear_consumed_pending_syncs_lane_to_chain_nonce() {
        let address = address!("4200000000000000000000000000000000000006");
        let mut lane = EoaLane::new(address);

        lane.mark_submitted(
            uuid::Uuid::new_v4(),
            Some(uuid::Uuid::new_v4()),
            b256!("0202020202020202020202020202020202020202020202020202020202020202"),
            189,
            46575829,
            alloy_primitives::U256::from(475_650u64),
            alloy_primitives::U256::from(135_992_647u64),
            alloy_primitives::U256::from(1_440_001u64),
        );

        lane.clear_consumed_pending(194, 194, alloy_primitives::U256::from(1_000_000_000u64));

        assert_eq!(lane.state.status, EoaLaneStatus::Idle);
        assert_eq!(lane.state.confirmed_nonce, 194);
        assert_eq!(lane.state.local_nonce, 194);
        assert_eq!(
            lane.state.eth_balance,
            alloy_primitives::U256::from(1_000_000_000u64)
        );
        assert!(lane.state.pending_tx.is_none());
        assert!(lane.state.pending_opportunity_id.is_none());
        assert!(lane.state.pending_simulation_id.is_none());
        assert!(lane.state.pending_nonce.is_none());
        assert!(lane.state.pending_submitted_block.is_none());
        assert_eq!(lane.state.pending_replacement_count, 0);
    }
}
