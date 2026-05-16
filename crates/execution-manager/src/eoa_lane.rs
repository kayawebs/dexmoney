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
                eth_balance: U256::ZERO,
                status: EoaLaneStatus::Idle,
            },
        }
    }

    pub fn mark_submitted(
        &mut self,
        opportunity_id: Uuid,
        simulation_id: Uuid,
        tx_hash: B256,
        nonce: u64,
    ) {
        self.state.pending_tx = Some(tx_hash);
        self.state.pending_opportunity_id = Some(opportunity_id);
        self.state.pending_simulation_id = Some(simulation_id);
        self.state.pending_nonce = Some(nonce);
        self.state.local_nonce = nonce.saturating_add(1);
        self.state.status = EoaLaneStatus::Pending;
    }

    pub fn mark_confirmed(&mut self, confirmed_nonce: u64) {
        self.state.confirmed_nonce = confirmed_nonce;
        self.state.pending_tx = None;
        self.state.pending_opportunity_id = None;
        self.state.pending_simulation_id = None;
        self.state.pending_nonce = None;
        self.state.status = EoaLaneStatus::Idle;
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
            uuid::Uuid::new_v4(),
            b256!("0101010101010101010101010101010101010101010101010101010101010101"),
            7,
        );
        assert_eq!(lane.state.status, EoaLaneStatus::Pending);
        assert_eq!(lane.state.local_nonce, 8);
        assert_eq!(lane.state.pending_nonce, Some(7));

        lane.mark_confirmed(1);
        assert_eq!(lane.state.status, EoaLaneStatus::Idle);
        assert_eq!(lane.state.confirmed_nonce, 1);
        assert!(lane.state.pending_tx.is_none());
        assert!(lane.state.pending_opportunity_id.is_none());
        assert!(lane.state.pending_simulation_id.is_none());
        assert!(lane.state.pending_nonce.is_none());

        lane.mark_blocked();
        assert_eq!(lane.state.status, EoaLaneStatus::Blocked);

        lane.mark_cooldown();
        assert_eq!(lane.state.status, EoaLaneStatus::Cooldown);
    }
}
