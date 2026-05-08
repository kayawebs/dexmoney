use alloy_primitives::{Address, B256, U256};

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
                eth_balance: U256::ZERO,
                status: EoaLaneStatus::Idle,
            },
        }
    }

    pub fn mark_submitted(&mut self, tx_hash: B256) {
        self.state.pending_tx = Some(tx_hash);
        self.state.local_nonce += 1;
        self.state.status = EoaLaneStatus::Pending;
    }

    pub fn mark_confirmed(&mut self, confirmed_nonce: u64) {
        self.state.confirmed_nonce = confirmed_nonce;
        self.state.pending_tx = None;
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

        lane.mark_submitted(b256!(
            "0101010101010101010101010101010101010101010101010101010101010101"
        ));
        assert_eq!(lane.state.status, EoaLaneStatus::Pending);
        assert_eq!(lane.state.local_nonce, 1);

        lane.mark_confirmed(1);
        assert_eq!(lane.state.status, EoaLaneStatus::Idle);
        assert_eq!(lane.state.confirmed_nonce, 1);
        assert!(lane.state.pending_tx.is_none());

        lane.mark_blocked();
        assert_eq!(lane.state.status, EoaLaneStatus::Blocked);

        lane.mark_cooldown();
        assert_eq!(lane.state.status, EoaLaneStatus::Cooldown);
    }
}
