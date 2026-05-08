use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArbBotError {
    #[error("config error: {0}")]
    Config(String),
    #[error("quote error: {0}")]
    Quote(String),
    #[error("risk gate rejected: {0}")]
    RiskGate(String),
    #[error("simulation error: {0}")]
    Simulation(String),
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, ArbBotError>;
