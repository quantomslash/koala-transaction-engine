use thiserror::Error;
#[derive(Error, Debug)]
pub enum KoalaError {
    #[error("invalid input received")]
    InputError,
    #[error("balance not enough for transaction")]
    BalanceError,
    #[error("partener information error")]
    PartnerError,
    #[error("account is frozen")]
    AccountLockedError,
    #[error("other error")]
    Other(#[from] csv::Error),
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("db error")]
    DB(#[from] rusqlite::Error)
}