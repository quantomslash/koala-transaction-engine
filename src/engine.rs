use crate::errors::KoalaError;
use serde::{Deserialize, Serialize};

/// Represents a transaction
#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: String,
    #[serde(rename = "client")]
    pub client: u16,
    pub tx: String,
    #[serde(deserialize_with = "csv::invalid_option")]
    pub amount: Option<f32>,
}

/// Represents individual client record
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRecord {
    #[serde(rename = "client")]
    pub id: u16,
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool,
}

impl Transaction {
    pub fn new(
        tx_type: String,
        client: u16,
        tx: String,
        amount: Option<f32>,
    ) -> Self {
        Transaction {
            tx_type,
            client,
            tx,
            amount,
        }
    }
}

/// Engine trait governs the main transaction related functionality
pub trait Engine {
    // Deposit transaction
    fn deposit_tx(&mut self, tx: Transaction) -> Result<(), KoalaError>;

    // Withdrawal transaction
    fn withdrawal_tx(&mut self, tx: Transaction) -> Result<(), KoalaError>;

    // Dispute transaction
    fn dispute_tx(&self, tx: &Transaction) -> Result<(), KoalaError>;

    // Resolve transaction
    fn resolve_tx(&self, tx: &Transaction) -> Result<(), KoalaError>;

    // Chargeback transaction
    fn chargeback_tx(&self, tx: &Transaction) -> Result<(), KoalaError>;

    fn find_original_transaction(&self, tx_id: &str) -> Option<&Transaction>;

    // Get client record
    fn get_client_record(
        &self,
        client_id: u16,
    ) -> Result<ClientRecord, KoalaError>;

    // Update client record
    fn update_record(&self, cr: &ClientRecord) -> Result<(), KoalaError>;

    // Print the transaction info for easy viewing
    fn print_tx_header(&self, tx_type: &str) {
        println!();
        println!("==================================");
        println!("Processing {} transaction", tx_type);
        println!("==================================");
    }    
}
