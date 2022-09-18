pub mod cs;
pub mod db;
pub mod engine;
pub mod errors;

use csv::{ReaderBuilder, Trim};
use engine::{Engine, Transaction};
use errors::KoalaError;

/// Read the transaction data and call the
/// appropriate transaction function
pub fn process_txs(
    input_file: String,
    mut processor: impl Engine,
) -> Result<(), KoalaError> {
    let mut counter = 0;
    let mut rdr =
        ReaderBuilder::new().trim(Trim::All).from_path(input_file)?;

    for result in rdr.deserialize() {
        let record: Transaction = result?;
        let tx_type = record.tx_type.as_str();

        // Print some info
        let amount = match &record.amount {
            Some(amt) => amt,
            None => &0.0,
        };
        println!("Transaction - Amount: {} Id: {} Client: {}", amount, &record.tx, &record.client);

        match tx_type {
            "deposit" => processor.deposit_tx(record)?,
            "withdrawal" => processor.withdrawal_tx(record)?,
            "dispute" => processor.dispute_tx(&record)?,
            "resolve" => processor.resolve_tx(&record)?,
            "chargeback" => processor.chargeback_tx(&record)?,
            &_ => Err(KoalaError::InputError)?,
        };
        counter += 1;
        println!("Processed {} transactions", counter);
    }

    Ok(())
}
