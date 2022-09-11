mod errors;

use csv::{Reader, ReaderBuilder, Trim, Writer};
use errors::KoalaError;
use serde::{Deserialize, Serialize};
use std::fs::{remove_file, File};
use std::path::Path;

static CLIENT_FILE_NAME: &str = "client_records.csv";
static TEMP_FILE_NAME: &str = "tmp.csv";

#[derive(Debug, Serialize, Deserialize)]

pub struct ClientRecord {
    #[serde(rename = "client")]
    pub id: u16,
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool,
}

#[derive(Debug, Deserialize)]
struct Transaction {
    #[serde(rename = "type")]
    tx_type: String,
    #[serde(rename = "client")]
    client: u16,
    tx: String,
    #[serde(deserialize_with = "csv::invalid_option")]
    amount: Option<f32>,
}

pub fn process(filename: String) -> Result<String, KoalaError> {
    initialize_files()?;

    let mut txs: Vec<Transaction> = Vec::new();

    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(filename)?;

    for result in rdr.deserialize() {
        let record: Transaction = result?;
        let tx_type = record.tx_type.as_str();

        match tx_type {
            "deposit" => match deposit_tx(&record) {
                Err(_) => continue,
                Ok(_) => txs.push(record),
            },
            "withdrawal" => match withdrawal_tx(&record) {
                Err(_) => continue,
                Ok(_) => txs.push(record),
            },
            "dispute" => match dispute_tx(&record, &txs) {
                Err(_) => continue,
                Ok(_) => (),
            },
            "resolve" => match resolve_tx(&record, &txs) {
                Err(_) => continue,
                Ok(_) => (),
            },
            "chargeback" => match chargeback_tx(&record, &txs) {
                Err(_) => continue,
                Ok(_) => (),
            },
            &_ => Err(KoalaError::InputError)?,
        };
    }

    Ok(CLIENT_FILE_NAME.to_string())

}

fn initialize_files() -> Result<(), KoalaError> {
    if Path::new(CLIENT_FILE_NAME).exists() {
        remove_file(CLIENT_FILE_NAME)?;
    }
    File::create(CLIENT_FILE_NAME)?;

    Ok(())
}

fn get_client_record(client_id: u16) -> Result<ClientRecord, KoalaError> {
    let mut rdr = Reader::from_path(CLIENT_FILE_NAME)?;

    for result in rdr.deserialize() {
        let record: ClientRecord = result?;
        if record.id == client_id {
            return Ok(record);
        }
    }

    let client_record = ClientRecord {
        id: client_id,
        available: 0.0,
        held: 0.0,
        total: 0.0,
        locked: false,
    };

    Ok(client_record)
}

fn update_record(cr: &ClientRecord) -> Result<(), KoalaError> {
    let mut existing = false;
    let mut rdr = Reader::from_path(CLIENT_FILE_NAME)?;
    let mut wtr = Writer::from_path("tmp.csv")?;

    for result in rdr.deserialize() {
        let record: ClientRecord = result?;

        if record.id == cr.id {
            wtr.serialize(cr)?;
            existing = true;
        } else {
            wtr.serialize(record)?;
        }
    }

    if !existing {
        wtr.serialize(cr)?;
    }

    wtr.flush()?;
    drop(wtr);

    std::fs::remove_file(CLIENT_FILE_NAME)?;
    std::fs::rename(TEMP_FILE_NAME, CLIENT_FILE_NAME)?;

    Ok(())
}

fn find_original_transaction<'a>(tx_id: &str, txs: &'a Vec<Transaction>) -> Option<&'a Transaction> {
    for transaction in txs {
        if transaction.tx == tx_id {
            return Some(transaction);
        }
    }
    None
}

fn deposit_tx(tx: &Transaction) -> Result<String, KoalaError> {
    let client_id = tx.client;
    let mut client_record = get_client_record(client_id)?;

    if client_record.locked {
        return Err(KoalaError::AccountLockedError);
    }

    let amount = tx.amount.unwrap_or_default();

    if amount > 0.0 {
        client_record.available += amount;
        client_record.total = client_record.available + client_record.held;
        update_record(&client_record)?;
        return Ok(tx.tx.to_string());
    }

    Err(KoalaError::PartnerError)
}

fn withdrawal_tx(tx: &Transaction) -> Result<String, KoalaError> {
    let client_id = tx.client;
    let mut client_record = get_client_record(client_id)?;

    if client_record.locked {
        return Err(KoalaError::AccountLockedError);
    }

    let amount = tx.amount.unwrap_or_default();

    if amount <= client_record.available {
        client_record.available -= amount;
        client_record.total = client_record.available + client_record.held;
        update_record(&client_record)?;
        return Ok(tx.tx.to_string());
    }

    Err(KoalaError::BalanceError)
}

fn dispute_tx(tx: &Transaction, txs: &Vec<Transaction>) -> Result<String, KoalaError> {
    let client_id = tx.client;
    let mut client_record = get_client_record(client_id)?;

    if client_record.locked {
        return Err(KoalaError::AccountLockedError);
    }

    if let Some(transaction) = find_original_transaction(&tx.tx, txs) {
        let amount = transaction.amount.unwrap_or_default();
        client_record.available -= amount;
        client_record.held += amount;
        client_record.total = client_record.available + client_record.held;
        update_record(&client_record)?;
        return Ok(tx.tx.to_string());
    }

    Err(KoalaError::PartnerError)
}

fn resolve_tx(tx: &Transaction, txs: &Vec<Transaction>) -> Result<String, KoalaError> {
    let client_id = tx.client;
    let mut client_record = get_client_record(client_id)?;

    if client_record.locked {
        return Err(KoalaError::AccountLockedError);
    }

    if let Some(transaction) = find_original_transaction(&tx.tx, txs) {
        let amount = transaction.amount.unwrap_or_default();
        client_record.held -= amount;
        client_record.available += amount;
        client_record.total = client_record.available + client_record.held;
        update_record(&client_record)?;
        return Ok(tx.tx.to_string());
    }

    Err(KoalaError::PartnerError)
}

fn chargeback_tx(tx: &Transaction, txs: &Vec<Transaction>) -> Result<String, KoalaError> {
    let client_id = tx.client;
    let mut client_record = get_client_record(client_id)?;

    if client_record.locked {
        return Err(KoalaError::AccountLockedError);
    }

    if let Some(transaction) = find_original_transaction(&tx.tx, txs) {
        let amount = transaction.amount.unwrap_or_default();
        client_record.held -= amount;
        client_record.locked = true;
        client_record.total = client_record.available + client_record.held;
        update_record(&client_record)?;
        return Ok(tx.tx.to_string());
    }

    Err(KoalaError::PartnerError)
}

