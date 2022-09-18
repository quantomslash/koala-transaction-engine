use crate::engine::{ClientRecord, Engine, Transaction};
use crate::errors::KoalaError;
use csv::{Reader, Writer};

/// The main struct for CSV processor, it keeps track
/// of trasactions, and the necessary files
pub struct CSVProcessor {
    txs: Vec<Transaction>,
    tmp_file: String,
    output_file: String,
}

impl CSVProcessor {
    /// Returns a new CSV processor
    pub fn new(
        tmp_file: String,
        output_file: String,
    ) -> Result<CSVProcessor, KoalaError> {
        Ok(CSVProcessor {
            txs: Vec::new(),
            tmp_file: tmp_file,
            output_file: output_file,
        })
    }
}

impl Engine for CSVProcessor {
    /// Deposit transaction
    fn deposit_tx(&mut self, tx: Transaction) -> Result<(), KoalaError> {
        self.print_tx_header("deposit");
        let client_id = tx.client;
        let mut client_record = self.get_client_record(client_id)?;

        if client_record.locked {
            return Err(KoalaError::AccountLockedError);
        }

        let amount = match tx.amount {
            Some(amt) => amt,
            None => 0.0,
        };

        self.txs.push(tx);

        if amount > 0.0 {
            client_record.available += amount;
            client_record.total = client_record.available + client_record.held;
            self.update_record(&client_record)?;
            return Ok(());
        }

        Err(KoalaError::PartnerError)
    }

    /// Withdrawal transaction
    fn withdrawal_tx(&mut self, tx: Transaction) -> Result<(), KoalaError> {
        self.print_tx_header("withdrawal");
        let client_id = tx.client;
        let mut client_record = self.get_client_record(client_id)?;

        if client_record.locked {
            return Err(KoalaError::AccountLockedError);
        }

        let amount = match tx.amount {
            Some(amt) => amt,
            None => 0.0,
        };

        self.txs.push(tx);

        if amount <= client_record.available {
            client_record.available -= amount;
            client_record.total = client_record.available + client_record.held;
            self.update_record(&client_record)?;
            return Ok(());
        }

        Err(KoalaError::BalanceError)
    }

    /// Dispute transaction
    fn dispute_tx(&self, tx: &Transaction) -> Result<(), KoalaError> {
        self.print_tx_header("dispute");
        let client_id = tx.client;
        let mut client_record = self.get_client_record(client_id)?;

        if client_record.locked {
            return Err(KoalaError::AccountLockedError);
        }

        if let Some(transaction) = self.find_original_transaction(&tx.tx) {
            let amount = transaction.amount.unwrap_or_default();
            client_record.available -= amount;
            client_record.held += amount;
            client_record.total = client_record.available + client_record.held;
            self.update_record(&client_record)?;
            return Ok(());
        }

        Err(KoalaError::PartnerError)
    }

    /// Resolve transaction
    fn resolve_tx(&self, tx: &Transaction) -> Result<(), KoalaError> {
        self.print_tx_header("resolve");
        let client_id = tx.client;
        let mut client_record = self.get_client_record(client_id)?;

        if client_record.locked {
            return Err(KoalaError::AccountLockedError);
        }

        if let Some(transaction) = self.find_original_transaction(&tx.tx) {
            let amount = transaction.amount.unwrap_or_default();
            client_record.held -= amount;
            client_record.available += amount;
            client_record.total = client_record.available + client_record.held;
            self.update_record(&client_record)?;
            return Ok(());
        }

        Err(KoalaError::PartnerError)
    }

    /// Chargeback transaction
    fn chargeback_tx(&self, tx: &Transaction) -> Result<(), KoalaError> {
        self.print_tx_header("chargeback");
        let client_id = tx.client;
        let mut client_record = self.get_client_record(client_id)?;

        if client_record.locked {
            return Err(KoalaError::AccountLockedError);
        }

        if let Some(transaction) = self.find_original_transaction(&tx.tx) {
            let amount = transaction.amount.unwrap_or_default();
            client_record.held -= amount;
            client_record.locked = true;
            client_record.total = client_record.available + client_record.held;
            self.update_record(&client_record)?;
            return Ok(());
        }

        Err(KoalaError::PartnerError)
    }

    /// Returns a transaction, given a transaction id
    fn find_original_transaction(&self, tx_id: &str) -> Option<&Transaction> {
        for transaction in &self.txs {
            if transaction.tx == tx_id {
                return Some(transaction);
            }
        }
        None
    }

    /// Returns a client record, given a client id
    fn get_client_record(
        &self,
        client_id: u16,
    ) -> Result<ClientRecord, KoalaError> {
        let mut rdr = Reader::from_path(self.output_file.as_str())?;

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

    /// Updates the client record, with the new record
    fn update_record(&self, cr: &ClientRecord) -> Result<(), KoalaError> {
        println!("Updating - {:?}", cr);

        let mut existing = false;
        let mut rdr = Reader::from_path(self.output_file.as_str())?;
        let mut wtr = Writer::from_path(self.tmp_file.as_str())?;

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

        std::fs::remove_file(self.output_file.as_str())?;
        std::fs::rename(self.tmp_file.as_str(), self.output_file.as_str())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    const TMP_FILE: &str = "data/tmp/tmp_test.csv";
    const TMP_OUT_FILE: &str = "data/tmp/tmp_out_test.csv";

    #[test]
    fn test_deposit_tx() {
        let (mut processor, tmp_file, tmp_out_file) = prep_test();
        let tx_type = String::from("deposit");
        let client_id = 1;
        let tx_id = String::from("1");
        let amount = 10.0;

        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.deposit_tx(tx).unwrap();

        // Verify the transaction result
        let cr = processor.get_client_record(client_id).unwrap();

        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        cleanup(tmp_file, tmp_out_file);
    }

    #[test]
    fn test_withdrawal_tx() {
        let (mut processor, tmp_file, tmp_out_file) = prep_test();

        // We must do a deposit first
        let tx_type = String::from("deposit");
        let client_id = 1;
        let tx_id = String::from("1");
        let amount = 10.0;
        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.deposit_tx(tx).unwrap();

        // Verify the transaction result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        // Withdrawal now
        let tx_type = String::from("withdrawal");
        let tx_id = String::from("2");
        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.withdrawal_tx(tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, 0.0);
        assert_eq!(cr.total, 0.0);
        assert_eq!(cr.held, 0.0);

        cleanup(tmp_file, tmp_out_file);
    }

    #[test]
    fn test_dispute_tx() {
        let (mut processor, tmp_file, tmp_out_file) = prep_test();

        // We must do a deposit first
        let tx_type = String::from("deposit");
        let client_id = 1;
        let tx_id = String::from("1");
        let amount = 10.0;
        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.deposit_tx(tx).unwrap();

        // Verify the transaction result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        // Dispute now
        let tx_type = String::from("dispute");
        let tx_id = String::from("1");
        let tx = Transaction::new(tx_type, client_id, tx_id, None);
        processor.dispute_tx(&tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, 0.0);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, amount);

        cleanup(tmp_file, tmp_out_file);
    }

    #[test]
    fn test_resolve_tx() {
        let (mut processor, tmp_file, tmp_out_file) = prep_test();

        // We must do a deposit first
        let tx_type = String::from("deposit");
        let client_id = 1;
        let tx_id = String::from("1");
        let amount = 10.0;
        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.deposit_tx(tx).unwrap();

        // Verify the transaction result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        // Dispute now
        let tx_type = String::from("dispute");
        let tx_id = String::from("1");
        let tx = Transaction::new(tx_type, client_id, tx_id, None);
        processor.dispute_tx(&tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, 0.0);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, amount);

        // Resolve now
        let tx_type = String::from("resolve");
        let tx_id = String::from("1");
        let tx = Transaction::new(tx_type, client_id, tx_id, None);
        processor.resolve_tx(&tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        cleanup(tmp_file, tmp_out_file);
    }

    #[test]
    fn test_chargeback_tx() {
        let (mut processor, tmp_file, tmp_out_file) = prep_test();

        // We must do a deposit first
        let tx_type = String::from("deposit");
        let client_id = 1;
        let tx_id = String::from("1");
        let amount = 10.0;
        let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
        processor.deposit_tx(tx).unwrap();

        // Verify the transaction result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, amount);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, 0.0);

        // Dispute now
        let tx_type = String::from("dispute");
        let tx_id = String::from("1");
        let tx = Transaction::new(tx_type, client_id, tx_id, None);
        processor.dispute_tx(&tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, 0.0);
        assert_eq!(cr.total, amount);
        assert_eq!(cr.held, amount);

        // Chargeback now
        let tx_type = String::from("chargeback");
        let tx_id = String::from("1");
        let tx = Transaction::new(tx_type, client_id, tx_id, None);
        processor.chargeback_tx(&tx).unwrap();

        // Verify the result
        let cr = processor.get_client_record(client_id).unwrap();
        assert_eq!(cr.available, 0.0);
        assert_eq!(cr.total, 0.0);
        assert_eq!(cr.held, 0.0);
        assert_eq!(cr.locked, true);

        cleanup(tmp_file, tmp_out_file);
    }

    fn prep_test() -> (self::CSVProcessor, String, String) {
        let mut rng = thread_rng();
        let rnum: u32 = rng.gen();

        let tmp_file = format!("{}_{}", TMP_FILE, rnum);
        let tmp_out_file = format!("{}_{}", TMP_OUT_FILE, rnum);

        std::fs::File::create(tmp_file.as_str()).unwrap();
        std::fs::File::create(tmp_out_file.as_str()).unwrap();

        (
            self::CSVProcessor::new(tmp_file.clone(), tmp_out_file.clone())
                .unwrap(),
            tmp_file,
            tmp_out_file,
        )
    }

    fn cleanup(file1: String, file2: String) {
        if std::path::Path::new(&file1).exists() {
            println!("Cleaning {}", file1);
            std::fs::remove_file(file1).unwrap();
        }

        if std::path::Path::new(&file2).exists() {
            println!("Cleaning {}", file2);
            std::fs::remove_file(file2).unwrap();
        }
    }
}
