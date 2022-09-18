extern crate rusqlite;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::engine::{ClientRecord, Engine, Transaction};
use crate::errors::KoalaError;

const TABLE_NAME: &str = "CLIENT_RECORDS";

/// The main struct for DB processor, it keeps track
/// of trasactions and the DB connection
pub struct DBProcessor {
    txs: Vec<Transaction>,
    connection: Connection
}

impl DBProcessor {
    /// Returns a new DB Processor
    pub fn new(
        db_file: String,
    ) -> Result<DBProcessor, KoalaError> {
        let connection = Connection::open_with_flags(
            &db_file,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        let query = format!("CREATE TABLE {} ( id INTEGER PRIMARY KEY, available REAL, held REAL, total REAL, locked BOOL )", TABLE_NAME);
        connection.execute(&query, ())?;

        Ok(DBProcessor {
            txs: Vec::new(),
            connection
        })
    }

    /// Given the record, creates a new record in the db
    fn create_new_record(&self, cr: &ClientRecord) -> Result<(), KoalaError> {
        println!("Creating new record: {:?}", cr);
        let query = 
        "INSERT INTO CLIENT_RECORDS ( id, available, held, total, locked ) VALUES (?1, ?2, ?3, ?4, ?5)";
        self.connection.execute(query, params![cr.id, cr.available, cr.held, cr.total, cr.locked])?;
        Ok(())
    }

    /// Update an existing record, give the new record
    fn update_existing_record(&self, cr: &ClientRecord) -> Result<(), KoalaError> {
        let query =
            "UPDATE CLIENT_RECORDS SET available=?1, held=?2, total=?3, locked=?4 where id=?5";
        
        if let Ok(_) = self.connection.execute(query, params![cr.available, cr.held, cr.total, cr.locked, cr.id]) {
            return Ok(());
        } 
        Err(KoalaError::PartnerError)
    }
    
    /// Checks if the client exists, given an client id
    fn client_exists(&self, id: u64) -> bool {
        let query =
        format!("SELECT 1 FROM {} WHERE id={}", TABLE_NAME, id);
        let record: u16 = self.connection.query_row(&query, [], |row| row.get(0)).unwrap_or_default();
        record == 1
    }
}

impl Engine for DBProcessor {
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
            None => 0.0
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
            None => 0.0
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
                return Some(&transaction);
            }
        }
        None
    }

    /// Returns a client record, given a client id
    fn get_client_record(
        &self,
        client_id: u16,
    ) -> Result<ClientRecord, KoalaError> {

        let query =
            format!("SELECT id, available, held, total, locked FROM {} WHERE ID={}", TABLE_NAME, client_id);

        let record = self
            .connection
            .query_row(&query, [], |row| {
                let id = row.get(0)?;
                let available = row.get(1)?;
                let held = row.get(2)?;
                let total = row.get(3)?;
                let locked = row.get(4)?;

                Ok(ClientRecord {
                    id,
                    available,
                    held,
                    total,
                    locked,
                })
            })
            .optional()?;

        if let Some(rec) = record {
            return Ok(rec);
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

        let existing_client = self.client_exists(cr.id.into());        
        match existing_client {
            true => {
                self.update_existing_record(cr)?;
                return Ok(());
            },
            false => {
                self.create_new_record(cr)?;
                return Ok(());
            }
        }        
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    const TMP_DB: &str = "data/tmp/tmp_db.sqlite";   

    
    #[test]
    fn test_deposit_tx() {
        let (mut processor, db_file) = prep_test();
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

        cleanup(db_file);
    }

    #[test]
    fn test_withdrawal_tx() {
        let (mut processor, db_file) = prep_test();

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

        cleanup(db_file);
    }

    #[test]
    fn test_dispute_tx() {
        let (mut processor, db_file) = prep_test();

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

        cleanup(db_file);
    }

    #[test]
    fn test_resolve_tx() {
        let (mut processor, db_file) = prep_test();

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

        cleanup(db_file);
    }

    #[test]
    fn test_chargeback_tx() {
        let (mut processor, db_file) = prep_test();

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

        cleanup(db_file);
    }    

    
    fn prep_test() -> (self::DBProcessor, String) {
        let mut rng = thread_rng();
        let rnum: u32 = rng.gen();        
        let db_file = format!("{}_{}", TMP_DB, rnum);
        // Create the temporary db for the test
        std::fs::File::create(db_file.as_str()).unwrap();

        let processor = self::DBProcessor::new(db_file.clone()).unwrap();
        (
            processor,
            db_file
        )        
    }

    fn cleanup(db_file: String) {
        if std::path::Path::new(db_file.as_str()).exists() {
            println!("Cleaning {}", db_file);
            std::fs::remove_file(db_file).unwrap();
        }
    }
}
