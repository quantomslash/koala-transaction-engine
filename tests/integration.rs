use koala_transaction_engine::cs::CSVProcessor;
use koala_transaction_engine::db::{DBProcessor};
use koala_transaction_engine::engine::{Engine, Transaction};
use rand::{thread_rng, Rng};

const TMP_CSV_FILE: &str = "data/tmp/tmp_test.csv";
const TMP_CSV_OUT_FILE: &str = "data/tmp/tmp_out_test.csv";
const TMP_DB: &str = "data/tmp/tmp_db.sqlite";

#[test]
fn test_csv_method() {
    // Prepare the necessary files
    let mut rng = thread_rng();
    let rnum: u32 = rng.gen();
    let tmp_file = format!("{}_{}", TMP_CSV_FILE, rnum);
    let tmp_out_file = format!("{}_{}", TMP_CSV_OUT_FILE, rnum);

    std::fs::File::create(tmp_file.as_str()).unwrap();
    std::fs::File::create(tmp_out_file.as_str()).unwrap();

    let mut processor: CSVProcessor =
        CSVProcessor::new(tmp_file.clone(), tmp_out_file.clone()).unwrap();

    // Run test
    transact_and_verify(&mut processor);

    // Cleanup
    println!("Cleaning {}", tmp_file);
    if std::path::Path::new(&tmp_file).exists() {
        println!("Cleaning {}", tmp_file);
        std::fs::remove_file(tmp_file).unwrap();
    }

    if std::path::Path::new(&tmp_out_file).exists() {
        println!("Cleaning {}", tmp_out_file);
        std::fs::remove_file(tmp_out_file).unwrap();
    }
}

#[test]
fn test_db_method() {
    // Prepare the db
    let mut rng = thread_rng();
    let rnum: u32 = rng.gen();
    let db_file = format!("{}_{}", TMP_DB, rnum);

    std::fs::File::create(db_file.as_str()).unwrap();

    let mut processor = DBProcessor::new(db_file.clone()).unwrap();

    // Run test
    transact_and_verify(&mut processor);

    // Cleanup
    if std::path::Path::new(db_file.as_str()).exists() {
        println!("Cleaning {}", db_file);
        std::fs::remove_file(db_file).unwrap();
    }
}

fn transact_and_verify(processor: &mut impl Engine) {
    // Perform transactions

    // Test deposit and withdrawal
    let client_id = 1;
    let tx_id = "1".to_string();
    let amount = 10.0;
    deposit(processor, client_id, tx_id.clone(), amount);

    let tx_id = "2".to_string();
    let amount = 5.0;
    deposit(processor, client_id, tx_id.clone(), amount);

    let tx_id = "3".to_string();
    let amount = 3.0;
    withdraw(processor, client_id, tx_id.clone(), amount);

    // Verify
    let cr = processor.get_client_record(client_id).unwrap();
    assert_eq!(cr.available, 12.0);
    assert_eq!(cr.total, 12.0);
    assert_eq!(cr.held, 0.0);
    assert_eq!(cr.locked, false);

    // Test dispute and resolve
    let client_id = 2;
    let tx_id = "4".to_string();
    let amount = 5.0;
    deposit(processor, client_id, tx_id.clone(), amount);

    let tx_id = "5".to_string();
    let amount = 50000.0;
    deposit(processor, client_id, tx_id.clone(), amount);

    dispute(processor, client_id, tx_id.clone());

    // Verify dispute
    let cr = processor.get_client_record(client_id).unwrap();
    assert_eq!(cr.available, 5.0);
    assert_eq!(cr.total, 50005.0);
    assert_eq!(cr.held, 50000.0);
    assert_eq!(cr.locked, false);

    resolve(processor, client_id, tx_id);

    // Verify resolve
    let cr = processor.get_client_record(client_id).unwrap();
    assert_eq!(cr.available, 50005.0);
    assert_eq!(cr.total, 50005.0);
    assert_eq!(cr.held, 0.0);
    assert_eq!(cr.locked, false);

    // Test chargeback
    let client_id = 3;
    let tx_id = "6".to_string();
    let amount = 5000000.0;
    deposit(processor, client_id, tx_id.clone(), amount);

    dispute(processor, client_id, tx_id.clone());

    chargeback(processor, client_id, tx_id);

    // Verify chargeback
    let cr = processor.get_client_record(client_id).unwrap();
    assert_eq!(cr.available, 0.0);
    assert_eq!(cr.total, 0.0);
    assert_eq!(cr.held, 0.0);
    assert_eq!(cr.locked, true);
}

fn deposit(
    processor: &mut impl Engine,
    client_id: u16,
    tx_id: String,
    amount: f32,
) {
    let tx_type = String::from("deposit");
    let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
    processor.deposit_tx(tx).unwrap();
}

fn withdraw(
    processor: &mut impl Engine,
    client_id: u16,
    tx_id: String,
    amount: f32,
) {
    let tx_type = String::from("withdrawal");
    let tx = Transaction::new(tx_type, client_id, tx_id, Some(amount));
    processor.withdrawal_tx(tx).unwrap();
}

fn dispute(processor: &mut impl Engine, client_id: u16, tx_id: String) {
    let tx_type = String::from("dispute");
    let tx = Transaction::new(tx_type, client_id, tx_id, None);
    processor.dispute_tx(&tx).unwrap();
}

fn resolve(processor: &mut impl Engine, client_id: u16, tx_id: String) {
    let tx_type = String::from("resolve");
    let tx = Transaction::new(tx_type, client_id, tx_id, None);
    processor.resolve_tx(&tx).unwrap();
}

fn chargeback(processor: &mut impl Engine, client_id: u16, tx_id: String) {
    let tx_type = String::from("chargeback");
    let tx = Transaction::new(tx_type, client_id, tx_id, None);
    processor.chargeback_tx(&tx).unwrap();
}
