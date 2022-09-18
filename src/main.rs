use config::Config;
use koala_transaction_engine::cs::CSVProcessor;
use koala_transaction_engine::db::DBProcessor;
use koala_transaction_engine::errors::KoalaError;
use koala_transaction_engine::process_txs;
use std::env::args;
use std::error::Error;
use std::fs;

/// Starts the processing, checks for the
/// method provided by args and call the
/// appropriate processing engine
fn main() -> Result<(), Box<dyn Error>> {
    // Get the config
    let conf = Config::builder()
        .add_source(config::File::with_name("proj-config.toml"))
        .build()
        .unwrap();
    let input_file = conf.get_string("input_file").unwrap();
    let output_file = conf.get_string("output_file").unwrap();

    // Get the method for processing
    let input = args().nth(1).unwrap();
    let method = input.as_str();

    // Check if we have the method preference from the user
    if !(method == "csv" || method == "db") {
        println!("{:?}", method);
        panic!("Either use 'csv' or 'db' for method");
    }

    // Check what method is given, and process with appropriate data
    match method {
        "csv" => {
            let tmp_file = conf.get_string("tmp_csv_file").unwrap();
            reset_file(output_file.as_str())?;
            reset_file(&tmp_file.as_str())?;
            let proc = CSVProcessor::new(tmp_file, output_file)?;
            process_txs(input_file, proc)?;
        }
        "db" => {
            let db_file = conf.get_string("tmp_db_file").unwrap();
            reset_file(db_file.as_str())?;
            let proc = DBProcessor::new(db_file)?;
            process_txs(input_file, proc)?;
        }
        _ => panic!("Something unexpected went wrong"),
    }
    println!();
    println!("Success!!!");

    Ok(())
}

/// Resets the required data files
pub fn reset_file(file: &str) -> Result<(), KoalaError> {
    println!("Resetting file {}", file);
    if std::path::Path::new(file).exists() {
        println!("Removing file: {:?}", file);
        fs::remove_file(file)?;
    }
    println!("Creating file: {:?}", file);
    fs::File::create(file)?;

    println!("Finished resetting file");

    Ok(())
}
