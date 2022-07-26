use std::error::Error;
use koala_transaction_engine::{ClientRecord, process};

static CLIENT_FILE_NAME: &str = "transactions.csv";

#[test]
fn test_data() -> Result<(), Box<dyn Error>>{
    let filename = CLIENT_FILE_NAME.to_string();
    let output_file = process(filename)?;

    let mut rdr = csv::Reader::from_path(output_file)?;
    
    for result in rdr.deserialize() {
        let record: ClientRecord = result?;

        match record.id {
            1 => {
                assert_eq!(record.available, 4.5);
                assert_eq!(record.held, 0.0);
                assert_eq!(record.total, 4.5);
                assert_eq!(record.locked, false);
            },

            2 => {
                assert_eq!(record.available, 2.02);
                assert_eq!(record.held, 0.0);
                assert_eq!(record.total, 2.02);
                assert_eq!(record.locked, false); 
            },

            3333 => {
                assert_eq!(record.available, 40.0);
                assert_eq!(record.held, 0.0);
                assert_eq!(record.total, 40.0);
                assert_eq!(record.locked, false);
            },
            
            356 => {
                assert_eq!(record.available, 0.0);
                assert_eq!(record.held, 0.0);
                assert_eq!(record.total, 0.0);
                assert_eq!(record.locked, true);
            },
            
            _ => ()
        }
    }    

    Ok(())
}
