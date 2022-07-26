use std::error::Error;
use koala_transaction_engine::process;

fn main() -> Result<(), Box<dyn Error>>{
    if let Some(filename) = std::env::args().nth(1) {
        let output_file = process(filename)?;
        let contents = std::fs::read_to_string(output_file)?;
        print!("{}", contents);
        Ok(())
    } else {
        println!(" You must provide a filename");
        Ok(())
    }    
}
