# Koala Transaction Engine

Koala transaction engine (KTE) is the state of the art transaction processing engine for all your transaction simulation needs. It can either use CSV files to process the data or the database.

## Commands
- CSV method - `cargo run csv`
- DB method - `cargo run db`

### Main directories
- data - Contains the generated data for transactions, and is also a location for output data files and database.
- src - Contains all the rust code.
- tests - Integration tests for rust code.
- utils - Contains utility scripts for generating data and output testing

### Building and running locally
1. Git clone the project
2. Run either `cargo run csv` or `cargo run db`

You might have to install `libsqlite3-dev` on your linux system.

### Generating data sets
Use `tx_gen.py` in utils to generate transactions.

`python3 tx_gen.py -n 1000` 

where -n specifies the number of transactions to generate.

## Assumptions

1. Every new session with KTE assumes a fresh start with client accounts (Data is cleared).
2. Dispute is only for the deposit. 
3. No transaction will apply to an account after it's been frozen.

## Tests

Tests are included at various levels.

- Unit tests for main rust modules are present.
- Integration tests are present both for database processor and csv processor
- Python utility to match the output for both methods. Can be used to test large data sets.

## Testing locally
- Use `cargo test` to run Rust tests
- Use `test_output.py` to match the output with test_output data

example - `python3 test_output.py -t csv` or `python3 test_output.py -t db`

## CI

Test passes in CI with the large test data.
