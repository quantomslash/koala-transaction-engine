# Koala Transaction Engine

Koala transaction engine (KTE) is the state of the the art transaction processing engine for all your transaction simulation needs.

## What works?

- Builds and Runs - YES
- Formatted - YES,
- Handle all cases - YES,
- Tested - YES

## Safety

- CSV reading and writing is serialized and pretty thoroughly checked for issues.
- Where possible result types are used.

## Efficiency

- Uses CSV to keep track of (and simulate) client data. This is to avoid issue where the dataset could be very large and keeping everything in memory might come at a cost. Ideally you would use some kind of performance driven atomic database at the backend. 
- Processed transactions are kept in memory for now. In order to limit the time spend on the project, this decision was made. Normally you would also keep this kind of information in the backend database as well.

## Assumptions

1. Every new session with KTE assumes fresh start with client accounts (Data is cleared);
2. Dispute is only for the deposit. 
3. No transaction will apply to an account after it's been frozen.

## Tests

One quick integration test is present that accepts transactions.csv as an input and verifies the output against precalculated data set.

In order to limit the time spend on this, I skipped the unit tests and decided to test the whole thing with this test.

Also, the transaction set has been manually tested frequently during development.

## CI

Test passes in CI with the sample test data.

## Further improvements

1. find_original_transaction algorithm could perhaps be improved if the dataset is really large.
2. Use a fast atomic database.
3. Don't use CSVs.
4. Remove hardcoded filenames.
