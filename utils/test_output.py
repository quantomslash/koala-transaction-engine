# A simple utility to test the output data with test data

import csv
import os
import toml
import math
import sqlite3
import argparse

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "proj-config.toml")

config = toml.load(CONFIG_FILE)

TEST_DATA_FILE = config["output_test_file"]
OUTPUT_FILE = config["output_file"]
DB_FILE = config["tmp_db_file"]
DB_NAME = "CLIENT_RECORDS"


def assert_data(a, b):
    data_a = math.floor(float(a))
    data_b = math.floor(float(b))
    print("Comparing {} with {}".format(data_a, data_b))
    try:
        assert data_a == data_b
    except AssertionError:
        print("Possible rounding issue")
        if abs(data_a - data_b) <= 1:
            print("Aprox equal value, it's a pass")
        else:
            raise AssertionError


def test_record(row):
    with open(TEST_DATA_FILE) as test_file:
        test_reader = csv.DictReader(test_file)
        for test_row in test_reader:

            if test_row["client"] == row["client"]:
                print("\n===================")
                print(row)
                print(test_row)
                print("======================")

                assert_data(test_row["available"], row["available"])
                assert_data(test_row["held"], row["held"])
                assert_data(test_row["total"], row["total"])

                assert test_row["locked"].lower() == row["locked"].lower()


def test_csv_output():
    with open(OUTPUT_FILE) as out_file:
        output_reader = csv.DictReader(out_file)

        for row in output_reader:
            test_record(row)


def test_db_output():
    conn = sqlite3.connect(DB_FILE)

    with open(TEST_DATA_FILE) as test_file:
        test_reader = csv.DictReader(test_file)

        for test_row in test_reader:
            client_id = test_row["client"]
            cursor = conn.execute(f"SELECT * FROM {DB_NAME} WHERE id={client_id}")
            row = cursor.fetchone()

            print("\n===================")
            row_info = f"'client': '{row[0]}', 'available': '{row[1]}', 'held': '{row[2]}', 'total': '{row[3]}', 'locked': '{row[4]}'"
            print(f"{{{row_info}}}")
            print(test_row)
            print("======================")

            assert_data(test_row["available"], row[1])
            assert_data(test_row["held"], row[2])
            assert_data(test_row["total"], row[3])

            locked = None
            if row[4] == 1:
                locked = "true"
            else:
                locked = "false"

            assert test_row["locked"].lower() == locked


def main(method):
    if method == "csv":
        test_csv_output()
    elif method == "db":
        test_db_output()
    else:
        print("Something went wrong")
        from sys import exit

        exit()

    print("\n\nSuccess!!")
    print("==========================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m", "--method", type=str, default="csv", help="Test db or csv?"
    )
    args = parser.parse_args()
    main(args.method)
