# Utility file to generate large number of transactions quickly

import os
import csv
import uuid
import toml
import random
import argparse

config_file = os.path.join(os.path.dirname(__file__), "..", "proj-config.toml")
config = toml.load(config_file)

TX_FILE_PATH = config["input_file"]
REC_FILE_PATH = config["output_test_file"]

TX_TO_GENERATE = 100
MAX_AMOUNT = 50000
APPROX_TOTAL_CLIENTS = 2000  # Auto incremented if accounts are locked
ROUND_DIGITS = 2
TXS = []
CLIENT_DICT = {}
TX_COUNT = 0


def increment_tx():
    global TX_COUNT
    TX_COUNT += 1


def gen_tx_id():
    return str(uuid.uuid4())


def get_random_client_ids():
    client_ids = list(CLIENT_DICT.keys())
    random.shuffle(client_ids)

    return client_ids


def find_unlocked_client():
    client_id = None
    client_ids = get_random_client_ids()

    for key in client_ids:
        if CLIENT_DICT[key]["locked"] == False:
            client_id = key

    return client_id


def deposit(writer, provided_id=None):
    tx_id = gen_tx_id()
    client_id = None

    if provided_id:
        client_id = provided_id
    else:
        client_id = find_unlocked_client()

    if client_id == None:
        print("Unable to find client with unlocked account, new client required")
        return 3

    amount = random.uniform(1, MAX_AMOUNT)
    amount = round(amount, ROUND_DIGITS)

    writer.writerow(["deposit", client_id, tx_id, amount])

    available = CLIENT_DICT[client_id]["available"]
    new_available = available + amount
    CLIENT_DICT[client_id]["available"] = new_available

    total = CLIENT_DICT[client_id]["total"]
    new_total = total + amount
    CLIENT_DICT[client_id]["total"] = new_total

    TXS.append((tx_id, client_id, amount, "dep"))
    increment_tx()

    return 0


def withdrawal(writer):
    tx_id = gen_tx_id()
    client_id = find_unlocked_client()

    if client_id == None:
        print("Unable to find client with unlocked account, new client required")
        return 3

    available_balance = CLIENT_DICT[client_id]["available"]

    if available_balance > 0:
        amount = random.uniform(0, available_balance - 1)
        amount = round(amount, ROUND_DIGITS)

        writer.writerow(["withdrawal", client_id, tx_id, amount])

        available = CLIENT_DICT[client_id]["available"]
        new_available = available - amount
        CLIENT_DICT[client_id]["available"] = new_available

        total = CLIENT_DICT[client_id]["total"]
        new_total = total - amount
        CLIENT_DICT[client_id]["total"] = new_total

        TXS.append((tx_id, client_id, amount, "wdl"))
        increment_tx()

        return 0
    else:
        return 1


def dispute(writer):
    client_id = None
    available = None

    client_ids = get_random_client_ids()
    # Find a client that has positive balance
    for key in client_ids:
        client = CLIENT_DICT[key]
        if (
            client["held"] == 0
            and client["available"] > 0
            and client["locked"] == False
        ):
            client_id = key
            available = client["available"]

    if client_id == None:
        return 1

    # Get the amount
    amount, tx_id = get_tx_data(client_id, available)

    if amount == None:
        return 1

    # Create the transaction
    writer.writerow(["dispute", client_id, tx_id, None])

    available = CLIENT_DICT[client_id]["available"]
    new_available = available - amount
    CLIENT_DICT[client_id]["available"] = new_available

    held = CLIENT_DICT[client_id]["held"]
    new_held = held + amount
    CLIENT_DICT[client_id]["held"] = new_held
    increment_tx()

    return 0


def resolve(writer):
    client_id = None
    held = None

    client_ids = get_random_client_ids()
    # Find a client with held balance
    for key in client_ids:
        client = CLIENT_DICT[key]
        if client["held"] > 0:
            client_id = key
            held = client["held"]

    if client_id == None:
        return 1

    tx_id = get_held_tx_data(client_id, held)

    if tx_id == None:
        return 1

    # Create the transaction
    writer.writerow(["resolve", client_id, tx_id, None])

    curr_held = CLIENT_DICT[client_id]["held"]
    new_held = curr_held - held
    CLIENT_DICT[client_id]["held"] = new_held

    available = CLIENT_DICT[client_id]["available"]
    new_available = available + held
    CLIENT_DICT[client_id]["available"] = new_available
    increment_tx()

    return 0


def chargeback(writer):
    client_id = None
    held = None

    client_ids = get_random_client_ids()
    # Find a client with held balance
    for key in client_ids:
        client = CLIENT_DICT[key]
        if client["held"] > 0:
            client_id = key
            held = client["held"]

    if client_id == None:
        return 1

    tx_id = get_held_tx_data(client_id, held)

    if tx_id == None:
        return 1

    # Create the transaction
    writer.writerow(["chargeback", client_id, tx_id, None])

    curr_held = CLIENT_DICT[client_id]["held"]
    new_held = curr_held - held
    CLIENT_DICT[client_id]["held"] = new_held

    total = CLIENT_DICT[client_id]["total"]
    new_total = total - held
    CLIENT_DICT[client_id]["total"] = new_total

    CLIENT_DICT[client_id]["locked"] = True
    increment_tx()

    return 0


def get_tx_data(client_id, available):
    amount = None
    tx_id = None
    for i in TXS:
        if i[1] == client_id and i[3] == "dep" and available > i[2]:
            amount = i[2]
            tx_id = i[0]

    return amount, tx_id


def get_held_tx_data(client_id, held):
    tx_id = None
    for i in TXS:
        if i[1] == client_id and i[3] == "dep" and held == i[2]:
            tx_id = i[0]

    return tx_id


def init_client_dict():
    for i in range(1, APPROX_TOTAL_CLIENTS + 1):
        CLIENT_DICT[i] = {
            "available": 0,
            "held": 0,
            "total": 0,
            "locked": False,
        }


def gen_new_client():
    id = len(CLIENT_DICT) + 1
    CLIENT_DICT[id] = {
        "available": 0,
        "held": 0,
        "total": 0,
        "locked": False,
    }

    return id


def main(number_of_txs):
    init_client_dict()

    # Reset the csv files
    if os.path.exists(TX_FILE_PATH):
        os.remove(TX_FILE_PATH)
    if os.path.exists(REC_FILE_PATH):
        os.remove(REC_FILE_PATH)

    with open(TX_FILE_PATH, "w", newline="") as csvfile:
        wrt = csv.writer(csvfile, delimiter=",")
        # Write the headers first
        wrt.writerow(["type", "client", "tx", "amount"])

        while TX_COUNT < number_of_txs:
            # Choose a random transaction type
            tx_func = random.choices(
                population=[deposit, withdrawal, dispute, resolve, chargeback],
                weights=[2, 1, 1, 0.5, 0.05],
            )[0]

            result = tx_func(wrt)
            # This is to ensure the number of transactions match
            if result == 1:
                deposit(wrt)
            elif result == 3:
                id = gen_new_client()
                deposit(wrt, id)

            print("Generated txs", TX_COUNT)

    print("Total transactions generated ----> ", TX_COUNT)

    # Write the output
    with open(REC_FILE_PATH, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        # Write the headers
        writer.writerow(["client", "available", "held", "total", "locked"])

        for client in CLIENT_DICT:
            available = CLIENT_DICT[client]["available"]
            held = CLIENT_DICT[client]["held"]
            total = CLIENT_DICT[client]["total"]
            locked = CLIENT_DICT[client]["locked"]

            if available != 0 or held != 0 or total != 0 or locked != False:

                writer.writerow(
                    [
                        client,
                        CLIENT_DICT[client]["available"],
                        CLIENT_DICT[client]["held"],
                        CLIENT_DICT[client]["total"],
                        CLIENT_DICT[client]["locked"],
                    ]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        default=TX_TO_GENERATE,
        help="Number of transactions to generate",
    )
    args = parser.parse_args()
    main(args.number)
