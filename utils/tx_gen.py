import os
import csv
import uuid
import random

CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
TX_FILE_PATH = os.path.join(CSV_DIR, "transactions.csv")
REC_FILE_PATH = os.path.join(CSV_DIR, "client_records.csv")
TX_TO_GENERATE = 100
MAX_AMOUNT = 50000
MIN_CLIENTS = 5
MAX_CLIENTS = 50
TXS = []
CLIENT_DICT = {}


def gen_tx_id():
    return str(uuid.uuid4())


def deposit(writer, client_id, amount):
    tx_id = gen_tx_id()
    writer.writerow(["deposit", client_id, tx_id, amount])

    available = CLIENT_DICT[client_id]["available"]
    new_available = round(available + amount, 2)
    CLIENT_DICT[client_id]["available"] = new_available
    
    total = CLIENT_DICT[client_id]["total"]
    new_total = round(total + amount, 2)
    CLIENT_DICT[client_id]["total"] = new_total

    TXS.append((tx_id, client_id, amount, "dep"))


def withdrawal(writer, client_id, amount):
    tx_id = gen_tx_id()
    writer.writerow(["withdrawal", client_id, tx_id, amount])

    available = CLIENT_DICT[client_id]["available"]
    new_available = round(available - amount, 2)
    CLIENT_DICT[client_id]["available"] = new_available

    total = CLIENT_DICT[client_id]["total"]
    new_total = round(total - amount, 2)
    CLIENT_DICT[client_id]["total"] = new_total

    TXS.append((tx_id, client_id, amount, "wdl"))


def dispute(writer):
    client_id = None
    available = None
    # Find a client that has positive balance and dispute for simplicity
    for key, value in CLIENT_DICT.items():
        if value["held"] == 0 and value["available"] > 0:
            client_id = key
            available = value["available"]

    if client_id == None:
        print("Couldn't find valid tx for dispute, skipping ...")
        return

    # Get the amount
    amount, tx_id = get_tx_data(client_id, available)

    if amount == None:
        print("Couldn't find valid tx for dispute, skipping ...")
        return

    # Create the transaction
    writer.writerow(["dispute", client_id, tx_id, None])

    available = CLIENT_DICT[client_id]["available"]
    new_available = round(available - amount, 2)
    CLIENT_DICT[client_id]["available"] = new_available

    held = CLIENT_DICT[client_id]["held"]
    new_held = round(held + amount, 2)
    CLIENT_DICT[client_id]["held"] = new_held


def get_tx_data(client_id, available):
    amount = None
    tx_id = None
    for i in TXS:
        if i[1] == client_id and i[3] == "dep" and available > i[2]:
            amount = i[2]
            tx_id = i[0]
    return amount, tx_id


def init_client_dict():
    total_clients = random.randint(MIN_CLIENTS, MAX_CLIENTS)
    for i in range(total_clients):
        id = i + 1
        CLIENT_DICT[id] = {
            "available": 0,
            "held": 0,
            "total": 0,
            "locked": False,
        }


def main():
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

        for i in range(TX_TO_GENERATE):
            # Choose a random transaction type
            tx_func = random.choices(
                population=[deposit, withdrawal, dispute], weights=[1, 1, 0.1]
            )[0]

            # Find a random client for the transaction
            random_client = random.choice(list(CLIENT_DICT.keys()))

            amount = 0
            # if it's a withdrawal transaction, ensure the balance remains positive
            available_balance = CLIENT_DICT[random_client]["available"]
            if tx_func == withdrawal:
                if available_balance > 0:
                    amount = random.uniform(1, available_balance)
                    amount = round(amount, 2)
                else:
                    tx_func = deposit
            else:
                amount = random.uniform(1, MAX_AMOUNT)
                amount = round(amount, 2)

            # Generate the transactions
            if tx_func == dispute:
                tx_func(wrt)
            else:
                tx_func(wrt, random_client, amount)

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
    main()
