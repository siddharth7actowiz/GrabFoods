import sys
import time
from config import *
from utils import load_files
from parser import parse_json
from db import insert_into_database,make_connection,create_tables


def main():
    start = int(sys.argv[1])
    end = int(sys.argv[2])

    con = make_connection()
    cursor = con.cursor()

    create_tables(cursor, rst_tble, menu_tble)

    parsed_batch = []

    for raw_json in load_files(DATA_DIR, start, end):

        parsed = parse_json(raw_json)

        if not parsed:
            continue

        parsed_batch.append(parsed)

        if len(parsed_batch) >= 500:   # large batch
            insert_into_database(cursor, con, parsed_batch)
            parsed_batch.clear()

    # insert remaining
    if parsed_batch:
        insert_into_database(cursor, con, parsed_batch)

    cursor.close()
    con.close()

main()
