import sys
import time

import config

from utils import load_files
from parser import parse_json
from db import insert_into_database


def main():

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    batch_data = []

    start_time = time.time()

    for raw_json in load_files(config.DATA_DIR, start, end):

        parsed = parse_json(raw_json)

        if not parsed:
            continue

        batch_data.append(parsed)

        # When batch full → insert
        if len(batch_data) >= config.BATCH_SIZE:

            insert_into_database(batch_data)

            batch_data.clear()

    # Insert remaining records
    if batch_data:

        insert_into_database(batch_data)

    print("Total pipeline time:", time.time() - start_time)


if __name__ == "__main__":
    main()