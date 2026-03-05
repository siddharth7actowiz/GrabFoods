import sys
import time
import config
from utils import load_files
from parser import parse_json
from db import insert_into_database


def main():
    start = int(sys.argv[1])
    end = int(sys.argv[2])

    start_time = time.time()

    for raw_json in load_files(config.DATA_DIR, start, end):

        parsed = parse_json(raw_json)
        insert_into_database([parsed])

    print(f"Total time for {start} to {end}:", time.time() - start_time)



if __name__ == "__main__":
    main()

