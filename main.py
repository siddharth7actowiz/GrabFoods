import sys
import time
import threading
import logging
import os

from config import *
from utils import load_files
from parser import parse_json
from db import insert_into_database, make_connection, create_tables

#Logger Setup
logger = logging.getLogger("Pipeline_Logger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(threadName)s - %(levelname)s - %(message)s"
)

file_handler = logging.FileHandler("MainLog")
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


#Helper Function for threading
def process_chunk(chunk_start, chunk_end):

    con = make_connection()
    cursor = con.cursor()

    logger.info(f"Started files {chunk_start} to {chunk_end}")

    parsed_batch = []

    for raw_json in load_files(DATA_DIR, chunk_start, chunk_end):

        parsed = parse_json(raw_json)

        if not parsed:
            continue

        parsed_batch.append(parsed)

        if len(parsed_batch) >= BATCH_SIZE:

            insert_into_database(cursor, con, parsed_batch, TABLE_NAME)

            parsed_batch.clear()

    if parsed_batch:
        insert_into_database(cursor, con, parsed_batch, TABLE_NAME)

    cursor.close()
    con.close()

# Main

def main():

    logger.info("Pipeline started")

    start = int(sys.argv[1])
    end = int(sys.argv[2])

    total_files = end - start
    chunk_size = max(1, total_files // total_threads)

    logger.info(
        f"Files: {start} to {end} | Total: {total_files} | Threads: {total_threads} | Chunk size: {chunk_size}"
    )


    try:

        con = make_connection()

        cursor = con.cursor()

        create_tables(cursor, TABLE_NAME)

        logger.info("Database connected and table verified")

    except Exception as e:

        logger.error(f"Database initialization failed: {e}")

        return


    threads = []


    for i in range(total_threads):

        chunk_start = start + i * chunk_size
        chunk_end = end if i == total_threads - 1 else chunk_start + chunk_size


        t = threading.Thread(
            target=process_chunk,
            args=(chunk_start, chunk_end),
            name=f"Thread-{i+1}"
        )

        threads.append(t)


    start_time = time.time()


    for t in threads:

        t.start()

        logger.info(f"{t.name} launched   range {t._args}")


    for t in threads:

        t.join()


    con.close()

    elapsed = time.time() - start_time

    logger.info(f"All threads finished in {elapsed:.2f}s")
    logger.info("Pipeline complete")
    


if __name__ == "__main__":
    main()
