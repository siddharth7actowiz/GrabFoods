import sys
import time
import threading
import logging
import os
from config import *
from utils import load_files
from parser import parse_json
from db import insert_into_database, make_connection, create_tables

# Logger Setup 

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(threadName)-10s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log", mode="a"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

#  Worker 
def process_chunk(chunk_start, chunk_end):
    logger.info(f"Started  → files {chunk_start} to {chunk_end}")
    
    try:
        con = make_connection()
        cursor = con.cursor()
        create_tables(cursor, tab1, tab2)
        logger.debug(f"DB connection established for range {chunk_start}-{chunk_end}")
    except Exception as e:
        logger.error(f"DB connection failed for range {chunk_start}-{chunk_end}: {e}")
        return

    parsed_batch = []
    total_parsed = 0
    total_failed = 0

    for raw_json in load_files(DATA_DIR, chunk_start, chunk_end):
        parsed = parse_json(raw_json)

        if not parsed:
            total_failed += 1
            
            continue

        parsed_batch.append(parsed)
        total_parsed += 1

        if len(parsed_batch) >= BATCH_SIZE:
            try:
                insert_into_database(cursor, con, parsed_batch)
                logger.debug(f"Inserted batch of {len(parsed_batch)} records")
            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
            parsed_batch.clear()

    # Insert remaining
    if parsed_batch:
        try:
            insert_into_database(cursor, con, parsed_batch)
            logger.debug(f"Inserted final batch of {len(parsed_batch)} records")
        except Exception as e:
            logger.error(f"Final batch insert failed: {e}")

    cursor.close()
    con.close()

    logger.info(f"Finished → files {chunk_start} to {chunk_end} | parsed={total_parsed} skipped={total_failed}")


#  Main Thread
def main():
    
    logger.info("Pipeline started")

    start = int(sys.argv[1])
    end   = int(sys.argv[2])

    total_files = end - start
    chunk_size  = total_files // total_threads

    logger.info(f"Files: {start} to {end} | Total: {total_files} | Threads: {total_threads} | Chunk size: {chunk_size}")

    threads = []

    for i in range(total_threads):
        chunk_start = start + i * chunk_size
        chunk_end   = end if i == total_threads - 1 else chunk_start + chunk_size

        t = threading.Thread(target=process_chunk,args=(chunk_start, chunk_end),name=f"Thread-{i+1}"
        )
        threads.append(t)

    start_time = time.time()

    for t in threads:
        t.start()
        logger.debug(f"{t.name} launched → {t._args}")

    for t in threads:
        t.join()

    elapsed = time.time() - start_time
    logger.info(f"All threads finished in {elapsed:.2f}s")
    logger.info("Pipeline complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
