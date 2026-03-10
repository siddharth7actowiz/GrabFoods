import mysql.connector
import json
import time
import logging
from typing import List, Tuple
from config import *

logger = logging.getLogger(__name__)


def make_connection():
    try:
        con = mysql.connector.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            database=DB
        )
        logger.debug("DB connection established")
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        raise


def create_tables(cursor, tab1, tab2):
    try:
        rest_ddl = f"""
        CREATE TABLE IF NOT EXISTS {tab1}(
            id INT AUTO_INCREMENT PRIMARY KEY,
            Restaurant_ID VARCHAR(50) UNIQUE,
            Restaurant_Name VARCHAR(255),
            Branch_Name VARCHAR(100),
            Cuisine TEXT,
            Tip JSON,
            Timezone VARCHAR(50),
            ETA SMALLINT,
            DeliveryOptions JSON,
            Rating DECIMAL(3,2),
            Is_Open BOOLEAN,
            Currency_Code CHAR(3),
            Currency_Symbol VARCHAR(10),
            Offers JSON,
            Timing_Everyday VARCHAR(150)
        )"""

        menu_ddl = f"""
        CREATE TABLE IF NOT EXISTS {tab2}(
            id INT AUTO_INCREMENT PRIMARY KEY,
            Restaurant_ID VARCHAR(50),
            Category_Name VARCHAR(255),
            Item_Id VARCHAR(500) UNIQUE,
            Item_Name VARCHAR(255),
            Item_Description TEXT,
            Item_Price DECIMAL(10,2),
            Item_Discounted_Price DECIMAL(10,2),
            Item_Image_URL JSON,
            Item_Thumbnail_URL JSON,
            Item_Available BOOLEAN,
            Is_Top_Seller BOOLEAN
        )"""

        logger.debug(f"DDL query for {tab1}:\n{rest_ddl}")
        cursor.execute(rest_ddl)

        logger.debug(f"DDL query for {tab2}:\n{menu_ddl}")
        cursor.execute(menu_ddl)

        logger.info(f"Tables ensured: '{tab1}', '{tab2}'")

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise


def batch_insert(cursor, con, insert_query: str, values: List[Tuple], batch_size: int = BATCH_SIZE):

    total_records  = len(values)
    batch_count    = 0
    failed_batches = []

    logger.debug(f"Starting batch insert — total records: {total_records}, batch size: {batch_size}")

    for start in range(0, total_records, batch_size):

        end   = min(start + batch_size, total_records)
        batch = values[start:end]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()
            batch_count += 1
            logger.debug(f"Batch {batch_count} inserted ({start} : {end})")

        except Exception as e:
            logger.error(f"Batch failed ({start} : {end}): {e}")
            failed_batches.append(batch)

    return batch_count, failed_batches


def insert_into_database(cursor, con, parsed_data_list):

    start_time  = time.time()
    rest_values = []
    menu_values = []

    for parsed_data in parsed_data_list:

        rest = parsed_data["Restaurant_Details"]

        rest_values.append((
            rest["Restaurant_ID"],
            rest["Restaurant_Name"],
            rest["Branch_Name"],
            rest["Cuisine"],
            json.dumps(rest["Tip"]),
            rest["Timezone"],
            rest["ETA"],
            json.dumps(rest["DeliveryOptions"]),
            rest["Rating"],
            rest["Is_Open"],
            rest["Currency_Code"],
            rest["Currency_Symbol"],
            json.dumps(rest["Offers"]),
            rest["Timing_Everyday"]
        ))

        for item in parsed_data["Menu_Items"]:
            menu_values.append((
                item["Restaurant_ID"],
                item["Category_Name"],
                item["Item_ID"],
                item["Item_Name"],
                item["Item_Description"],
                item["Item_Price"],
                item["Item_Discounted_Price"],
                json.dumps(item["Item_Image_URL"]),
                json.dumps(item["Item_Thumbnail_URL"]),
                item["Item_Available"],
                item["Is_Top_Seller"]
            ))

    logger.info(f"Preparing to insert — restaurants: {len(rest_values)}, menu items: {len(menu_values)}")

    rest_query = f"""
    INSERT INTO {tab1}(
        Restaurant_ID, Restaurant_Name, Branch_Name, Cuisine,
        Tip, Timezone, ETA, DeliveryOptions, Rating, Is_Open,
        Currency_Code, Currency_Symbol, Offers, Timing_Everyday
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE Restaurant_ID = Restaurant_ID
    """

    menu_query = f"""
    INSERT INTO {tab2}(
        Restaurant_ID, Category_Name, Item_Id, Item_Name,
        Item_Description, Item_Price, Item_Discounted_Price,
        Item_Image_URL, Item_Thumbnail_URL, Item_Available, Is_Top_Seller
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE Item_Id = Item_Id
    """

    # Log insert queries once before batching starts
    logger.debug(f"REST insert query:\n{rest_query}")
    logger.debug(f"MENU insert query:\n{menu_query}")

    rest_batches, rest_failed = batch_insert(cursor, con, rest_query, rest_values)
    menu_batches, menu_failed = batch_insert(cursor, con, menu_query, menu_values)

    

    logger.info(f"Restaurant batches inserted: {rest_batches} | failed: {len(rest_failed)}")
    logger.info(f"Menu item batches inserted:  {menu_batches} | failed: {len(menu_failed)}")
    logger.info(f"insert_into_database finished in {time.time() - start_time}s")

    if rest_failed:
        logger.warning(f"{len(rest_failed)} restaurant batch(es) failed — data may be incomplete")
    if menu_failed:
        logger.warning(f"{len(menu_failed)} menu batch(es) failed — data may be incomplete")
