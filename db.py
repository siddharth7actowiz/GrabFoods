import mysql.connector
import json
import time
from typing import List, Tuple
from config import *


def make_connection():
    return mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DB
    )


def create_tables(cursor, tab1, tab2):

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {rst_tble}(
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
    )
    """)

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {menu_tble}(
        id INT AUTO_INCREMENT PRIMARY KEY,
        Restaurant_ID VARCHAR(50),
        Category_Name VARCHAR(255),
        Item_Id VARCHAR(500),
        Item_Name VARCHAR(255),
        Item_Description TEXT,
        Item_Price DECIMAL(10,2),
        Item_Discounted_Price DECIMAL(10,2),
        Item_Image_URL JSON,
        Item_Thumbnail_URL JSON,
        Item_Available BOOLEAN,
        Is_Top_Seller BOOLEAN
    )
    """)


def batch_insert(cursor, con, insert_query: str, values: List[Tuple], batch_size: int = BATCH_SIZE):

    total_records = len(values)
    batch_count = 0
    failed_batches = []

    for start in range(0, total_records, batch_size):

        end = min(start + batch_size, total_records)
        batch = values[start:end]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()

            batch_count += 1
            print(f"Inserted batch {batch_count} ({start} -> {end})")

        except Exception as e:
            print(f"Batch failed ({start} -> {end})")
            print("Error:", e)
            failed_batches.append(batch)

    return batch_count, failed_batches


def insert_into_database(cursor,con,parsed_data_list):

    start_time = time.time()





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


    rest_query = f"""
    INSERT INTO {rst_tble}(
        Restaurant_ID,
        Restaurant_Name,
        Branch_Name,
        Cuisine,
        Tip,
        Timezone,
        ETA,
        DeliveryOptions,
        Rating,
        Is_Open,
        Currency_Code,
        Currency_Symbol,
        Offers,
        Timing_Everyday
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE Restaurant_ID = Restaurant_ID
    """

    menu_query = f"""
    INSERT INTO {menu_tble}(
        Restaurant_ID,
        Category_Name,
        Item_Id,
        Item_Name,
        Item_Description,
        Item_Price,
        Item_Discounted_Price,
        Item_Image_URL,
        Item_Thumbnail_URL,
        Item_Available,
        Is_Top_Seller
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE Item_Id = Item_Id
    """

    rest_batches, rest_failed = batch_insert(cursor, con, rest_query, rest_values)
    menu_batches, menu_failed = batch_insert(cursor, con, menu_query, menu_values)



    print("Restaurant batches:", rest_batches)
    print("Menu batches:", menu_batches)

    print("Failed restaurant batches:", len(rest_failed))
    print("Failed menu batches:", len(menu_failed))

    print("Total time:", time.time() - start_time)
