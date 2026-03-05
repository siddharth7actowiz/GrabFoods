import mysql.connector
import json
import time

from typing import List, Tuple
from config import USER, PASSWORD, HOST, PORT, DB, BATCH_SIZE


# -------------------------------
# Database Connection
# -------------------------------

def make_connection():

    return mysql.connector.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=DB
    )


# -------------------------------
# Create Tables
# -------------------------------

def create_tables(cursor):

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PDP_RESTAURANT_TABLE(
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PDP_MENU_ITEMS_TABLE(
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


# -------------------------------
# Batch Insert Function
# -------------------------------

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


# -------------------------------
# Main Insert Function
# -------------------------------

def insert_into_database(parsed_data_list):

    start_time = time.time()

    con = make_connection()
    cursor = con.cursor()

    create_tables(cursor)

    rest_values = []
    menu_values = []

    # Prepare Values
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

    # Insert Queries

    rest_query = """
    INSERT IGNORE INTO PDP_RESTAURANT_TABLE
    VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    menu_query = """
    INSERT IGNORE INTO PDP_MENU_ITEMS_TABLE
    VALUES (NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # Batch Insert

    rest_batches, rest_failed = batch_insert(
        cursor,
        con,
        rest_query,
        rest_values
    )

    menu_batches, menu_failed = batch_insert(
        cursor,
        con,
        menu_query,
        menu_values
    )

    cursor.close()
    con.close()

    # Summary

    print("\nInsertion Summary")
    print("-------------------")

    print("Restaurant batches:", rest_batches)
    print("Menu batches:", menu_batches)

    print("Failed restaurant batches:", len(rest_failed))
    print("Failed menu batches:", len(menu_failed))

    print("Total time:", time.time() - start_time)