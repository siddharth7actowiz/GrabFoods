import mysql.connector
import logging
import json
import os
from config import *

os.makedirs("logs", exist_ok=True)

db_logger = logging.getLogger("db_logger")
db_logger.setLevel(logging.INFO)

handler = logging.FileHandler("db_logs.log")

formatter = logging.Formatter("%(message)s")
handler.setFormatter(formatter)

db_logger.handlers.clear()
db_logger.addHandler(handler)

db_logger.propagate = False


def make_connection():

    return mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB,
        port=PORT
    )


def create_tables(cursor, table_name):

    create_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    Restaurant_ID VARCHAR(50) PRIMARY KEY,
    Restaurant_Name TEXT,
    Branch_Name TEXT,
    Cuisine TEXT,
    Tip JSON,
    Timezone TEXT,
    ETA TEXT,
    DeliveryOptions JSON,
    Rating FLOAT,
    Is_Open BOOLEAN,
    Currency_Code TEXT,
    Currency_Symbol TEXT,
    Offers JSON,
    Timing_Everyday TEXT,
    Menu JSON
)
"""

    db_logger.info(create_query)

    cursor.execute(create_query)


def insert_into_database(cursor, con, batch, table_name):

    try:

        for data in batch:

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))

            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE Restaurant_ID=Restaurant_ID
                """

            values = []

            for v in data.values():

                if isinstance(v, (list, dict)):
                    v = json.dumps(v)

                elif isinstance(v, bool):
                    v = int(v)

                values.append(v)

            values = tuple(values)


            formatted_values = []

            for v in values:

                if isinstance(v, str):
                    formatted_values.append(f"'{v}'")

                elif v is None:
                    formatted_values.append("NULL")

                else:
                    formatted_values.append(str(v))

            values_str = ", ".join(formatted_values)

            log_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({values_str})
            ON DUPLICATE KEY UPDATE Restaurant_ID=Restaurant_ID;
            """
            db_logger.info(log_query)
        
            cursor.execute(insert_query, values)

        
        con.commit()

    except Exception as e:

        con.rollback()

        db_logger.error(f"INSERT FAILED: {e}")
