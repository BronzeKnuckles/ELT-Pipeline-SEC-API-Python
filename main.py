from dataclasses import dataclass, asdict
import pandas as pd
import os
import stat
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import register_adapter, AsIs
import numpy
from multiprocessing import Pool, cpu_count
import logging


logging.basicConfig(
    filename="app.log", filemode="w", format="%(name)s - %(levelname)s - %(message)s"
)


@dataclass
class Row_item:
    cik: str
    name: str
    fact: str
    label: str
    units: str
    end_date: str
    val: str
    accn: str
    fy: str
    fp: str
    form: str
    filed: str
    frame: str = "NA"


def close_connection(conn, cursor):
    cursor.close()
    conn.close()


def get_files_list(path):

    directory_path = path

    files_list = []
    # Loop through each file in the directory
    for filename in os.listdir(directory_path):
        # Construct the full path to the file
        file_path = os.path.join(directory_path, filename)

        # Check if it's a file and ends with '.json'
        if os.path.isfile(file_path) and filename.endswith(".json"):
            # Get the size of the file in bytes
            size = os.path.getsize(file_path)

            # Check if the file is over 100KB (1KB = 1024 bytes, so 100KB = 1024 * 100 bytes)
            if size > 1024 * 100:
                files_list.append(filename)

    return files_list


def get_data(filename):
    all_rows_in_file = []
    df = pd.read_json(filename)
    if len(df["entityName"]) != 0:
        for fact in list(df["facts"]["us-gaap"].keys()):
            for unit_keys in list(df["facts"]["us-gaap"][fact]["units"].keys()):
                for i in df["facts"]["us-gaap"][fact]["units"][unit_keys]:
                    # print(i)
                    if "frame" in i:
                        frame = i["frame"]
                    else:
                        frame = "NA"
                    all_rows_in_file.append(
                        Row_item(
                            cik=str(df["cik"]["us-gaap"]),
                            name=df["entityName"]["us-gaap"],
                            fact=fact,
                            label=df["facts"]["us-gaap"][fact]["label"],
                            units=str(unit_keys).strip(),
                            end_date=i["end"],
                            val=i["val"],
                            accn=i["accn"],
                            fy=i["fy"],
                            fp=i["fp"],
                            form=i["form"],
                            filed=i["filed"],
                            frame=frame,
                        )
                    )
    return all_rows_in_file


def connect_to_db():
    # Database connection parameters #TODO: Fill this! DB credentials...
    try:
        DB_PARAMS = {
            "dbname": "sec-dev",
            "user": "postgres",
            "password": "pass",
            "host": "localhost",
            "port": "5432",
        }
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
    except Exception as e:
        print(f"An error occurred at connect_to_db(): {e}")

    return conn, cursor


def create_table_run_once(conn, cursor):
    # Create table -- RUN ONCE !
    # TODO Change data types !
    CREATE_TABLE_QUERY = """
    CREATE TABLE main (
        cik VARCHAR(20), 
        name VARCHAR(100), 
        fact VARCHAR(200), 
        label TEXT,
        units VARCHAR(20), 
        end_date DATE,
        val TEXT, 
        accn VARCHAR(20), 
        fy VARCHAR(10), 
        fp VARCHAR(10), 
        form VARCHAR(10), 
        filed DATE, 
        frame VARCHAR(10)
    );

    """
    try:
        cursor.execute(CREATE_TABLE_QUERY)
        conn.commit()
        print("Table Created !")
    except Exception as e:
        print(f"An error occurred at create_table_run_once(): {e}")


def add_numpy_int64_adapter():
    def adapt_numpy_int64(numpy_int64):
        return AsIs(int(numpy_int64))

    register_adapter(numpy.int64, adapt_numpy_int64)


def insert_rows(rows, conn, cursor):
    # Convert data class instances to dictionaries
    rows_dicts = [asdict(row) for row in rows]
    # print(rows_dicts)

    try:

        # SQL query to insert data # TODO
        query = """
        INSERT INTO main (cik, name, fact, label, units, end_date, val, accn, fy, fp, form, filed, frame)
        VALUES (%(cik)s, %(name)s, %(fact)s, %(label)s, %(units)s, %(end_date)s, %(val)s, %(accn)s, %(fy)s, %(fp)s, %(form)s, %(filed)s, %(frame)s);
        """

        # Insert all rows into the database
        execute_batch(cursor, query, rows_dicts)

        # Commit the transaction
        conn.commit()

        print("Data inserted successfully")

    except Exception as e:
        print(f"An error occurred at insert_rows(): {e}")


def main():  # Not in use TODO: Remove
    conn, cursor = connect_to_db()
    create_table_run_once(conn, cursor)
    add_numpy_int64_adapter()
    path = "./extracted"
    files_list = get_files_list(path)
    for idx, file in enumerate(files_list):
        all_rows_in_file = get_data(f"{path}/{file}")

        insert_rows(all_rows_in_file, conn, cursor)
        logging.info(f"Data Inserted Successfully from {file}")
        if (idx + 1) % 10 == 0:
            print(f"Inserted {idx+1} files ! \n")
    close_connection(conn, cursor)


def process_file(file_path):
    """
    Process a single file to read its content, extract data, and insert into the database.
    This function is intended to be used with multiprocessing.
    """
    conn, cursor = connect_to_db()
    add_numpy_int64_adapter()
    try:
        all_rows_in_file = get_data(file_path)
        if all_rows_in_file:
            insert_rows(all_rows_in_file, conn, cursor)

        print(f"Processed and inserted data from {file_path}")
    except Exception as e:
        logging.error(f" Error at process_file({file_path}) - {e}")
    close_connection(conn, cursor)


def give_permission(path):
    directory_path = path

    # Define the new permissions: Read, write, and execute for the owner,
    # read and execute for group and others.
    new_permissions = (
        stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    )

    try:
        os.chmod(directory_path, new_permissions)
        print(f"Permissions changed for {directory_path}")
    except PermissionError as e:
        print(f"PermissionError: {e}")
    except FileNotFoundError as e:
        print(f"FileNotFoundError: {e}")


def main_multiprocessing():
    path = "C:/Users/srive/Desktop/Code/python/sec-financial-data/extracted"
    give_permission(path)  # Not Required
    conn, cursor = connect_to_db()
    create_table_run_once(conn, cursor)
    close_connection(conn, cursor)  # Close the initial DB connection

    files_list = get_files_list(path)
    files_full_path = [os.path.join(path, file) for file in files_list]

    # Determine the number of processes to use
    num_processes = 6

    # Use a multiprocessing Pool to process files in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(process_file, files_full_path)

    print("All files processed and data inserted.")


if __name__ == "__main__":
    main_multiprocessing()
