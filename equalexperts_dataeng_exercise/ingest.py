import duckdb
import json
import logging
import time
import os
from pathlib import Path
import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def setup_database(db_file):
    try:
        conn = duckdb.connect(database=str(db_file))
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS blog_analysis;")
        logging.info("Schema 'blog_analysis' created or already exists.")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            id INTEGER PRIMARY KEY,
            postid INTEGER,
            votetypeid INTEGER,
            creationdate TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        logging.info(
            "Table 'votes' created or already exists in schema 'blog_analysis'."
        )

        return conn, cursor
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        raise


def insert_votes(cursor, votes):
    try:
        cursor.execute("BEGIN TRANSACTION;")
        insert_sql = """
        INSERT INTO blog_analysis.votes(id, postid, votetypeid, creationdate)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING;
        """
        cursor.executemany(insert_sql, votes)
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        logging.error(f"Error during batch insert: {e}")
        raise


def validate_vote(data):
    required_keys = {"Id", "PostId", "VoteTypeId", "CreationDate"}
    if not all(key in data for key in required_keys):
        return False
    try:
        int(data["Id"])
        int(data["PostId"])
        int(data["VoteTypeId"])
        # Assuming ISO 8601 date format
        datetime.datetime.fromisoformat(data["CreationDate"].replace("Z", ""))
    except ValueError:
        return False
    return True


def ingest_data(file_path, db_file, batch_size=10000):
    conn = None
    try:
        start_time = time.time()
        conn, cursor = setup_database(db_file)
        votes = []
        total_records = 0

        with open(file_path, "r") as file:
            for line in file:
                data = json.loads(line)
                if validate_vote(data):
                    vote = (
                        data["Id"],
                        data["PostId"],
                        data["VoteTypeId"],
                        data["CreationDate"],
                    )
                    votes.append(vote)
                    if len(votes) == batch_size:
                        insert_votes(cursor, votes)
                        total_records += len(votes)
                        votes = []

            if votes:
                insert_votes(cursor, votes)
                total_records += len(votes)

        conn.commit()
        end_time = time.time()
        logging.info(
            f"Data ingestion completed successfully. Total records ingested: {total_records}. Time taken: {end_time - start_time:.2f} seconds."
        )
    except Exception as e:
        logging.error(f"Error during data ingestion: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


def get_database_path():
    return os.getenv("DATABASE_PATH", "warehouse.db")


def get_data_file_path():
    return os.getenv("DATA_FILE_PATH", "uncommitted/votes.jsonl")


if __name__ == "__main__":
    db_file = get_database_path()
    vote_data_file = get_data_file_path()

    ingest_data(vote_data_file, db_file)
