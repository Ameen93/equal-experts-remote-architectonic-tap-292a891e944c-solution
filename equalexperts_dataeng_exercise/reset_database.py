import duckdb
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Function to create database connection
def create_connection(db_file):
    try:
        conn = duckdb.connect(database=str(db_file), read_only=False)
        return conn
    except Exception as e:
        logging.error(f"Error creating database connection: {e}")
        raise


# Function to drop the schema and all tables
def reset_database(conn):
    try:
        cursor = conn.cursor()
        # Drop schema which will drop all tables in the schema
        cursor.execute("DROP SCHEMA IF EXISTS blog_analysis CASCADE;")
        logging.info("Schema 'blog_analysis' and all its tables have been dropped.")
        conn.commit()
    except Exception as e:
        logging.error(f"Error dropping schema: {e}")
        if conn:
            conn.rollback()
        raise


if __name__ == "__main__":
    db_file = os.getenv("DATABASE_PATH", "warehouse.db")

    try:
        conn = create_connection(db_file)
        reset_database(conn)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")
