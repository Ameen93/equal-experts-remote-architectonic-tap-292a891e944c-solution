import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_connection(db_file):
    """
    Create a database connection.

    Args:
        db_file (str): Path to the database file.

    Returns:
        conn: Database connection object.
    """
    try:
        conn = duckdb.connect(database=db_file, read_only=False)
        return conn
    except Exception as e:
        logging.error(f"Error creating database connection: {e}")
        return None


def list_tables(conn):
    try:
        result = conn.execute("SHOW TABLES;").fetchall()
        logging.info("Existing tables:")
        for row in result:
            logging.info(row)
    except Exception as e:
        logging.error(f"Error listing tables: {e}")


def check_votes_table(conn):
    try:
        result = conn.execute("SELECT * FROM blog_analysis.votes LIMIT 1;")
        logging.info("Table 'blog_analysis.votes' exists and is accessible.")
    except Exception as e:
        logging.error(
            f"Table 'blog_analysis.votes' does not exist or is not accessible: {e}"
        )
        raise


def calculate_outlier_weeks(conn):
    """
    Calculate outlier weeks and create a view.

    Args:
        conn: Database connection object.
    """
    try:
        check_votes_table(conn)

        create_view_query = """
        CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
        WITH weekly_votes AS (
            SELECT
                strftime('%Y', creationdate) AS year,
                strftime('%W', creationdate) AS week,
                COUNT(*) AS vote_count
            FROM
                blog_analysis.votes
            GROUP BY
                year, week
        ),
        avg_votes AS (
            SELECT AVG(vote_count) AS avg_vote_count FROM weekly_votes
        )
        SELECT
            year,
            week AS week_number,
            vote_count
        FROM
            weekly_votes,
            avg_votes
        WHERE
            ABS(1.0 - vote_count / avg_votes.avg_vote_count) > 0.2
        ORDER BY
            year, week_number;
        """
        conn.execute(create_view_query)
        logging.info("View 'outlier_weeks' created successfully.")

        select_query = "SELECT * FROM blog_analysis.outlier_weeks;"
        result = conn.execute(select_query).fetchall()

        logging.info("Outlier weeks:")
        for row in result:
            print(row)

    except Exception as e:
        logging.error(f"Error calculating outlier weeks: {e}")


if __name__ == "__main__":
    db_file = "warehouse.db"
    conn = create_connection(db_file)

    if conn:
        list_tables(conn)
        calculate_outlier_weeks(conn)
        conn.close()
        logging.info("Database connection closed.")
    else:
        logging.error("Failed to create database connection.")
