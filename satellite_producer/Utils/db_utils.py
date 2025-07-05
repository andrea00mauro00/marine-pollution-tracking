
# Utilities
import psycopg2

def connect_to_db() -> psycopg2.extensions.connection:
    """
    Establishes and returns a connection to the PostgreSQL database.

    Returns:
        conn (psycopg2.extensions.connection): A connection object to the 'california_db' database 
        using user 'gruppo3' on localhost at port 5433.
    """
    conn = psycopg2.connect(
        dbname="california_db",
        user="gruppo3",
        password="gruppo3",
        host="postgres",
        port=5432
    )

    return conn