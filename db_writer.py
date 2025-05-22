import psycopg2 # PostgreSQL adapter for Python
import logging
import datetime
# import numpy as np # Not strictly needed here, but good if you were to register adapters

logger = logging.getLogger(__name__)

# --- TimescaleDB Configuration (Consider moving to a shared config file or environment variables) ---
DB_HOST = "192.168.50.208"  # e.g., 192.168.50.208
DB_PORT = "5432"                         # Default PostgreSQL port
DB_NAME = "th_emg_processed_db"          # Database where your MDF table resides
DB_USER = "postgres"                 # e.g., postgres
DB_PASSWORD = "postgres"         # e.g., postgres
MDF_TABLE_NAME = "th_emg_mdf_values"     # The new table you'll create for MDF values

# Global connection object (can be managed more robustly, e.g., with a connection pool)
_connection = None
_cursor = None

def _get_db_connection():
    """Establishes or reuses a database connection."""
    global _connection, _cursor
    if _connection is None or _connection.closed != 0:
        try:
            _connection = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _connection.autocommit = True # Or manage transactions explicitly
            _cursor = _connection.cursor()
            logger.info(f"Successfully connected to TimescaleDB: {DB_NAME}")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to TimescaleDB: {e}")
            _connection = None
            _cursor = None
            return None, None
    return _connection, _cursor

def close_db_connection():
    """Closes the database connection if open."""
    global _connection, _cursor
    if _cursor:
        _cursor.close()
        _cursor = None
    if _connection and _connection.closed == 0:
        _connection.close()
        _connection = None
        logger.info("TimescaleDB connection closed.")

def save_mdf_value_to_db(thingid: str, muscle_name: str, timestamp_ms: int, mdf_value: float):
    """
    Saves a single MDF value to the TimescaleDB.
    Assumes a table 'th_emg_mdf_values' exists with columns:
    time TIMESTAMPTZ NOT NULL,
    thingid TEXT NOT NULL,
    muscle_name TEXT NOT NULL,
    mdf_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time, thingid, muscle_name)
    """
    connection, cursor = _get_db_connection()
    if not connection or not cursor:
        logger.error("Cannot save MDF to DB: No database connection.")
        return False

    try:
        dt_object = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
        sql_timestamp_str = dt_object.isoformat()
    except Exception as e:
        logger.error(f"Error converting timestamp_ms {timestamp_ms} to datetime: {e}")
        return False

    sql = f"""
        INSERT INTO {MDF_TABLE_NAME} (time, thingid, muscle_name, mdf_value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time, thingid, muscle_name) DO NOTHING;
    """

    try:
        python_native_float_mdf_value = float(mdf_value)

        cursor.execute(sql, (sql_timestamp_str, thingid, muscle_name, python_native_float_mdf_value))
        # logger.debug(f"Saved MDF to DB: {thingid}, {muscle_name}, {sql_timestamp_str}, {python_native_float_mdf_value}")
        return True
    except psycopg2.Error as e:
        logger.error(f"Error inserting MDF value into DB: {e}")
        # Consider reconnecting if it's a connection error
        if "connection" in str(e).lower():
             global _connection # Signal to re-establish connection on next call
             _connection = None
        return False
    except Exception as e_generic:
        logger.error(f"Generic error inserting MDF value into DB: {e_generic}")
        return False