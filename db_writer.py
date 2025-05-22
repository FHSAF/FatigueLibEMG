# db_writer.py
import psycopg2
import logging
import datetime
# from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, MDF_TABLE_NAME
# Note: To make db_writer.py truly standalone or testable without importing main app's config,
# you might pass DB params to functions or initialize a DBManager class.
# For this structure, we'll assume it can access config if run in the same context,
# or we modify it to take params. Let's opt for passing config for better modularity.

logger = logging.getLogger(__name__)

_connection = None
_cursor = None

# Store DB config globally within this module, set by an init function
_DB_CONFIG = {}

def init_db_config(host, port, dbname, user, password, mdf_table_name):
    """Initializes DB configuration for this module."""
    global _DB_CONFIG
    _DB_CONFIG['host'] = host
    _DB_CONFIG['port'] = port
    _DB_CONFIG['dbname'] = dbname
    _DB_CONFIG['user'] = user
    _DB_CONFIG['password'] = password
    _DB_CONFIG['mdf_table_name'] = mdf_table_name
    logger.info("db_writer configuration initialized.")

def _get_db_connection():
    """Establishes or reuses a database connection."""
    global _connection, _cursor
    if not _DB_CONFIG:
        logger.error("DB configuration not initialized. Call init_db_config() first.")
        return None, None

    if _connection is None or _connection.closed != 0:
        try:
            _connection = psycopg2.connect(
                host=_DB_CONFIG['host'],
                port=_DB_CONFIG['port'],
                dbname=_DB_CONFIG['dbname'],
                user=_DB_CONFIG['user'],
                password=_DB_CONFIG['password']
            )
            _connection.autocommit = True # Or manage transactions explicitly
            _cursor = _connection.cursor()
            logger.info(f"Successfully connected to TimescaleDB: {_DB_CONFIG['dbname']}")
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
    """
    connection, cursor = _get_db_connection()
    if not connection or not cursor:
        logger.error("Cannot save MDF to DB: No database connection or DB config not set.")
        return False

    try:
        dt_object = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
        sql_timestamp_str = dt_object.isoformat()
    except Exception as e:
        logger.error(f"Error converting timestamp_ms {timestamp_ms} to datetime: {e}")
        return False

    sql = f"""
        INSERT INTO {_DB_CONFIG['mdf_table_name']} (time, thingid, muscle_name, mdf_value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time, thingid, muscle_name) DO NOTHING;
    """

    try:
        # Ensure mdf_value is a standard Python float for psycopg2
        python_native_float_mdf_value = float(mdf_value)
        cursor.execute(sql, (sql_timestamp_str, thingid, muscle_name, python_native_float_mdf_value))
        logger.debug(f"Saved MDF to DB: {thingid}, {muscle_name}, {sql_timestamp_str}, {python_native_float_mdf_value}")
        return True
    except psycopg2.Error as e:
        logger.error(f"Error inserting MDF value into DB: {e}")
        if "connection" in str(e).lower() or "closed" in str(e).lower():
             global _connection # Signal to re-establish connection on next call
             _connection = None
        return False
    except Exception as e_generic:
        logger.error(f"Generic error inserting MDF value into DB: {e_generic}")
        return False