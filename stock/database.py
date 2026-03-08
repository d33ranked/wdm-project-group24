import os
import atexit
import psycopg2
from psycopg2 import pool

# Configuration from environment variables
DB_NAME = os.getenv('POSTGRES_DB', 'payment')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'postgres')
DB_HOST = os.getenv('POSTGRES_HOST', 'payment-db')

connection_pool = None
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        1, 100,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port="5432"
    )
except Exception as e:
    print(f"Error creating connection pool: {e}")

def get_db_conn():
    if connection_pool is None:
        init_pool()
    conn = connection_pool.getconn()
    conn.autocommit = False  # 2PC MUST have this False
    return conn

def release_db_conn(conn):
    if connection_pool:
        # Best practice: rollback any uncommitted junk before returning to pool
        try:
            conn.rollback() 
        except:
            pass
        connection_pool.putconn(conn)
@atexit.register
def close_db_pool():
    if connection_pool:
        connection_pool.closeall()