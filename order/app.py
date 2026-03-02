import logging
import os
import atexit
import uuid
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, abort, Response
import requests

# Constants
DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ.get('GATEWAY_URL', 'http://gateway:80')

app = Flask("order-service")

# --- DATABASE SETUP ---
# We use a ThreadedConnectionPool for better performance with Gunicorn
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        host=os.environ['POSTGRES_HOST'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        port=os.environ.get('POSTGRES_PORT', 5432)
    )
except Exception as e:
    app.logger.error(f"Failed to connect to Postgres: {e}")
    exit(1)

def get_db_conn():
    return db_pool.getconn()

def release_db_conn(conn):
    db_pool.putconn(conn)

@atexit.register
def close_db_pool():
    db_pool.closeall()

# Initialize Tables (In a real app, use migrations like Alembic)
def init_db():
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id UUID PRIMARY KEY,
                user_id TEXT NOT NULL,
                paid BOOLEAN DEFAULT FALSE,
                total_cost BIGINT DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS order_items (
                order_id UUID REFERENCES orders(order_id) ON DELETE CASCADE,
                item_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                PRIMARY KEY (order_id, item_id)
            );
        """)
    conn.commit()
    release_db_conn(conn)

with app.app_context():
    init_db()

# --- HELPER FUNCTIONS ---

def send_request(method, url):
    try:
        res = requests.request(method, url)
        return res
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)

# --- ROUTES ---

@app.post('/create/<user_id>')
def create_order(user_id: str):
    order_id = str(uuid.uuid4())
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (order_id, user_id) VALUES (%s, %s)",
                (order_id, user_id)
            )
        conn.commit()
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)
    return jsonify({'order_id': order_id})

@app.get('/find/<order_id>')
def find_order(order_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get Order details
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                abort(404, "Order not found")
            
            # Get Items
            cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = [(row['item_id'], row['quantity']) for row in cur.fetchall()]
            
            order['items'] = items
            return jsonify(order)
    finally:
        release_db_conn(conn)

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    # 1. Verify item exists via Gateway
    item_reply = send_request("GET", f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, "Item not found")
    
    price = item_reply.json()['price']
    added_cost = int(quantity) * price

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Update or Insert item (Upsert)
            cur.execute("""
                INSERT INTO order_items (order_id, item_id, quantity) 
                VALUES (%s, %s, %s)
                ON CONFLICT (order_id, item_id) 
                DO UPDATE SET quantity = order_items.quantity + EXCLUDED.quantity
            """, (order_id, item_id, int(quantity)))
            
            # Update total cost
            cur.execute(
                "UPDATE orders SET total_cost = total_cost + %s WHERE order_id = %s",
                (added_cost, order_id)
            )
        conn.commit()
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)
    
    return Response("Item added", status=200)

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, total_cost, paid FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            
            if not order or order['paid']:
                abort(400, "Order invalid or already paid")

            cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
            items = cur.fetchall()

        # Logic for stock subtraction
        removed_items = []
        for item in items:
            res = send_request("POST", f"{GATEWAY_URL}/stock/subtract/{item['item_id']}/{item['quantity']}")
            if res.status_code != 200:
                # Rollback stock
                for r_id, r_qty in removed_items:
                    send_request("POST", f"{GATEWAY_URL}/stock/add/{r_id}/{r_qty}")
                abort(400, f"Out of stock: {item['item_id']}")
            removed_items.append((item['item_id'], item['quantity']))

        # Payment
        pay_res = send_request("POST", f"{GATEWAY_URL}/payment/pay/{order['user_id']}/{order['total_cost']}")
        if pay_res.status_code != 200:
            for r_id, r_qty in removed_items:
                send_request("POST", f"{GATEWAY_URL}/stock/add/{r_id}/{r_qty}")
            abort(400, "Insufficient funds")

        # Mark as paid
        with conn.cursor() as cur:
            cur.execute("UPDATE orders SET paid = TRUE WHERE order_id = %s", (order_id,))
        conn.commit()
        
        return Response("Checkout successful", status=200)
    finally:
        release_db_conn(conn)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)