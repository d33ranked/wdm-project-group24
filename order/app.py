import logging
import os
import atexit
import uuid
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import TRANSACTION_STATUS_IDLE
from flask import Flask, jsonify, abort, Response, g
from time import perf_counter
import requests
import random
import json

FLAG_2PC = os.environ.get('ENABLE_2PC', "false") == "true"
FLAG_gRPC = os.environ.get('ENABLE_GRPC', "false") == "true"

if FLAG_2PC or FLAG_gRPC:
    import grpc
    import transaction_pb2
    import transaction_pb2_grpc



DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ.get('GATEWAY_URL', 'http://gateway:80')

app = Flask("order-service")

# --- DATABASE SETUP ---
# We use a ThreadedConnectionPool for better performance with Gunicorn
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=10,
        maxconn=200,
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

@app.before_request
def before_req():
    g.start_time = perf_counter()
    g.conn = db_pool.getconn()

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


import uuid
import random
from flask import g, jsonify

@app.post("/batch_init/<int:n>/<int:n_items>/<int:n_users>/<int:item_price>")
def batch_init_orders(n, n_items, n_users, item_price):
    try:
        cur = g.conn.cursor()

        for _ in range(n):
            # 1. Generate a proper UUID
            order_id = str(uuid.uuid4())
            user_id = str(random.randint(0, n_users - 1))
            total_cost = 2 * item_price
            
            # 2. Insert into 'orders' table
            cur.execute(
                """
                INSERT INTO orders (order_id, user_id, paid, total_cost) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (order_id, user_id, False, total_cost)
            )

            # 3. Insert 2 random items into 'order_items' table
            for _ in range(2):
                item_id = str(random.randint(0, n_items - 1))
                cur.execute(
                    """
                    INSERT INTO order_items (order_id, item_id, quantity) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (order_id, item_id) DO UPDATE 
                    SET quantity = order_items.quantity + EXCLUDED.quantity
                    """,
                    (order_id, item_id, 1)
                )

        g.conn.commit()
        cur.close()
        return jsonify({"msg": f"Successfully initialized {n} orders"}), 200

    except Exception as e:
        if hasattr(g, 'conn'):
            g.conn.rollback()
        app.logger.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

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

# Only define the safe_checkout function if 2PC is enabled
if FLAG_2PC:
    def get_stub(service_name):
        channel = grpc.insecure_channel(f"{service_name}:50051")
        return transaction_pb2_grpc.TransactionParticipantStub(channel)

    def finalize_local_prepared(tx_id: str, commit: bool):
        local_conn = get_db_conn()
        try:
            if local_conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                local_conn.rollback()
            local_conn.autocommit = True
            with local_conn.cursor() as cur:
                if commit:
                    cur.execute("COMMIT PREPARED %s", (tx_id,))
                else:
                    cur.execute("ROLLBACK PREPARED %s", (tx_id,))
        finally:
            try:
                local_conn.autocommit = False
            except Exception:
                pass
            release_db_conn(local_conn)

    @app.post('/2pc/checkout/<order_id>')
    def safe_checkout(order_id: str):
        conn = get_db_conn()
        tx_id = f"tx_{order_id}_{uuid.uuid4().hex}"
        stubs = {
            "stock": get_stub("stock-service"),
            "payment": get_stub("payment-service"),
        }
        prepared_order = False
        prepared_stock = False
        prepared_payment = False

        try:
            if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                conn.rollback()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT user_id, total_cost, paid FROM orders WHERE order_id = %s", (order_id,))
                order = cur.fetchone()
                if not order or order['paid']:
                    abort(400, "Invalid order or already paid")

                cur.execute("SELECT item_id, quantity FROM order_items WHERE order_id = %s", (order_id,))
                items = cur.fetchall()

            grpc_items = [
                transaction_pb2.OrderItem(item_id=item['item_id'], quantity=item['quantity'])
                for item in items
            ]
            prepare_req = transaction_pb2.PrepareRequest(
                transaction_id=tx_id,
                user_id=order['user_id'],
                order_id=order_id,
                total_cost=order['total_cost'],
                items=grpc_items,
            )

            stock_prepare = stubs["stock"].Prepare(prepare_req)
            prepared_stock = stock_prepare.success
            if not prepared_stock:
                raise Exception(stock_prepare.message or "Stock prepare failed")

            payment_prepare = stubs["payment"].Prepare(prepare_req)
            prepared_payment = payment_prepare.success
            if not prepared_payment:
                raise Exception(payment_prepare.message or "Payment prepare failed")

            with conn.cursor() as cur:
                cur.execute("UPDATE orders SET paid = TRUE WHERE order_id = %s", (order_id,))
                cur.execute("PREPARE TRANSACTION %s", (tx_id,))
            prepared_order = True

            stubs["stock"].Commit(transaction_pb2.CommitRequest(transaction_id=tx_id))
            stubs["payment"].Commit(transaction_pb2.CommitRequest(transaction_id=tx_id))
            finalize_local_prepared(tx_id, commit=True)

            return Response("Checkout successful", status=200)

        except Exception as exc:
            if prepared_stock:
                try:
                    stubs["stock"].Rollback(transaction_pb2.RollbackRequest(transaction_id=tx_id))
                except Exception:
                    pass

            if prepared_payment:
                try:
                    stubs["payment"].Rollback(transaction_pb2.RollbackRequest(transaction_id=tx_id))
                except Exception:
                    pass

            if prepared_order:
                try:
                    finalize_local_prepared(tx_id, commit=False)
                except Exception:
                    pass
            else:
                try:
                    if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                        conn.rollback()
                except Exception:
                    pass

            abort(400, f"Checkout failed: {str(exc)}")
        finally:
            release_db_conn(conn)




if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)




