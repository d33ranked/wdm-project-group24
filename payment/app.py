import uuid
from flask import Flask, jsonify, abort, Response
from psycopg2.extras import RealDictCursor
from database import get_db_conn, release_db_conn # Import shared logic

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

def init_db():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    credit BIGINT NOT NULL DEFAULT 0
                );
            """)
        conn.commit()
    finally:
        release_db_conn(conn)

with app.app_context():
    init_db()

# --- ROUTES ---

@app.post('/create_user')
def create_user():
    user_id = str(uuid.uuid4())
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (user_id, credit) VALUES (%s, 0)", (user_id,))
        conn.commit()
        return jsonify({'user_id': user_id})
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            data = [(str(i), int(starting_money)) for i in range(int(n))]
            cur.executemany("""
                INSERT INTO users (user_id, credit) VALUES (%s, %s) 
                ON CONFLICT (user_id) DO UPDATE SET credit = EXCLUDED.credit
            """, data)
        conn.commit()
        return jsonify({"msg": "Batch init for users successful"})
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id, credit FROM users WHERE user_id = %s", (user_id,))
            user = cur.fetchone()
            if not user:
                abort(404, f"User: {user_id} not found!")
            return jsonify(user)
    finally:
        release_db_conn(conn)

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET credit = credit + %s WHERE user_id = %s RETURNING credit",
                (int(amount), user_id)
            )
            result = cur.fetchone()
            if not result:
                abort(404, "User not found")
        conn.commit()
        return Response(f"User: {user_id} credit updated to: {result[0]}", status=200)
    except Exception:
        conn.rollback()
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    amount = int(amount)
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Atomic check-and-update to prevent negative balance
            cur.execute(
                "UPDATE users SET credit = credit - %s WHERE user_id = %s AND credit >= %s RETURNING credit",
                (amount, user_id, amount)
            )
            result = cur.fetchone()
            if not result:
                cur.execute("SELECT credit FROM users WHERE user_id = %s", (user_id,))
                user_exists = cur.fetchone()
                if not user_exists:
                    abort(404, "User not found")
                else:
                    abort(400, "Insufficient credit")
        conn.commit()
        return Response(f"User: {user_id} credit updated to: {result[0]}", status=200)
    except Exception as e:
        conn.rollback()
        if hasattr(e, 'code'): raise e 
        abort(400, DB_ERROR_STR)
    finally:
        release_db_conn(conn)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)