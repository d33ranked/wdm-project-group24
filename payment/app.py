import logging
import os
import atexit
import uuid
import redis
import asyncio
import threading

from models import UserValue
import saga_service.kafka_client as kafka_client
from saga_service.db import db, wait_for_redis, get_user_from_db

from msgspec import msgpack 
from flask import Flask, jsonify, abort, Response, g
from time import perf_counter

DB_ERROR_STR = "DB error"
app = Flask("payment-service")


@app.before_request
def start_timer():
    g.start_time = perf_counter()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"PAYMENT: Request took {duration:.7f} seconds")
    return response


@app.get('/health')
def health_check():
    """Liveness probe - is the service running?"""
    return jsonify({"status": "ok"})


@app.get('/ready')
def readiness_check():
    """Readiness probe - is the service ready to accept traffic?"""
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return jsonify({"status": "not ready", "reason": "redis not connected"}), 503

    if kafka_client.kafka_producer is None or kafka_client.kafka_consumer is None:
        return jsonify({"status": "not ready", "reason": "kafka not connected"}), 503

    return jsonify({"status": "ready"})


@app.post('/create_user')
def create_user():
    print(f"Creating user")
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

# Single persistent event loop in a background thread
kafka_client.loop = asyncio.new_event_loop()


def start_loop(lp):
    asyncio.set_event_loop(lp)
    lp.run_forever()


loop_thread = threading.Thread(target=start_loop, args=(kafka_client.loop,), daemon=True)
loop_thread.start()
print(f"[PAYMENT] loop is running: {kafka_client.loop.is_running()}")

print("[PAYMENT] Waiting for Redis...")
try:
    wait_for_redis()
    print("[PAYMENT] Redis connected successfully")
except Exception as e:
    print(f"[PAYMENT] Redis connection FAILED: {e}")
    raise

print("[PAYMENT] Starting Kafka...")
try:
    asyncio.run_coroutine_threadsafe(
        kafka_client._start_kafka(kafka_client.loop), kafka_client.loop
    ).result(timeout=30)
    print("[PAYMENT] Kafka started successfully")
except Exception as e:
    print(f"[PAYMENT] Kafka startup FAILED: {e}")
    raise

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
