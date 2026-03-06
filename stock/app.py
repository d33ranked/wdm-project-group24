import logging
import os
import atexit
import uuid
import asyncio
import threading
import redis

from time import perf_counter
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, g

from saga_service import kafka_client
from models import StockValue
from saga_service.db import db, wait_for_redis, DB_ERROR_STR

app = Flask("stock-service")


@app.before_request
def start_timer():
    g.start_time = perf_counter()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"STOCK: Request took {duration:.7f} seconds")
    return response


@app.get("/health")
def health_check():
    """Liveness probe - is the service running?"""
    return jsonify({"status": "ok"})


@app.get("/ready")
def readiness_check():
    """Readiness probe - is the service ready to accept traffic?"""
    try:
        db.ping()
    except redis.exceptions.RedisError:
        return jsonify({"status": "not ready", "reason": "redis not connected"}), 503

    if kafka_client.kafka_producer is None or kafka_client.kafka_consumer is None:
        return jsonify({"status": "not ready", "reason": "kafka not connected"}), 503

    return jsonify({"status": "ready"})


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    print(f"Creating item with price: {price}")
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    print(f"Item: {key} created with price: {price}")
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


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
print(f"[STOCK] loop is running: {kafka_client.loop.is_running()}")

print("[STOCK] Waiting for Redis...")
try:
    wait_for_redis()
    print("[STOCK] Redis connected successfully")
except Exception as e:
    print(f"[STOCK] Redis connection FAILED: {e}")
    raise

print("[STOCK] Starting Kafka...")
try:
    asyncio.run_coroutine_threadsafe(
        kafka_client._start_kafka(kafka_client.loop), kafka_client.loop
    ).result(timeout=30)
    print("[STOCK] Kafka started successfully")
except Exception as e:
    print(f"[STOCK] Kafka startup FAILED: {e}")
    raise

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
