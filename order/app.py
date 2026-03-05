import os
import asyncio
import atexit
import logging
import threading
import redis
import requests

from flask import Flask, jsonify, abort, g, Response
from time import perf_counter
from msgspec import msgpack


import saga_service.kafka_client as kafka_client
from saga_service.db import db, wait_for_redis
from saga_service.order_service import create_order, get_order_from_db, saga_checkout

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ["GATEWAY_URL"]
app = Flask("order-service")


@app.before_request
def start_timer():
    g.start_time = perf_counter()


@app.after_request
def log_response(response):
    duration = perf_counter() - g.start_time
    print(f"ORDER: Request took {duration:.7f} seconds")
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


@app.post('/create/<user_id>')
def create_order_endpoint(user_id: str):
    print(f"Received request to create order for user {user_id}")
    key = create_order(user_id)
    return jsonify({"order_id": key})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry = get_order_from_db(order_id)
    if not order_entry:
        return abort(400, f"Order: {order_id} not found!")

    return jsonify({
            'order_id': order_id, 
            'user_id': order_entry.user_id, 
            'items': order_entry.items, 
            'total_cost': order_entry.total_cost, 
            'paid': order_entry.paid
        })


def send_get_request(url: str):
    try:
        start_time = perf_counter()
        response = requests.get(url)
        duration = perf_counter() - start_time
        print(f"ORDER: GET request took {duration:.7f} seconds")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    current = order_entry.items.get(item_id, 0)
    order_entry.items[item_id] = current + int(quantity)
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    print(f"Received request to checkout order {order_id}")
    future = asyncio.run_coroutine_threadsafe(
        saga_checkout(order_id),
        kafka_client.loop
    )

    try:
        future.result(timeout=15)
    except Exception as e:
        return abort(400, str(e))
    return jsonify({'status': 'success'})


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

print("[ORDER] Waiting for Redis...")
try:
    wait_for_redis()
    print("[ORDER] Redis connected successfully")
except Exception as e:
    print(f"[ORDER] Redis connection FAILED: {e}")
    raise

print("[ORDER] Starting Kafka...")
try:
    asyncio.run_coroutine_threadsafe(
        kafka_client._start_kafka(kafka_client.loop), kafka_client.loop
    ).result(timeout=30)
    print("[ORDER] Kafka started successfully")
except Exception as e:
    print(f"[ORDER] Kafka startup FAILED: {e}")
    raise


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
