import os
import uuid
import redis
import requests
import logging
import asyncio
import threading

from msgspec import msgpack
from flask import Flask, jsonify, abort, Response
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from order.saga_service.models import OrderValue, BatchStockRequest, PaymentRequest
from db import get_order_from_db, db, DB_ERROR_STR
from checkout_service import _checkout
from kafka_logic import _start_kafka


REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
app = Flask("order-service")

pending_sagas: dict[str, dict] = {}

# A single persistent event loop running in a background thread
loop = asyncio.new_event_loop()

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

loop_thread = threading.Thread(target=start_loop, args=(loop,), daemon=True)
loop_thread.start()

# Start kafka when the app starts
asyncio.run_coroutine_threadsafe(_start_kafka(loop), loop).result()


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")

    # Bridge from sync Flask handler into the async event loop
    future = asyncio.run_coroutine_threadsafe(_checkout(order_id), loop)
    return future.result()

def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})

@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
