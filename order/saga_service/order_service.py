import asyncio
import kafka
import redis
import uuid

from flask import abort
from msgspec import msgpack

from saga_service.db import db
from models import BatchItemRequest, OrderCheckoutStatus, OrderValue, PaymentRequest
import saga_service.kafka_client as kafka_client

DB_ERROR_STR = "DB error"


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


def create_order(user_id: str) -> str:
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items={}, user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return key

async def saga_checkout(order_id: str) -> bool:
    order_entry = get_order_from_db(order_id)
    if not order_entry:
        raise Exception(f"Order {order_id} not found")

    if order_entry.paid:
        raise Exception(f"Order {order_id} has already been paid")

    if order_entry.total_cost == 0 or not order_entry.items or len(order_entry.items) == 0:
        raise Exception(f"Order {order_id} has no items")

    assert kafka_client.loop is not None, "Event loop is not initialized"

    print(f"Starting checkout for order {order_id} with total cost {order_entry.total_cost}")
    future = kafka_client.loop.create_future()
    kafka_client.pending_sagas[order_id] = future
    saga = OrderCheckoutStatus(order_id=order_id) 
    db.set(f"checkout_status:{order_id}", msgpack.encode(saga))

    checkout_stock_request = BatchItemRequest.from_order_value(order_id, order_entry)
    checkout_payment_request = PaymentRequest.from_order_value(order_id, order_entry)

    await kafka_client._send_stock_request(checkout_stock_request)
    await kafka_client._send_payment_request(checkout_payment_request)
    print(f"Sent stock and payment requests for order {order_id}, waiting for responses...")
    await future

    order_entry.paid = True
    db.set(order_id, msgpack.encode(order_entry))

    return True
