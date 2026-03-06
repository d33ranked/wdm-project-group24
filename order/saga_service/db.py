import os
import uuid
import redis

from models import OrderCheckoutStatus, OrderValue

from flask import abort
from msgspec import msgpack

DB_ERROR_STR = "DB error"

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


def wait_for_redis(max_attempts: int = 10, delay: float = 3.0) -> None:
    """Wait for Redis to be ready at startup."""
    for attempt in range(max_attempts):
        try:
            db.ping()
            return
        except redis.exceptions.ConnectionError as e:
            print(f"Redis not ready (attempt {attempt + 1}/{max_attempts}): {e}")
            import time

            time.sleep(delay)
    raise RuntimeError("Could not connect to Redis after {max_attempts} attempts")

def get_checkout_status(order_id: str) -> OrderCheckoutStatus | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    return msgpack.decode(entry, type=OrderCheckoutStatus) if entry else None

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