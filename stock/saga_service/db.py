import os
import redis

from flask import abort
from msgspec import msgpack

from models import StockValue

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

    
def get_item(item_id: str) -> StockValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    return msgpack.decode(entry, type=StockValue) if entry else None
