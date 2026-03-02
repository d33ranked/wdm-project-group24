from msgspec import msgpack

from order.saga_service.models import BatchStockRequest

from stock.saga_service.models import StockValue

from stock.saga_service.kafka_logic import register_stock_handlers
from stock.saga_service.db import db, get_item_from_db

import redis

async def handle_stock_request(request: BatchStockRequest) -> bool:
    for item in request.items:
        item_id = item.item_id
        amount = item.quantity

        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database
        item_entry.stock -= int(amount)

        if item_entry.stock < 0:
            # abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
            return False
        try:
            db.set(item_id, msgpack.encode(item_entry))
        except redis.exceptions.RedisError:
            # return abort(400, DB_ERROR_STR)
            return False

    # return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
    return True

async def handle_stock_rollback(request: BatchStockRequest) -> bool:
    for item in request.items:
        item_id = item.item_id
        amount = item.quantity

        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database
        item_entry.stock += int(amount)
        try:
            db.set(item_id, msgpack.encode(item_entry))
        except redis.exceptions.RedisError:
            # return abort(400, DB_ERROR_STR)
            return False

    # return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
    return True


register_stock_handlers(handle_stock_request, handle_stock_rollback)
