from msgspec import msgpack

from order.saga_service.models import PaymentRequest

from payment.saga_service.models import UserValue

from payment.saga_service.kafka_logic import register_payment_handlers
from payment.saga_service.db import db, get_user_from_db

import redis

async def handle_payment_request(request: PaymentRequest) -> bool:
    user_id = request.user_id
    amount = request.amount

    user_entry: UserValue = get_user_from_db(user_id)

    if not user_entry:
        return False

    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        return False #TODO maybe change this since we are losing information: what went wrong?
        # abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        # return abort(400, DB_ERROR_STR)
        return False

    # return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)
    return True

async def handle_payment_rollback(request: PaymentRequest) -> None:
    user_id = request.user_id
    amount = request.amount

    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry:
        user_entry.credit += amount
        db.set(user_id, msgpack.encode(user_entry))

    # TODO what if this fails?


register_payment_handlers(handle_payment_request, handle_payment_rollback)
