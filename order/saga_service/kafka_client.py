from nt import environ
import os
import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import msgpack

from models import BatchItemRequest, FindStockRequest, PaymentRequest
from db import db

kafka_producer: AIOKafkaProducer = None
kafka_consumer: AIOKafkaConsumer = None

# order_id -> future (resolved with True if successful, exception if failed)
pending_sagas: dict[str, asyncio.Future] = {}

# Set at startup in app.py
loop: asyncio.AbstractEventLoop = None

async def _start_kafka(event_loop: asyncio.AbstractEventLoop):
    global kafka_producer, kafka_consumer

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        value_serializer=lambda v: msgpack.encode(v),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
    )

    kafka_consumer = AIOKafkaConsumer(
        os.environ['TOPIC_STOCK_RESPONSE_SUCCESS'],
        os.environ['TOPIC_STOCK_RESPONSE_FAILURE'],
        os.environ['TOPIC_PAYMENT_RESPONSE_SUCCESS'],
        os.environ['TOPIC_PAYMENT_RESPONSE_FAILURE'],
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        group_id=os.environ['GROUP_ORDER'],
        value_deserializer=lambda v: msgpack.decode(v),
        key_deserializer=lambda k: k.decode('utf-8'),
    )

    for attempt in range(10):
        try:
            await kafka_producer.start()
            await kafka_consumer.start()
            break
        except Exception as e:
            print(f"Kafka not ready (attempt {attempt+1}): {e}")
            await asyncio.sleep(3)
    else:
        raise RuntimeError("Could not connect to Kafka after 10 attempts")

    asyncio.ensure_future(consume_loop(), loop=event_loop)


async def consume_loop():
    async for message in kafka_consumer:
        order_id = message.key
        topic = message.topic
        payload = message.value

        if topic == os.environ['TOPIC_STOCK_RESPONSE_SUCCESS']:
            current_checkout_status = db.get(f"checkout_status:{order_id}")
            if current_checkout_status is None:
                print(f"[ORDER] Received stock response for unknown order {order_id}, skipping")
                continue
            
            current_checkout_status['stock_success'] = True
            db.set(f"checkout_status:{order_id}", current_checkout_status)
            _maybe_resolve_saga(order_id, current_checkout_status)

        elif topic == os.environ['TOPIC_PAYMENT_RESPONSE_SUCCESS']:
            current_checkout_status = db.get(f"checkout_status:{order_id}")
            if current_checkout_status is None:
                print(f"[ORDER] Received payment response for unknown order {order_id}, skipping")
                continue
            
            current_checkout_status['payment_success'] = True
            db.set(f"checkout_status:{order_id}", current_checkout_status)
            _maybe_resolve_saga(order_id, current_checkout_status)

        elif topic == os.environ['TOPIC_STOCK_RESPONSE_FAILURE']:
            current_checkout_status = db.get(f"checkout_status:{order_id}")
            if current_checkout_status is None:
                print(f"[ORDER] Received stock failure for unknown order {order_id}, skipping")
                continue
            
            if current_checkout_status['payment_success']:
                print(f"[ORDER] Stock failed for order {order_id} but payment succeeded, initiating compensation")
                await _send_payment_rollback(PaymentRequest(user_id=payload['user_id'], amount=payload['amount']))

            _fail_saga(order_id, "Stock reservation failed")

        elif topic == os.environ['TOPIC_PAYMENT_RESPONSE_FAILURE']:
            current_checkout_status = db.get(f"checkout_status:{order_id}")
            if current_checkout_status is None:
                print(f"[ORDER] Received payment failure for unknown order {order_id}, skipping")
                continue
            
            if current_checkout_status['stock_success']:
                print(f"[ORDER] Payment failed for order {order_id} but stock succeeded, initiating compensation")
                await _send_stock_rollback(BatchItemRequest(order_id=order_id, items=payload['items']))
            
            _fail_saga(order_id, "Payment processing failed")

        else:
            print(f"Received message on unknown topic: {topic}")
            continue

def _maybe_resolve_saga(order_id: str, status: dict):
    """Resolve the saga future with success once both stock and payment have succeeded."""
    if status.get('stock_success') and status.get('payment_success'):
        db.delete(f"checkout_status:{order_id}")
        future = pending_sagas.pop(order_id, None)
        if future and not future.done():
            future.set_result(True)


def _fail_saga(order_id: str, reason: str):
    """Resolve the saga future with an exception (failure path)."""
    future = pending_sagas.pop(order_id, None)
    db.delete(f"checkout_status:{order_id}")

    if future and not future.done():
        future.set_exception(Exception(reason))


async def _send_payment_request(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_PAYMENT_REQUEST'],
        key=request.order_id,
        value=request,
    )

async def _send_stock_request(request: BatchItemRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_REQUEST'],
        key=request.order_id,
        value=request,
    )

async def _send_payment_rollback(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_PAYEMENT_ROLLBACK'],
        key=request.order_id,
        value=request,
    )

async def _send_stock_rollback(request: BatchItemRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_ROLLBACK'],
        key=request.order_id,
        value=request,
    )
