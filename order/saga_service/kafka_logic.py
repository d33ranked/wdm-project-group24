import os
import kafka
import asyncio
import threading

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from order.saga_service.models import OrderValue, BatchStockRequest, PaymentRequest
from db import get_order_from_db, db

from msgspec import msgpack

kafka_producer: AIOKafkaProducer = None
kafka_consumer: AIOKafkaConsumer = None

async def _start_kafka(event_loop: asyncio.AbstractEventLoop):
    global kafka_producer, kafka_consumer

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['ORDER_SERVICE_PRODUCER'],
        value_serializer=lambda v: msgpack.encode(v),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
    )

    kafka_consumer = AIOKafkaConsumer(
        os.environ['TOPIC_STOCK_RESERVED'],
        os.environ['TOPIC_PAYMENT_COMPLETED'], #TODO shouldn't you also listen to payment_failed? Same for stock
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['ORDER_SERVICE_CONSUMER'],
        group_id=os.environ['GROUP_ORDER'],
        value_deserializer=lambda v: msgpack.decode(v),
        key_deserializer=lambda k: k.decode('utf-8'),
    )

    await kafka_producer.start()
    await kafka_consumer.start()
    asyncio.ensure_future(consume_loop(), loop=event_loop)


async def consume_loop():
    async for message in kafka_consumer:
        order_id = message.key
        topic = message.topic
        payload = message.value

        success = payload.get('success', False) if isinstance(payload, dict) else getattr(payload, 'success', False)

        saga = get_order_from_db(order_id)
        if saga is None:
            continue

        if topic == os.environ['TOPIC_STOCK_RESERVED']:
            saga['stock_ok'] = success
            saga['stock_event'].set()
        elif topic == os.environ['TOPIC_PAYMENT_COMPLETED']:
            saga['payment_ok'] = success
            saga['payment_event'].set()


async def request_stock(request: BatchStockRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_REQUEST_STOCK'],
        key=request.order_id,
        value=request,
        headers=[('idempotency_key', request.order_id.encode('utf-8'))]
    )

async def request_payment(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_REQUEST_PAYMENT'],
        key=request.user_id,
        value=request,
        headers=[('idempotency_key', request.user_id.encode('utf-8'))]
    )

async def rollback_stock(order_id: str) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_ROLLBACK_STOCK'],
        key=order_id,
        value={'order_id': order_id},
        headers=[('idempotency_key', order_id.encode('utf-8'))]
    )

async def rollback_payment(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_ROLLBACK_PAYMENT'],
        key=request,
        value=request,
        headers=[('idempotency_key', request.user_id.encode('utf-8'))]
    )