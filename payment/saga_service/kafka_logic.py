import os
import kafka
import asyncio

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from order.saga_service.models import OrderValue, BatchStockRequest, PaymentRequest
from db import db

from msgspec import msgpack

kafka_producer: AIOKafkaProducer = None
kafka_consumer: AIOKafkaConsumer = None

async def _start_kafka(event_loop: asyncio.AbstractEventLoop):
    global kafka_producer, kafka_consumer

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['PAYMENT_SERVICE_PRODUCER'],
        value_serializer=lambda v: msgpack.encode(v),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
    )

    kafka_consumer = AIOKafkaConsumer(
        os.environ['TOPIC_REQUEST_PAYMENT'],
        os.environ['TOPIC_ROLLBACK_PAYMENT'],
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['PAYMENT_SERVICE_CONSUMER'],
        group_id=os.environ['GROUP_PAYMENT'],
        value_deserializer=lambda v: msgpack.decode(v),
        key_deserializer=lambda k: k.decode('utf-8'),
    )

    await kafka_producer.start()
    await kafka_consumer.start()
    asyncio.ensure_future(consume_loop(), loop=event_loop)


async def consume_loop():
    async for message in kafka_consumer:
        user_id = message.key
        topic = message.topic
        payload = message.value

        success = payload.get('success', False) if isinstance(payload, dict) else getattr(payload, 'success', False)

        if topic == os.environ['TOPIC_REQUEST_PAYMENT']:
            pass
            # TODO here request the payment
        elif topic == os.environ['TOPIC_ROLLBACK_PAYMENT']:
            pass
            # TODO here rollback the payment: I need more info than just user_id!

def remove_credit(user_id: str, amount: int):


async def complete_payment(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_REQUEST_PAYMENT'],
        key=request.user_id,
        value=request,
        headers=[('idempotency_key', request.user_id.encode('utf-8'))]
    )

async def fail_payment(request: PaymentRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_REQUEST_PAYMENT'],
        key=request.user_id,
        value=request,
        headers=[('idempotency_key', request.user_id.encode('utf-8'))]
    )
