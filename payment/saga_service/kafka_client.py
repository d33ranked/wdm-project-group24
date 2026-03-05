import os
import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import msgpack

from models import PaymentResponseFailure, PaymentResponseSuccess
from saga_service.db import db

kafka_producer: AIOKafkaProducer = None
kafka_consumer: AIOKafkaConsumer = None

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
        os.environ['TOPIC_PAYMENT_REQUEST'],
        os.environ['TOPIC_PAYMENT_ROLLBACK'],
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        group_id=os.environ['GROUP_PAYMENT'],
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

    # asyncio.ensure_future(consume_loop(), loop=event_loop)
    event_loop.create_task(consume_loop())


async def consume_loop():
    async for message in kafka_consumer:
        order_id = message.key
        topic = message.topic
        payload = message.value

        print(f"[PAYMENT] Received message on topic {topic} for order {order_id} with payload {payload}")
        if topic == os.environ['TOPIC_PAYMENT_REQUEST']:
            account_status = db.get(payload['user_id'])
            if payload['amount'] >= 0 and account_status.credit >= payload['amount']:
                response = PaymentResponseSuccess(
                    order_id=order_id,
                    user_id=payload['user_id'],
                    amount_subtracted=payload['amount'],
                    old_amount=account_status.credit,
                    new_amount=account_status.credit - payload['amount'],
                )
                db.update(payload['user_id'], -payload['amount'])
                await _send_payment_success(response)
            else:
                response = PaymentResponseFailure(
                    order_id=order_id,
                    user_id=payload['user_id'],
                    amount_account=account_status.credit,
                    msg="Insufficient credit",
                )
                await _send_payment_failure(response)
        
        if topic == os.environ['TOPIC_PAYMENT_ROLLBACK']:
            account_status = db.get(payload['user_id'])
            response = PaymentResponseSuccess(
                order_id=order_id,
                user_id=payload['user_id'],
                amount_subtracted=-payload['amount'],
                old_amount=account_status.credit,
                new_amount=account_status.credit + payload['amount'],
            )
            db.update(payload['user_id'], payload['amount'] + account_status.credit)
            await _send_payment_success(response)

        else:
            print(f"Received message on unknown topic: {topic}")
            continue

async def _send_payment_success(response: PaymentResponseSuccess) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_PAYMENT_RESPONSE_SUCCESS'],
        key=response.order_id,
        value=response,
    )

async def _send_payment_failure(response: PaymentResponseFailure) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_PAYMENT_RESPONSE_FAILURE'],
        key=response.order_id,
        value=response,
    )
