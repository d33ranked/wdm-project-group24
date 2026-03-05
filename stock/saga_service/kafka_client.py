import os
import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import msgpack

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
        os.environ['TOPIC_STOCK_REQUEST'],
        os.environ['TOPIC_STOCK_ROLLBACK'],
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

    # asyncio.ensure_future(consume_loop(), loop=event_loop)
    event_loop.create_task(consume_loop())


async def consume_loop():
    async for message in kafka_consumer:
        order_id = message.key
        topic = message.topic
        payload = message.value

        print(f"[STOCK] Received message on topic {topic} for order {order_id} with payload {payload}")
        if topic == os.environ['TOPIC_STOCK_REQUEST']:
            print(f"Received stock request for order {order_id}: {payload}")

            # Check if we have enough stock
            to_update = payload["items"]
            async with db.transaction() as transaction:
                for (item, amount) in to_update:
                    stock_value = await db.fetchrow("SELECT stock, price FROM stock WHERE id=$1", item)
                    if stock_value is None:
                        await _send_stock_failure(order_id, f"Item {item} not found")
                        print(f"Item {item} not found for order {order_id}")
                        transaction.rollback()
                        return
                    elif stock_value.stock < amount:
                        await _send_stock_failure(order_id, f"Not enough stock for item {item}")
                        print(f"Not enough stock for item {item} in order {order_id}")
                        transaction.rollback()
                        return
                    else:
                        await db.execute("UPDATE stock SET stock = stock - $1 WHERE id=$2", amount, item)
                        
            await _send_stock_success(order_id)

        if topic == os.environ['TOPIC_STOCK_ROLLBACK']:
            print(f"Received stock rollback for order {order_id}: {payload}")

            to_update = payload["items"]
            async with db.transaction() as transaction:
                for (item, amount) in to_update:
                    await db.execute("UPDATE stock SET stock = stock + $1 WHERE id=$2", amount, item)

        else:
            print(f"Received message on unknown topic: {topic}")
            continue

async def _send_stock_success(order_id) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_RESPONSE_SUCCESS'],
        key=order_id,
    )

async def _send_stock_failure(order_id, reason) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_RESPONSE_FAILURE'],
        key=order_id,
        value=reason,
    )
