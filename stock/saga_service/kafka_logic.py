import os
import kafka
import asyncio

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from order.saga_service.models import BatchStockRequest

from msgspec import msgpack

from typing import Callable, Awaitable, Optional

StockCallback = Callable[[BatchStockRequest], Awaitable[None]]

_on_stock_request: Optional[StockCallback] = None
_on_stock_rollback: Optional[StockCallback] = None


def register_stock_handlers(
    on_request: StockCallback,
    on_rollback: StockCallback,
) -> None:
    global _on_stock_request, _on_stock_rollback
    _on_stock_request = on_request
    _on_stock_rollback = on_rollback


kafka_producer: AIOKafkaProducer = None
kafka_consumer: AIOKafkaConsumer = None

async def _start_kafka(event_loop: asyncio.AbstractEventLoop):
    global kafka_producer, kafka_consumer

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['STOCK_SERVICE_PRODUCER'],
        value_serializer=lambda v: msgpack.encode(v),
        key_serializer=lambda k: k.encode('utf-8'),
        acks='all',
    )

    kafka_consumer = AIOKafkaConsumer(
        os.environ['TOPIC_REQUEST_STOCK'],
        os.environ['TOPIC_ROLLBACK_STOCK'],
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        client_id=os.environ['STOCK_SERVICE_CONSUMER'],
        group_id=os.environ['GROUP_STOCK'],
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

        if not isinstance(payload, BatchStockRequest):
            continue

        if topic == os.environ['TOPIC_REQUEST_STOCK']:
            result = await _on_stock_request(payload)
            if result:
                reserve_stock(payload)
            else:
                fail_stock(payload)
        elif topic == os.environ['TOPIC_ROLLBACK_STOCK']:
            if _on_stock_rollback:
                await _on_stock_rollback(payload)

async def reserve_stock(request: BatchStockRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_RESERVED'],
        key=request.order_id,
        value=request,
        headers=[('idempotency_key', request.order_id.encode('utf-8'))]
    )

async def fail_stock(request: BatchStockRequest) -> None:
    await kafka_producer.send(
        os.environ['TOPIC_STOCK_FAILED'],
        key=request.order_id,
        value=request,
        headers=[('idempotency_key', request.order_id.encode('utf-8'))]
    )
