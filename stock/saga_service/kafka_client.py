import os
import asyncio
import redis 

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from msgspec import msgpack

from models import StockValue
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
        group_id=os.environ['GROUP_STOCK'],
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

    event_loop.create_task(consume_loop())


async def consume_loop():
    async for message in kafka_consumer:
        order_id = message.key
        topic = message.topic
        payload = message.value

        print(f"[STOCK] Received message on topic {topic} for order {order_id} with payload {payload}")
        if topic == os.environ['TOPIC_STOCK_REQUEST']:

            if db.get(f"rolledback: {order_id}") is not None:
                print(f"got a request for a order we already got rollback for: {order_id}, skipping")
                db.set(f"handled: {order_id}", 0)
                continue

            to_update = payload["items"]
            keys = [item['item_id'] for item in to_update]
            original_state = {}

            try:
                with db.pipeline() as pipe:
                    while True:
                        try:
                            pipe.watch(*keys)  # Watch all keys for changes

                            # Read current stock values
                            stock_values = {}
                            for item_to_update in to_update:
                                item = item_to_update["item_id"]
                                amount = item_to_update["quantity"]
                                raw = pipe.get(item)  # Still in immediate mode after watch
                                if raw is None:
                                    await _send_stock_failure(order_id, f"Item {item} not found")
                                    print(f"Item {item} not found for order {order_id}")
                                    pipe.reset()
                                    return
                                stock: StockValue = msgpack.decode(raw, type=StockValue)
                                if stock.stock < amount:
                                    await _send_stock_failure(order_id, f"Not enough stock for item {item}")
                                    print(f"Not enough stock for item {item} in order {order_id}")
                                    pipe.reset()
                                    return
                                stock_values[item] = stock

                            print(f"Checked all stocks for order: {order_id}")

                            # Queue all updates atomically
                            pipe.multi()
                            for item_to_update in to_update:
                                item = item_to_update["item_id"]
                                original_state[item] = (stock_values[item].stock, item_to_update['quantity'], stock_values[item].stock-item_to_update['quantity'])
                                stock_values[item].stock -= item_to_update["quantity"]
                                pipe.set(item, msgpack.encode(stock_values[item]))

                            if pipe.get(f"handled: {order_id}") is not None or pipe.get(f"rolledback: {order_id}") is not None:
                                print(f"Cancelled transaction, because already rolledback or handled was set: {order_id}")
                                pipe.reset()

                            pipe.set(f"handled: {order_id}", 0)
                            pipe.execute()  # Fails if any watched key changed
                            break  # Success

                        except redis.WatchError:
                            print(f"Race condition detected for order {order_id}, retrying...")
                            continue  # Retry the whole transaction

                print(f"Stock request was successfull for order: {order_id}\nIn this transaction: ")
                for item, (start, diff, end) in original_state.items():
                    print(f"Item: {item}, was updated: start: {start}, minus: {diff}, end amount: {end}")
                await _send_stock_success(order_id)

            except Exception as e:
                print(f"Stock request failed for order: {order_id}, with reason: {str(e)}")
                await _send_stock_failure(order_id, str(e))

        elif topic == os.environ['TOPIC_STOCK_ROLLBACK']:
            if db.get(f"handled: {order_id}") is None or db.get(f"rolledback: {order_id}") is not None:
                print(f"Received rollback for unhandled request, so no changes made, order: {order_id}")
                db.set(f"rolledback: {order_id}", 0)
                continue
            
            to_update = payload['items']
            keys = [item['item_id'] for item in to_update]
            original_state = {}

            with db.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(*keys)  # Watch all keys for changes

                        # Read current stock values
                        stock_values = {}
                        for item_to_update in to_update:
                            item = item_to_update["item_id"]
                            amount = item_to_update["quantity"]
                            raw = pipe.get(item)  # Still in immediate mode after watch
                            if raw is None:
                                print(f"FAILURE IN ROLLBACK: Item {item} not found for order {order_id}")
                                pipe.reset()
                                return

                            stock = msgpack.decode(raw, type=StockValue)
                            stock_values[item] = stock

                        # Queue all updates atomically
                        pipe.multi()
                        for item_to_update in to_update:
                            item = item_to_update["item_id"]
                            original_state[item] = (stock_values[item].stock, item_to_update['quantity'], stock_values[item].stock+item_to_update['quantity'])
                            stock_values[item].stock += item_to_update["quantity"]
                            pipe.set(item, msgpack.encode(stock_values[item]))
                        pipe.execute()  # Fails if any watched key changed
                        break  # Success

                    except redis.WatchError:
                        print(f"Race condition detected for order {order_id}, retrying...")
                        continue  # Retry the whole transaction
            print(f"Rolled back for order: {order_id}")
            for item, (start, diff, end) in original_state.items():
                print(f"Item: {item}, was updated: start: {start}, plus: {diff}, end amount: {end}")


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
