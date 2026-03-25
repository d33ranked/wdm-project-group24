"""
This file demonstrates the abstraction boundary:
    - ZERO Kafka imports in service code
    - ZERO retry logic in service code
    - ZERO checkpoint management in service code
    - Business logic is fully self-contained in handler functions

Run with: python example_service.py
(Requires a running Kafka broker at localhost:9092)
"""

import asyncio
import logging

# Only the library's public surface is imported — service code never
# touches confluent_kafka, CheckpointStore, RetryHandler, etc.
from orchastrator import Orchestrator, Task, TaskResult, RetryPolicy
from orchastrator.logging_config import configure_logging


# ------------------------------------------------------------------
# 1. Configure logging once at startup
# ------------------------------------------------------------------
configure_logging(level="INFO")
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# 2. Create the orchestrator with infrastructure config
# ------------------------------------------------------------------
orch = Orchestrator(
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id":          "order-service",
    },
    producer_config = {
        "bootstrap.servers": "localhost:9092",
    },
    checkpoint_dir   = "/tmp/order-service-checkpoints",
    dlq_topic        = "order-service.dlq",

    # Optional: tune backoff for your downstream SLA
    retry_policy     = RetryPolicy(base_delay=2.0, max_delay=30.0, multiplier=2.0),
    max_concurrency  = 20,
)


# ------------------------------------------------------------------
# 3. Register handlers — one per input topic
#    Each handler is pure business logic, nothing more.
# ------------------------------------------------------------------

@orch.handler("orders.created", max_retries=5)
async def handle_order_created(task: Task) -> TaskResult:
    """
    Process a newly created order.

    Raises on transient failures (network, DB timeouts) — the orchestrator
    will retry automatically.  Returns TaskResult.fail() for permanent
    business errors that should not be retried.
    """
    order = task.value  # already deserialized from JSON by the orchestrator

    # Validate — permanent failure, no retry
    if not order.get("customer_id"):
        return TaskResult.fail("order missing customer_id")

    # Simulate work — replace with your actual domain calls
    logger.info("Processing order", extra={"order_id": order.get("id")})
    enriched = {**order, "status": "accepted", "processed_by": "order-service"}

    # On success, publish to the downstream topic
    return TaskResult.ok(
        output_topic = "orders.accepted",
        output_value = enriched,
    )


@orch.handler("inventory.reserved", max_retries=3)
async def handle_inventory_reserved(task: Task) -> TaskResult:
    """Handle a successful inventory reservation event."""
    reservation = task.value

    logger.info("Inventory reserved", extra={"reservation_id": reservation.get("id")})

    # Raise to simulate a transient downstream error — orchestrator will retry
    # (remove this in real code):
    # raise ConnectionError("payment gateway temporarily unavailable")

    return TaskResult.ok(
        output_topic = "payments.requested",
        output_value = {"reservation_id": reservation.get("id"), "amount": reservation.get("total")},
    )


# ------------------------------------------------------------------
# 4. Entry point
# ------------------------------------------------------------------

async def main():
    logger.info("Order service starting")
    # run() blocks until cancelled (Ctrl-C or SIGTERM)
    await orch.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Order service stopped")