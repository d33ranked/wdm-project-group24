"""
This package exposes only what service authors need:
    - Orchestrator       the main class to instantiate and run
    - Task               data model passed into handlers
    - TaskResult         returned by handlers to drive routing
    - TaskStatus         enum for task lifecycle states
    - RetryPolicy        override default backoff behaviour
    - configure_logging  optional structured JSON logging setup

Internal classes (KafkaConsumerWrapper, KafkaProducerWrapper,
CheckpointStore, RetryHandler) are intentionally not exported — they
are implementation details that may change.

QUICK START
-----------
    import asyncio
    from kafka_orchestrator import Orchestrator, Task, TaskResult
    from kafka_orchestrator.logging_config import configure_logging

    configure_logging("INFO")

    orch = Orchestrator(
        consumer_config = {"bootstrap.servers": "localhost:9092", "group.id": "my-service"},
        producer_config = {"bootstrap.servers": "localhost:9092"},
        checkpoint_dir  = "/tmp/checkpoints",
        dlq_topic       = "my-service.dlq",
    )

    @orch.handler("orders.created")
    async def handle_order(task: Task) -> TaskResult:
        # All your business logic lives here — not in the orchestrator
        processed = await my_domain_function(task.value)
        return TaskResult.ok("orders.processed", processed)

    asyncio.run(orch.run())
"""

from .orchastrator import Orchestrator
from .task import Task, TaskResult, TaskStatus
from .retry import RetryPolicy

__all__ = [
    "Orchestrator",
    "Task",
    "TaskResult",
    "TaskStatus",
    "RetryPolicy",
]