import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

import redis.asyncio as redis

from log import ctx_service, ctx_task_id
from models import Priority, TaskStatus, TaskRecord, ScheduleResponse

logger = logging.getLogger("fastdis.task_service")


class TaskService:
    """Generic queue-backed task service with concurrency control.

    Instantiate once per priority level, pointing at different Redis queues
    and concurrency limits. The actual work is delegated to a handler callback.
    """

    def __init__(
        self,
        priority: Priority,
        queue_key: str,
        max_concurrent: int,
        redis_client: redis.Redis,
        worker_name: str,
        handler: Callable[[str], Awaitable[None]],
    ):
        self.priority = priority
        self.queue_key = queue_key
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.redis_client = redis_client
        self.worker_name = worker_name
        self.handler = handler

    def _set_context(self):
        ctx_service.set(self.priority.value.upper())

    async def schedule(self, task_id: str) -> ScheduleResponse:
        """Push a task onto this service's queue and store its metadata."""
        self._set_context()
        ctx_task_id.set(task_id[:6])
        now = datetime.now(timezone.utc)

        record = TaskRecord(
            task_id=task_id,
            priority=self.priority,
            status=TaskStatus.QUEUED,
            scheduled_by=self.worker_name,
            scheduled_at=now,
        )
        await self.redis_client.hset(f"task:{task_id}", mapping=record.to_redis())
        await self.redis_client.lpush(self.queue_key, task_id)
        logger.info("Added to Redis queue at %s", now.isoformat())

        return ScheduleResponse(
            task_id=task_id,
            status=TaskStatus.QUEUED,
            priority=self.priority,
            worker=self.worker_name,
            scheduled_at=now,
        )

    async def _execute_task(self, task_id: str):
        """Run the full task lifecycle: metadata tracking, handler, completion."""
        start_time = datetime.now(timezone.utc)

        scheduled_at = await self.redis_client.hget(f"task:{task_id}", "scheduled_at")
        if scheduled_at:
            scheduled_at_dt = datetime.fromisoformat(scheduled_at.decode("utf-8"))
            latency_ms = (start_time - scheduled_at_dt).total_seconds() * 1000
        else:
            latency_ms = 0

        await self.redis_client.hset(f"task:{task_id}", mapping={
            "processed_by": self.worker_name,
            "status": TaskStatus.PROCESSING,
            "started_at": start_time.isoformat(),
            "latency_ms": str(latency_ms),
        })

        scheduled_by = await self.redis_client.hget(f"task:{task_id}", "scheduled_by")
        scheduled_by = scheduled_by.decode("utf-8") if scheduled_by else "unknown"
        same_worker = "SAME" if scheduled_by == self.worker_name else "DIFFERENT"

        running = self.max_concurrent - self.semaphore._value
        logger.info(
            "Started (running: %d/%d) | Latency: %.2fms",
            running, self.max_concurrent, latency_ms,
        )
        logger.info(
            "  Scheduled by: %s | Processed by: %s | %s",
            scheduled_by, self.worker_name, same_worker,
        )

        await self.handler(task_id)

        completed_at = datetime.now(timezone.utc)
        await self.redis_client.hset(f"task:{task_id}", mapping={
            "status": TaskStatus.COMPLETED,
            "completed_at": completed_at.isoformat(),
        })
        logger.info("Completed")

    async def _execute_with_semaphore(self, task_id: str):
        """Wrapper that releases the semaphore when the task finishes."""
        self._set_context()
        ctx_task_id.set(task_id[:6])
        try:
            await self._execute_task(task_id)
        finally:
            self.semaphore.release()
            available = self.semaphore._value
            logger.info(
                "Slots available = %d/%d", available, self.max_concurrent,
            )

    async def worker(self):
        """Main loop: acquire a concurrency slot, pop from queue, execute."""
        self._set_context()
        logger.info("Worker started, waiting for tasks...")
        while True:
            available = self.semaphore._value
            logger.info(
                "Waiting for capacity (available slots: %d/%d)",
                available, self.max_concurrent,
            )
            await self.semaphore.acquire()
            logger.info("Acquired capacity, checking queue...")

            result = await self.redis_client.brpop(self.queue_key, timeout=0)
            if result:
                _, task_id = result
                task_id = task_id.decode("utf-8")
                logger.info("Picked up task %s from queue", task_id[:6])
                asyncio.create_task(self._execute_with_semaphore(task_id))
            else:
                self.semaphore.release()
