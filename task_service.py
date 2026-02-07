import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable, Any

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
        self.records: dict[str, TaskRecord] = dict()

    def _set_context(self, task_id: str | None = None):
        ctx_service.set(self.priority.value.upper())
        if task_id:
            ctx_task_id.set(task_id[:6])

    async def set_task_record(self, task_id: str, content: dict[str, Any]) -> None:
        """Set or update a task record in redis"""
        key = f"task:{task_id}"

        if task_id not in self.records:
            existing = await self.redis_client.hgetall(key)
            if existing:
                content = {**existing, **content}
            self.records[task_id] = TaskRecord(**content)
        else:
            self.records[task_id] = self.records[task_id].copy(update=content)

        async with self.redis_client.pipeline(transaction=True) as pipe:
            await pipe.hset(key, mapping=self.records[task_id].to_redis())
            await pipe.expire(key, 3600)
            await pipe.execute()

    async def schedule(self, task_id: str) -> ScheduleResponse:
        """Push a task onto this service's queue and store its metadata."""
        self._set_context(task_id)
        
        now = datetime.now(timezone.utc)
        
        record = {
            "task_id": task_id,
            "priority": self.priority,
            "status": TaskStatus.QUEUED,
            "scheduled_by": self.worker_name,
            "scheduled_at": now,
        }

        await self.set_task_record(task_id, record)
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
            scheduled_at_dt = datetime.fromisoformat(scheduled_at)
            latency_ms = (start_time - scheduled_at_dt).total_seconds() * 1000
        else:
            latency_ms = 0

        record = {
            "processed_by": self.worker_name,
            "status": TaskStatus.PROCESSING,
            "started_at": start_time,
            "latency_ms": latency_ms,
        }

        await self.set_task_record(task_id, record)

        scheduled_by = await self.redis_client.hget(f"task:{task_id}", "scheduled_by")
        scheduled_by = scheduled_by if scheduled_by else "unknown"
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

        try:
            await self.handler(task_id)
            status = TaskStatus.COMPLETED
        except Exception as exc:
            logger.exception("Task failed")
            status = TaskStatus.FAILED
        finally:
            completed_at = datetime.now(timezone.utc)
            record = {
                "status": status,
                "completed_at": completed_at,
            }
            await self.set_task_record(task_id, record)
            logger.info(status.value)


    async def _execute_with_semaphore(self, task_id: str):
        """Wrapper that releases the semaphore when the task finishes."""
        self._set_context(task_id[:6])
        try:
            await self._execute_task(task_id)
        finally:
            self.records.pop(task_id, None)
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
                logger.info("Picked up task %s from queue", task_id[:6])
                asyncio.create_task(self._execute_with_semaphore(task_id))
            else:
                self.semaphore.release()
