import uuid
import asyncio
import logging
import os
from fastapi import FastAPI
import redis.asyncio as redis

import log
from models import Priority, TaskStatusResponse
from task_service import TaskService

app = FastAPI()

logger = logging.getLogger("fastdis.main")

# Redis connection
redis_client: redis.Redis = None

# Worker identification
WORKER_NAME = f"worker-{uuid.uuid4().hex[:8]}"

# Concurrency configuration
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "3"))
HIGH_PRIORITY_RESERVED = int(os.getenv("HIGH_PRIORITY_RESERVED", "1"))

# Task services (initialized on startup)
high_priority: TaskService = None
low_priority: TaskService = None

# Worker tasks
worker_tasks = list()


async def do_work(task_id: str):
    """The actual work a task performs."""
    await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    global redis_client, high_priority, low_priority, worker_tasks

    log.setup(WORKER_NAME)

    redis_client = await redis.from_url("redis://localhost:6379", decode_responses=True)
    logger.info("Connected to Redis")
    logger.info("Max concurrent tasks: %d", MAX_CONCURRENT_TASKS)
    logger.info("High priority reserved slots: %d", HIGH_PRIORITY_RESERVED)
    logger.info("Low priority max slots: %d", MAX_CONCURRENT_TASKS - HIGH_PRIORITY_RESERVED)

    high_priority = TaskService(
        priority=Priority.HIGH,
        queue_key="high_priority_queue",
        max_concurrent=MAX_CONCURRENT_TASKS,
        redis_client=redis_client,
        worker_name=WORKER_NAME,
        handler=do_work,
    )
    low_priority = TaskService(
        priority=Priority.LOW,
        queue_key="low_priority_queue",
        max_concurrent=MAX_CONCURRENT_TASKS - HIGH_PRIORITY_RESERVED, # this technically allows for more than MAX_CONCURRENCY tasks to run at once
        redis_client=redis_client,
        worker_name=WORKER_NAME,
        handler=do_work,
    )

    worker_tasks.append(asyncio.create_task(high_priority.worker()))
    worker_tasks.append(asyncio.create_task(low_priority.worker()))


@app.on_event("shutdown")
async def shutdown_event():
    if redis_client:
        await redis_client.close()
        logger.info("Redis connection closed")

    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)


@app.get("/task")
async def start_task(priority: Priority = Priority.LOW):
    task_id = str(uuid.uuid4())
    service = high_priority if priority == Priority.HIGH else low_priority
    return await service.schedule(task_id)


@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    task_data = await redis_client.hgetall(f"task:{task_id}")
    if not task_data:
        return {"error": "Task not found"}
    return TaskStatusResponse.model_validate(task_data)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
