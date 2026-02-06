import uuid
import asyncio
import os
from fastapi import FastAPI
import redis.asyncio as redis

from task_service import TaskService

app = FastAPI()

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


async def do_work(task_id: str):
    """The actual work a task performs."""
    await asyncio.sleep(10)


@app.on_event("startup")
async def startup_event():
    global redis_client, high_priority, low_priority

    redis_client = await redis.from_url("redis://localhost:6379", decode_responses=False)
    print(f"[{WORKER_NAME}] Connected to Redis")
    print(f"[{WORKER_NAME}] Max concurrent tasks: {MAX_CONCURRENT_TASKS}")
    print(f"[{WORKER_NAME}] High priority reserved slots: {HIGH_PRIORITY_RESERVED}")
    print(f"[{WORKER_NAME}] Low priority max slots: {MAX_CONCURRENT_TASKS - HIGH_PRIORITY_RESERVED}")

    high_priority = TaskService(
        name="high",
        queue_key="high_priority_queue",
        max_concurrent=MAX_CONCURRENT_TASKS,
        redis_client=redis_client,
        worker_name=WORKER_NAME,
        handler=do_work,
    )
    low_priority = TaskService(
        name="low",
        queue_key="low_priority_queue",
        max_concurrent=MAX_CONCURRENT_TASKS - HIGH_PRIORITY_RESERVED,
        redis_client=redis_client,
        worker_name=WORKER_NAME,
        handler=do_work,
    )

    asyncio.create_task(high_priority.worker())
    asyncio.create_task(low_priority.worker())


@app.on_event("shutdown")
async def shutdown_event():
    if redis_client:
        await redis_client.close()
        print(f"[{WORKER_NAME}] Redis connection closed")


@app.get("/task")
async def start_task(priority: str = "low"):
    task_id = str(uuid.uuid4())
    if priority not in ("high", "low"):
        priority = "low"
    service = high_priority if priority == "high" else low_priority
    return await service.schedule(task_id)


@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    task_data = await redis_client.hgetall(f"task:{task_id}")
    if not task_data:
        return {"error": "Task not found"}
    result = {k.decode("utf-8"): v.decode("utf-8") for k, v in task_data.items()}
    if "scheduled_by" in result and "processed_by" in result:
        result["same_worker"] = result["scheduled_by"] == result["processed_by"]
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
