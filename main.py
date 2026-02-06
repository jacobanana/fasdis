import uuid
import asyncio
import os
from fastapi import FastAPI
import redis.asyncio as redis

app = FastAPI()

# Redis connection
redis_client: redis.Redis = None
QUEUE_KEY = "task_queue"

# Worker identification
WORKER_NAME = f"worker-{uuid.uuid4().hex[:8]}"

# Semaphore to limit concurrent tasks
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "3"))
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)


async def long_running_task(task_id: str):
    """Executes a long task (waits for 10 seconds)"""
    # Update task metadata - mark as processing
    await redis_client.hset(f"task:{task_id}", mapping={
        "processed_by": WORKER_NAME,
        "status": "processing"
    })

    # Get scheduled_by info for logging
    scheduled_by = await redis_client.hget(f"task:{task_id}", "scheduled_by")
    scheduled_by = scheduled_by.decode('utf-8') if scheduled_by else "unknown"
    same_worker = "✓ SAME" if scheduled_by == WORKER_NAME else "✗ DIFFERENT"

    print(f"[{WORKER_NAME}] Task {task_id} started (concurrent tasks: {MAX_CONCURRENT_TASKS - task_semaphore._value})")
    print(f"[{WORKER_NAME}]   Scheduled by: {scheduled_by} | Processed by: {WORKER_NAME} | {same_worker}")

    await asyncio.sleep(10)

    # Update task metadata - mark as completed
    await redis_client.hset(f"task:{task_id}", "status", "completed")
    print(f"[{WORKER_NAME}] Task {task_id} completed")


async def execute_task_with_semaphore(task_id: str):
    """Wrapper that executes task and releases semaphore when done"""
    try:
        await long_running_task(task_id)
    finally:
        task_semaphore.release()
        available_slots = task_semaphore._value
        print(f"[{WORKER_NAME}] available slots = {available_slots}/{MAX_CONCURRENT_TASKS}")



async def worker():
    """Worker that reads from the Redis queue and executes tasks"""
    print(f"[{WORKER_NAME}] Worker started, waiting for tasks...")
    while True:
        # Wait until we have capacity (acquire semaphore first)
        available_slots = task_semaphore._value
        print(f"[{WORKER_NAME}] Waiting for capacity (available slots: {available_slots}/{MAX_CONCURRENT_TASKS})")
        await task_semaphore.acquire()
        print(f"[{WORKER_NAME}] Acquired capacity, checking queue...")

        # Now we have capacity, pop from queue
        result = await redis_client.brpop(QUEUE_KEY, timeout=0)
        if result:
            _, task_id = result
            task_id = task_id.decode('utf-8')
            print(f"[{WORKER_NAME}] Picked up task {task_id} from Redis queue")
            # Execute task in background, will release semaphore when done
            asyncio.create_task(execute_task_with_semaphore(task_id))
        else:
            # No task available (shouldn't happen with timeout=0), release semaphore
            task_semaphore.release()


@app.on_event("startup")
async def startup_event():
    """Initialize Redis and start the worker when the app starts"""
    global redis_client
    redis_client = await redis.from_url("redis://localhost:6379", decode_responses=False)
    print(f"[{WORKER_NAME}] Connected to Redis")
    print(f"[{WORKER_NAME}] Max concurrent tasks: {MAX_CONCURRENT_TASKS}")
    asyncio.create_task(worker())


@app.on_event("shutdown")
async def shutdown_event():
    """Close Redis connection on shutdown"""
    if redis_client:
        await redis_client.close()
        print(f"[{WORKER_NAME}] Redis connection closed")


@app.get("/uuid")
async def get_uuid():
    return {"uuid": str(uuid.uuid4())}


@app.get("/task")
async def start_task():
    """Endpoint to add a task to the Redis queue"""
    task_id = str(uuid.uuid4())

    # Store task metadata
    await redis_client.hset(f"task:{task_id}", mapping={
        "task_id": task_id,
        "scheduled_by": WORKER_NAME,
        "status": "queued"
    })

    # Add to queue
    await redis_client.lpush(QUEUE_KEY, task_id)
    print(f"Task {task_id} added to Redis queue")
    return {"task_id": task_id, "status": "queued", "worker": WORKER_NAME}


@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """Get task status and worker information"""
    task_data = await redis_client.hgetall(f"task:{task_id}")

    if not task_data:
        return {"error": "Task not found"}

    # Decode bytes to strings
    result = {k.decode('utf-8'): v.decode('utf-8') for k, v in task_data.items()}

    # Add comparison
    if "scheduled_by" in result and "processed_by" in result:
        result["same_worker"] = result["scheduled_by"] == result["processed_by"]

    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
