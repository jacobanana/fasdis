# fasdis

Async job queue with priority scheduling, backed by Redis.

## Architecture

Each worker instance runs a FastAPI server with two worker loops — one consuming
from `high_priority_queue` and one from `low_priority_queue` in Redis. Jobs are
submitted via HTTP and processed asynchronously with configurable concurrency.

## Concurrency & Priority Design

### Goals

1. **Hard concurrency limit** — total running tasks on a single instance must
   never exceed `MAX_CONCURRENT_TASKS`.
2. **Reserved bandwidth** — high priority jobs must always have slots available,
   even when the system is saturated with low priority work.
3. **Full utilization** — high priority jobs should be able to use *all* slots
   when no low priority work is running (and vice versa for unreserved capacity).

### Implementation: Shared Global Semaphore + Low Priority Cap

Concurrency is controlled by two `asyncio.Semaphore` instances, created once at
startup and shared across both worker loops:

```
global_sem      = Semaphore(MAX_CONCURRENT_TASKS)               # e.g. 3
low_priority_cap = Semaphore(MAX_CONCURRENT_TASKS - RESERVED)   # e.g. 2
```

The high priority worker acquires only `global_sem`. The low priority worker
acquires `low_priority_cap` first, then `global_sem`.

```
High worker:   BRPOP(high_queue) -> acquire(global_sem)                    -> execute
Low worker:    BRPOP(low_queue)  -> acquire(low_priority_cap, global_sem)  -> execute
```

This produces the following behavior (example: MAX=3, RESERVED=1):

| Scenario             | High running | Low running | Total | Allowed? |
|----------------------|--------------|-------------|-------|----------|
| All low              | 0            | 2           | 2     | Yes      |
| All high             | 3            | 0           | 3     | Yes      |
| Mixed                | 1            | 2           | 3     | Yes      |
| Low tries 3rd slot   | 1            | 2 -> 3      | 4     | Blocked  |
| High bursts (no low) | 3            | 0           | 3     | Yes      |

- `global_sem` enforces the hard ceiling. Both priorities share it, so total
  concurrency can never exceed `MAX_CONCURRENT_TASKS`.
- `low_priority_cap` prevents low priority from occupying more than
  `MAX - RESERVED` slots, guaranteeing that `RESERVED` slots remain available
  for high priority.
- High priority only needs `global_sem`, so it can use the full capacity when
  low priority workers are idle.

### Worker Loop Order: BRPOP-then-Acquire

The worker loop pops a task from Redis *before* acquiring semaphores:

```python
while True:
    task = BRPOP(queue)           # 1. wait for a task (blocking)
    for sem in semaphores:
        await sem.acquire()       # 2. wait for capacity
    asyncio.create_task(execute)  # 3. run and loop back
```

The alternative would be to acquire semaphores first, then pop (acquire-then-BRPOP).
The choice between the two is a deliberate tradeoff:

**Acquire-then-BRPOP** — the worker holds a `global_sem` slot while blocked on
an empty queue. If no high priority tasks arrive, one global slot is permanently
occupied by the idle high-priority worker, reducing effective capacity for low
priority. With a shared global semaphore this is unacceptable: an idle high
worker would silently reduce the system to `MAX - 1` effective slots.

**BRPOP-then-acquire** (chosen) — the worker only holds semaphore slots while a
task is actually running. No capacity is wasted on idle workers. The cost is a
"task-in-limbo" edge case in multi-instance deployments (see below).

### Multi-Instance Behavior

Multiple instances share the same Redis queues. Redis `BRPOP` is atomic, so
exactly one instance receives each task. The semaphores are `asyncio.Semaphore`
(in-process), so each instance enforces its own limits independently:

- **Total capacity**: `N * MAX_CONCURRENT_TASKS` where N is the number of instances.
- **Per-instance guarantees**: Each instance independently reserves
  `HIGH_PRIORITY_RESERVED` slots for high priority and enforces its own hard
  limit. The system-wide invariant holds in aggregate.

**Task-in-limbo edge case**: Because the worker pops before acquiring a
semaphore, a task can be removed from Redis and then block waiting for local
capacity — while another instance with free slots cannot help (the task is
already gone from Redis). This is bounded: at most one task per worker loop
(two per instance) can be in limbo, and the delay is bounded by the duration of
the shortest running task on that instance. This is a significantly better
tradeoff than the permanent slot waste of the acquire-first alternative.

## Configuration

| Variable                | Default | Description                              |
|-------------------------|---------|------------------------------------------|
| `MAX_CONCURRENT_TASKS`  | 3       | Hard limit on concurrent tasks per instance |
| `HIGH_PRIORITY_RESERVED`| 1       | Slots reserved for high priority per instance |

## Running

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

## API

- `GET /task?priority=low|high` — schedule a new task
- `GET /task/{task_id}` — check task status
