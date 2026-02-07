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
High worker:   acquire(global_sem)                    -> BRPOP(high_queue) -> execute
Low worker:    acquire(low_priority_cap, global_sem)  -> BRPOP(low_queue)  -> execute
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

### Worker Loop Order: Acquire-then-BRPOP

The worker loop acquires semaphore slots *before* popping a task from Redis:

```python
while True:
    for sem in semaphores:
        await sem.acquire()       # 1. wait for capacity
    task = BRPOP(queue)           # 2. wait for a task (blocking)
    asyncio.create_task(execute)  # 3. run and loop back
```

The alternative would be BRPOP-then-acquire (pop first, then wait for capacity).
The choice between the two is a deliberate tradeoff:

**Acquire-then-BRPOP** (chosen) — the worker holds a `global_sem` slot while
blocked on an empty queue. For the high priority worker, this means 1 global
slot is held while waiting for work. This is actually desirable: the held slot
*is* the reserved slot. The high priority worker pre-claims its reserved
capacity, so when a high priority task arrives it can execute immediately with
zero semaphore acquisition latency. Low priority effective capacity is
`MAX - RESERVED` regardless — the same limit enforced by `low_priority_cap` —
so no usable capacity is lost. In a multi-instance deployment, tasks stay in
Redis until a worker with confirmed capacity pops them, so any instance with
free slots can pick up the work.

**BRPOP-then-acquire** — the worker only holds semaphore slots while a task is
actually running, so no capacity is held by idle workers. However, this creates
a "task-in-limbo" problem in multi-instance deployments: a worker pops a task
from Redis (removing it from the shared queue), then blocks on local semaphore
acquisition. The task is now stuck in one instance's memory while another
instance with free capacity cannot help. This is bounded (at most 1 task per
worker loop) but unnecessary given that the acquire-first pattern's "cost" is
holding the reserved slot — which is the intended behavior anyway.

### Multi-Instance Behavior

Multiple instances share the same Redis queues. Redis `BRPOP` is atomic, so
exactly one instance receives each task. The semaphores are `asyncio.Semaphore`
(in-process), so each instance enforces its own limits independently:

- **Total capacity**: `N * MAX_CONCURRENT_TASKS` where N is the number of instances.
- **Per-instance guarantees**: Each instance independently reserves
  `HIGH_PRIORITY_RESERVED` slots for high priority and enforces its own hard
  limit. The system-wide invariant holds in aggregate.

Because the worker acquires semaphores before popping, tasks remain in Redis
until a worker with confirmed local capacity takes them. This means work
naturally distributes to instances that have available slots, avoiding the
task-in-limbo problem entirely.

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
