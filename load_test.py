import httpx
import asyncio
from datetime import datetime

# Schedule tasks
BASE_URL = "http://localhost:8000"


def log(msg):
    """Log with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {msg}")


async def schedule_task(client: httpx.AsyncClient, task_num: int, priority: str = "low"):
    """Schedule a single task with specified priority"""
    response = await client.get(f"{BASE_URL}/task", params={"priority": priority})
    data = response.json()
    log(f"Task {task_num} [{priority.upper()}] scheduled: {data['task_id'][:8]}... - Worker: {data['worker']}")
    return data


async def check_task_status(client: httpx.AsyncClient, task_id: str):
    """Check task status"""
    response = await client.get(f"{BASE_URL}/task/{task_id}")
    return response.json()


async def monitor_task(client: httpx.AsyncClient, task_id: str, priority: str):
    """Monitor a task until it starts processing"""
    start_time = datetime.now()
    logged = False
    while True:
        status = await check_task_status(client, task_id)
        if status.get('status') == 'processing':
            elapsed = (datetime.now() - start_time).total_seconds()
            # Only log high priority and slow low priority tasks
            if priority == "high" or elapsed > 1.0:
                if not logged:
                    log(f"  └─ [{priority.upper():4}] Task {task_id[:8]}... STARTED after {elapsed:.2f}s")
                    logged = True
            break
        elif status.get('status') == 'completed':
            break
        await asyncio.sleep(0.2)


async def main():
    log("=" * 80)
    log("REALISTIC PRIORITY QUEUE TEST - STRESS TEST")
    log("=" * 80)
    log("")
    log("Scenario:")
    log("  1. Schedule 30 LOW priority tasks RAPIDLY to saturate all capacity")
    log("  2. Wait 1 second, then schedule 10 HIGH priority tasks")
    log("  3. Wait 15 seconds, then schedule 5 MORE HIGH priority tasks")
    log("  4. HIGH priority should consistently have LOW latency")
    log("  5. LOW priority should have HIGH latency due to waiting")
    log("")
    log("Expected with 3 workers @ 3 concurrent (2 low + 1 high reserved):")
    log("  - Max low priority running: 6 tasks (2 per worker)")
    log("  - Max high priority running: 9 tasks (3 per worker)")
    log("  - 24+ LOW tasks will be queued = high latency")
    log("  - ALL HIGH tasks should jump the queue = low latency (even late arrivals)")
    log("")
    log("=" * 80)
    log("")

    async with httpx.AsyncClient() as client:
        all_tasks = []

        # Phase 1: FLOOD with low priority tasks to saturate capacity
        log("PHASE 1: FLOODING with 30 LOW priority tasks...")
        low_priority_tasks = []

        # Submit all low priority tasks as fast as possible
        low_submit_tasks = []
        for i in range(30):
            low_submit_tasks.append(schedule_task(client, i+1, "low"))

        results = await asyncio.gather(*low_submit_tasks)
        low_priority_tasks.extend(results)
        all_tasks.extend(results)

        # Monitor them
        for result in results:
            asyncio.create_task(monitor_task(client, result['task_id'], "low"))

        log(f"  → {len(low_priority_tasks)} LOW priority tasks submitted!")
        log("")
        log("Waiting 1 second for queue to build up...")
        await asyncio.sleep(1)

        # Phase 2: Schedule high priority tasks - should process IMMEDIATELY
        log("")
        log("PHASE 2: Scheduling 10 HIGH priority tasks...")
        log("  → These should start IMMEDIATELY despite 30 LOW tasks in queue!")
        high_priority_tasks = []

        # Submit high priority tasks rapidly
        high_submit_tasks = []
        for i in range(10):
            high_submit_tasks.append(schedule_task(client, i+31, "high"))

        results = await asyncio.gather(*high_submit_tasks)
        high_priority_tasks.extend(results)
        all_tasks.extend(results)

        # Monitor them
        for result in results:
            asyncio.create_task(monitor_task(client, result['task_id'], "high"))

        log(f"  → {len(high_priority_tasks)} HIGH priority tasks submitted!")

        # Phase 3: Wait and schedule more high priority tasks to prove sustained performance
        log("")
        log("Waiting 15 seconds for some tasks to complete...")
        await asyncio.sleep(15)

        log("")
        log("PHASE 3: Scheduling 5 MORE HIGH priority tasks (to test sustained priority)...")
        log("  → System is still busy with LOW tasks, HIGH should STILL be fast!")

        # Submit more high priority tasks
        more_high_submit_tasks = []
        for i in range(5):
            more_high_submit_tasks.append(schedule_task(client, i+41, "high"))

        results = await asyncio.gather(*more_high_submit_tasks)
        high_priority_tasks.extend(results)
        all_tasks.extend(results)

        # Monitor them
        for result in results:
            asyncio.create_task(monitor_task(client, result['task_id'], "high"))

        log(f"  → {len(results)} MORE HIGH priority tasks submitted!")
        log(f"  → Total HIGH priority: {len(high_priority_tasks)} tasks")

        log("")
        log("=" * 80)
        log(f"All {len(all_tasks)} tasks scheduled. Waiting for completion...")
        log("This will take a while due to 30 LOW priority tasks...")
        log("=" * 80)

        # Wait for all tasks to complete (30 tasks * 10s / 6 concurrent low = ~50s)
        await asyncio.sleep(50)

        # Get final status of all tasks
        log("")
        log("=" * 80)
        log("FINAL RESULTS")
        log("=" * 80)

        status_tasks = [check_task_status(client, r['task_id']) for r in all_tasks]
        statuses = await asyncio.gather(*status_tasks)

        # Analyze results
        high_same = 0
        high_diff = 0
        low_same = 0
        low_diff = 0

        log("")
        log("Task Processing Summary (showing notable tasks):")
        log("")

        high_latencies = []
        low_latencies = []

        # Track tasks per worker
        worker_stats = {}  # {worker_name: {"high": count, "low": count}}

        # Collect all stats first
        for status in statuses:
            if 'error' not in status:
                task_id = status.get('task_id', 'unknown')
                scheduled_by = status.get('scheduled_by', 'unknown')
                processed_by = status.get('processed_by', 'not yet')
                same = status.get('same_worker', False)
                priority = status.get('priority', 'unknown')
                task_status = status.get('status', 'unknown')
                latency_ms = status.get('latency_ms', 'N/A')

                # Track worker processing
                if processed_by != 'not yet':
                    if processed_by not in worker_stats:
                        worker_stats[processed_by] = {"high": 0, "low": 0}
                    if priority in ["high", "low"]:
                        worker_stats[processed_by][priority] += 1

                if priority == "high":
                    if same:
                        high_same += 1
                    else:
                        high_diff += 1
                    if latency_ms != 'N/A':
                        high_latencies.append(float(latency_ms))
                else:
                    if same:
                        low_same += 1
                    else:
                        low_diff += 1
                    if latency_ms != 'N/A':
                        low_latencies.append(float(latency_ms))

        # Show sample of high priority tasks from different phases
        log("Sample HIGH priority tasks:")
        log("  Phase 2 (early arrivals - tasks 31-35):")
        count = 0
        for status in statuses:
            if 'error' not in status and status.get('priority') == 'high' and count < 3:
                task_id = status.get('task_id', 'unknown')
                latency_ms = status.get('latency_ms', 'N/A')
                same = status.get('same_worker', False)
                marker = "✓ SAME" if same else "✗ DIFF"
                latency_str = f"{float(latency_ms):.2f}ms" if latency_ms != 'N/A' else 'N/A'
                log(f"    {task_id[:8]}... | Latency: {latency_str:10} | {marker}")
                count += 1

        log("  Phase 3 (late arrivals - tasks 41-45, after 15s delay):")
        count = 0
        for status in reversed(statuses):  # Check from end to get late arrivals
            if 'error' not in status and status.get('priority') == 'high' and count < 3:
                task_id = status.get('task_id', 'unknown')
                latency_ms = status.get('latency_ms', 'N/A')
                same = status.get('same_worker', False)
                marker = "✓ SAME" if same else "✗ DIFF"
                latency_str = f"{float(latency_ms):.2f}ms" if latency_ms != 'N/A' else 'N/A'
                log(f"    {task_id[:8]}... | Latency: {latency_str:10} | {marker}")
                count += 1

        # Show sample of low priority tasks with highest latency (top 5)
        log("")
        log("Sample LOW priority tasks (highest latency):")
        low_tasks_with_latency = []
        for status in statuses:
            if 'error' not in status and status.get('priority') == 'low':
                latency_ms = status.get('latency_ms', 'N/A')
                if latency_ms != 'N/A':
                    low_tasks_with_latency.append((float(latency_ms), status))

        low_tasks_with_latency.sort(reverse=True)
        for latency, status in low_tasks_with_latency[:5]:
            task_id = status.get('task_id', 'unknown')
            same = status.get('same_worker', False)
            marker = "✓ SAME" if same else "✗ DIFF"
            log(f"  {task_id[:8]}... | Latency: {latency:.2f}ms | {marker}")

        log("")
        log("=" * 80)
        log("LATENCY ANALYSIS")
        log("=" * 80)

        # Calculate average latencies
        avg_high_latency = sum(high_latencies) / len(high_latencies) if high_latencies else 0
        avg_low_latency = sum(low_latencies) / len(low_latencies) if low_latencies else 0
        min_high_latency = min(high_latencies) if high_latencies else 0
        max_high_latency = max(high_latencies) if high_latencies else 0
        min_low_latency = min(low_latencies) if low_latencies else 0
        max_low_latency = max(low_latencies) if low_latencies else 0

        log("")
        log(f"HIGH Priority Tasks ({len(high_priority_tasks)} tasks):")
        log(f"  Latency: avg={avg_high_latency:.2f}ms | min={min_high_latency:.2f}ms | max={max_high_latency:.2f}ms")
        log(f"  Same worker: {high_same} | Different worker: {high_diff}")

        log("")
        log(f"LOW Priority Tasks ({len(low_priority_tasks)} tasks):")
        log(f"  Latency: avg={avg_low_latency:.2f}ms | min={min_low_latency:.2f}ms | max={max_low_latency:.2f}ms")
        log(f"  Same worker: {low_same} | Different worker: {low_diff}")

        log("")
        log("Priority System Performance:")
        if avg_high_latency > 0 and avg_low_latency > 0:
            improvement = (avg_low_latency / avg_high_latency)
            log(f"  → LOW priority tasks waited {improvement:.1f}x LONGER than HIGH priority")
            log(f"  → Average difference: {avg_low_latency - avg_high_latency:.2f}ms")

            if avg_high_latency < 100:
                log(f"  ✓ HIGH priority latency < 100ms: EXCELLENT")
            elif avg_high_latency < 500:
                log(f"  ✓ HIGH priority latency < 500ms: GOOD")
            else:
                log(f"  ✗ HIGH priority latency > 500ms: NEEDS IMPROVEMENT")

            if improvement > 10:
                log(f"  ✓ Priority system working EFFECTIVELY (>{improvement:.0f}x improvement)")
            elif improvement > 2:
                log(f"  ✓ Priority system working ({improvement:.1f}x improvement)")
            else:
                log(f"  ✗ Priority system NOT working effectively ({improvement:.1f}x)")

        log("")
        log("=" * 80)
        log("WORKER LOAD DISTRIBUTION")
        log("=" * 80)
        log("")

        # Display worker statistics
        if worker_stats:
            for worker, stats in sorted(worker_stats.items()):
                total_tasks = stats["high"] + stats["low"]
                log(f"{worker}:")
                log(f"  HIGH priority: {stats['high']:2d} tasks")
                log(f"  LOW priority:  {stats['low']:2d} tasks")
                log(f"  Total:         {total_tasks:2d} tasks")
                log("")

            # Calculate totals
            total_high_processed = sum(stats["high"] for stats in worker_stats.values())
            total_low_processed = sum(stats["low"] for stats in worker_stats.values())
            total_processed = total_high_processed + total_low_processed

            log("Overall Distribution:")
            log(f"  Total HIGH processed: {total_high_processed} tasks across {len(worker_stats)} workers")
            log(f"  Total LOW processed:  {total_low_processed} tasks across {len(worker_stats)} workers")
            log(f"  Total processed:      {total_processed} tasks")

            # Check load balance
            if worker_stats:
                task_counts = [stats["high"] + stats["low"] for stats in worker_stats.values()]
                max_tasks = max(task_counts)
                min_tasks = min(task_counts)
                balance_ratio = max_tasks / min_tasks if min_tasks > 0 else 0

                log("")
                log("Load Balance:")
                log(f"  Max tasks per worker: {max_tasks}")
                log(f"  Min tasks per worker: {min_tasks}")
                if balance_ratio <= 1.5:
                    log(f"  ✓ Well balanced ({balance_ratio:.2f}x ratio)")
                elif balance_ratio <= 2.0:
                    log(f"  ~ Moderately balanced ({balance_ratio:.2f}x ratio)")
                else:
                    log(f"  ✗ Unbalanced ({balance_ratio:.2f}x ratio)")

        log("")
        log("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
