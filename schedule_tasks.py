import httpx
import asyncio

# Schedule 10 tasks
BASE_URL = "http://localhost:8000"
N_TASKS = 10

async def schedule_task(client: httpx.AsyncClient, task_num: int):
    """Schedule a single task"""
    response = await client.get(f"{BASE_URL}/task")
    data = response.json()
    print(f"Task {task_num} scheduled: {data['task_id']} - Status: {data['status']} - Worker: {data['worker']}")
    return data


async def check_task_status(client: httpx.AsyncClient, task_id: str):
    """Check task status"""
    response = await client.get(f"{BASE_URL}/task/{task_id}")
    return response.json()


async def main():
    print(f"Scheduling {N_TASKS} tasks concurrently to {BASE_URL}/task...")
    print()

    async with httpx.AsyncClient() as client:
        # Create concurrent tasks
        tasks = [schedule_task(client, i+1) for i in range(N_TASKS)]
        results = await asyncio.gather(*tasks)

        print()
        print("=" * 80)
        print("SUMMARY - All Tasks Scheduled")
        print("=" * 80)

        # Group tasks by worker
        by_worker = {}
        for result in results:
            worker = result['worker']
            if worker not in by_worker:
                by_worker[worker] = []
            by_worker[worker].append(result['task_id'])

        # Print summary by worker
        for worker, task_ids in by_worker.items():
            print(f"\n{worker}: {len(task_ids)} tasks")
            for task_id in task_ids:
                print(f"  - {task_id}")

        print()
        print(f"Total: {len(results)} tasks scheduled across {len(by_worker)} workers")
        print("=" * 80)

        # Wait for tasks to complete and show processing info
        print()
        print("Waiting for tasks to complete (this will take ~10 seconds)...")
        await asyncio.sleep(12)  # Wait for tasks to finish (10s + buffer)

        print()
        print("=" * 80)
        print("TASK PROCESSING RESULTS")
        print("=" * 80)

        # Check status of all tasks
        status_tasks = [check_task_status(client, r['task_id']) for r in results]
        statuses = await asyncio.gather(*status_tasks)

        same_worker_count = 0
        diff_worker_count = 0

        for status in statuses:
            if 'error' not in status:
                task_id = status.get('task_id', 'unknown')
                scheduled_by = status.get('scheduled_by', 'unknown')
                processed_by = status.get('processed_by', 'not yet')
                same = status.get('same_worker', False)
                task_status = status.get('status', 'unknown')

                marker = "✓ SAME" if same else "✗ DIFF"
                if same:
                    same_worker_count += 1
                else:
                    diff_worker_count += 1

                print(f"\n{task_id[:8]}... | Status: {task_status}")
                print(f"  Scheduled by: {scheduled_by}")
                print(f"  Processed by: {processed_by}")
                print(f"  {marker}")

        print()
        print(f"Same worker: {same_worker_count} | Different worker: {diff_worker_count}")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
