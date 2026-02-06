#!/bin/bash

echo "Starting multiple FastAPI workers..."
echo ""

# Start workers in background
uvicorn fastdis.main:app --host 0.0.0.0 --port 8001 &
sleep 1

uvicorn fastdis.main:app --host 0.0.0.0 --port 8002 &
sleep 1

uvicorn fastdis.main:app --host 0.0.0.0 --port 8003 &
sleep 1

echo "All workers started!"
echo "Worker 1: http://localhost:8001"
echo "Worker 2: http://localhost:8002"
echo "Worker 3: http://localhost:8003"
echo ""
echo "Press Ctrl+C to stop all workers"

# Wait for all background jobs
wait
