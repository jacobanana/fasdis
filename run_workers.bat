@echo off
echo Starting multiple FastAPI workers...
echo.

start "Worker 1 (Port 8001)" cmd /k "uvicorn fastdis.main:app --host 0.0.0.0 --port 8001"
timeout /t 2 /nobreak > nul

start "Worker 2 (Port 8002)" cmd /k "uvicorn fastdis.main:app --host 0.0.0.0 --port 8002"
timeout /t 2 /nobreak > nul

start "Worker 3 (Port 8003)" cmd /k "uvicorn fastdis.main:app --host 0.0.0.0 --port 8003"
timeout /t 2 /nobreak > nul

echo All workers started!
echo Worker 1: http://localhost:8001
echo Worker 2: http://localhost:8002
echo Worker 3: http://localhost:8003
echo.
echo Press any key to exit...
pause > nul
