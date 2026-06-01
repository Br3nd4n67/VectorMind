@echo off
setlocal EnableDelayedExpansion

echo.
echo  VectorMind Setup
echo  ================
echo.

REM ── Check Python ─────────────────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Install Python 3.11+ from https://python.org
    pause & exit /b 1
)

REM ── Check Ollama ─────────────────────────────────────────────────────────────
ollama --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Ollama not found. Install from https://ollama.com
    pause & exit /b 1
)

REM ── Python venv ──────────────────────────────────────────────────────────────
echo [1/5] Creating Python environment...
cd /d "%~dp0vector-ai"
if not exist venv (
    python -m venv venv
)
call venv\Scripts\activate.bat
pip install -q --upgrade pip
pip install -q fastapi uvicorn httpx python-dotenv pydantic
echo      Done.

REM ── .env setup ───────────────────────────────────────────────────────────────
echo [2/5] Setting up config...
if not exist .env (
    copy .env.example .env >nul
    echo      Created vector-ai\.env from template.
    echo      IMPORTANT: Edit vector-ai\.env to set FFMPEG_PATH and YTDLP_PATH if needed.
) else (
    echo      .env already exists, skipping.
)

REM ── Pull Ollama models ────────────────────────────────────────────────────────
echo [3/5] Pulling Ollama models (this will take a while on first run)...
ollama pull llama3.3:70b
ollama pull llama3.2:3b
echo      Done.

REM ── Register scheduled task ──────────────────────────────────────────────────
echo [4/5] Registering VectorPod-Supervisor scheduled task...
set VENV_PY=%~dp0vector-ai\venv\Scripts\python.exe
set SUP=%~dp0supervisor.py
set WORKDIR=%~dp0

powershell -NoProfile -Command ^
  "$a = New-ScheduledTaskAction -Execute '%VENV_PY%' -Argument '\"%SUP%\"' -WorkingDirectory '%WORKDIR%'; ^
   $s = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 0) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1); ^
   Register-ScheduledTask -TaskName 'VectorPod-Supervisor' -Action $a -Settings $s -RunLevel Highest -Force | Out-Null"
echo      Done.

REM ── Startup shortcut ─────────────────────────────────────────────────────────
echo [5/5] Adding startup entry...
set STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\start-vectormind.bat
echo @echo off > "%STARTUP%"
echo timeout /t 10 /nobreak ^>nul >> "%STARTUP%"
echo schtasks /run /tn "VectorPod-Supervisor" >> "%STARTUP%"
echo      Done.

REM ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo  Setup complete!
echo.
echo  Next steps:
echo    1. Edit vector-ai\.env if you need to set FFMPEG_PATH or YTDLP_PATH
echo    2. Connect Vector to wire-pod (see README.md Part 1)
echo    3. Run: schtasks /run /tn "VectorPod-Supervisor"
echo    4. Open:  http://localhost:8000/settings  (personality)
echo             http://localhost:8000/music      (music player)
echo.
pause
