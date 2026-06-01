@echo off
echo.
echo  VectorMind Updater
echo  ==================
echo.

cd /d "%~dp0"

REM Check if git is available
git --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Git not found. Install from https://git-scm.com
    pause & exit /b 1
)

REM Show current version
echo Current version:
git log --oneline -1
echo.

REM Pull latest
echo Pulling latest from GitHub...
git pull origin main

if errorlevel 1 (
    echo.
    echo [ERROR] Update failed. Check your internet connection.
    pause & exit /b 1
)

echo.
echo Update complete!
echo.

REM Offer to restart the supervisor if it's running
schtasks /query /tn "VectorPod-Supervisor" /fo LIST 2>nul | findstr "Running" >nul
if not errorlevel 1 (
    echo VectorPod-Supervisor is running.
    set /p restart="Restart it to apply changes? (y/n): "
    if /i "%restart%"=="y" (
        schtasks /end /tn "VectorPod-Supervisor"
        timeout /t 3 /nobreak >nul
        schtasks /run /tn "VectorPod-Supervisor"
        echo Supervisor restarted.
    )
)

echo.
pause
