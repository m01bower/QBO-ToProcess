@echo off
REM QBO ToProcess - Batch file for Task Scheduler
REM This file can be used with Windows Task Scheduler for automated runs

cd /d "%~dp0"

REM Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Run with --all flag for scheduled execution (no GUI)
REM Remove --all to show client selector dialog
python src\main.py --all

REM Capture exit code
set EXIT_CODE=%ERRORLEVEL%

REM Deactivate virtual environment
if exist "venv\Scripts\deactivate.bat" (
    call deactivate
)

exit /b %EXIT_CODE%
