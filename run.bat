@echo off
REM QBO ToProcess - Batch file runner
REM This file can be used with Windows Task Scheduler for automated runs

cd /d "%~dp0"

REM Activate virtual environment if it exists
if exist "venv-win\Scripts\activate.bat" (
    call venv-win\Scripts\activate.bat
)

REM Run the application
python src\main.py %*

REM Capture exit code
set EXIT_CODE=%ERRORLEVEL%

REM Deactivate virtual environment
if exist "venv-win\Scripts\deactivate.bat" (
    call deactivate
)

exit /b %EXIT_CODE%
