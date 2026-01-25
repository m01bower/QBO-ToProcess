@echo off
REM Setup script to create Windows virtual environment and install dependencies

cd /d "%~dp0"

echo Creating Windows virtual environment...
python -m venv venv-win

echo Activating virtual environment...
call venv-win\Scripts\activate.bat

echo Installing dependencies...
pip install -r requirements.txt

echo.
echo Setup complete! To activate the virtual environment, run:
echo     venv-win\Scripts\activate.bat
echo.
pause
