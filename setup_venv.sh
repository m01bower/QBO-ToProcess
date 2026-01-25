#!/bin/bash
# Setup script to create Linux virtual environment and install dependencies

cd "$(dirname "$0")"

echo "Creating Linux virtual environment..."
python3 -m venv venv-linux

echo "Activating virtual environment..."
source venv-linux/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "Setup complete! To activate the virtual environment, run:"
echo "    source venv-linux/bin/activate"
