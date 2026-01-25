#!/bin/bash
# QBO ToProcess - Shell script runner
# Run with: ./run.sh or bash run.sh

cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv-linux" ]; then
    source venv-linux/bin/activate
fi

# Run the application
python src/main.py "$@"

# Capture exit code
EXIT_CODE=$?

# Deactivate virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

exit $EXIT_CODE
