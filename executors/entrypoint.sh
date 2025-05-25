#!/bin/sh
# Ensuring LF line endings with this new comment
# Exit immediately if a command exits with a non-zero status.
set -e
# ... rest of your script
if [ -z "" ]; then
  echo "Error: EXECUTOR_SCRIPT environment variable is not set."
  exit 1
fi

echo "Attempting to run Python script: "

# Check if the script exists and is readable
if [ ! -f "" ]; then
    echo "Error: Script  not found in /app/ directory."
    ls -la /app/ # List directory contents for debugging
    exit 1
fi

# Execute the specified Python script
# The "-u" flag ensures that Python output is unbuffered, which is good for logging.
exec python -u ""
