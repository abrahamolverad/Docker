#!/bin/sh
# Entrypoint for executor services

set -e

if [ -z "$EXECUTOR_SCRIPT" ]; then
  echo "Error: EXECUTOR_SCRIPT environment variable is not set. This script requires it."
  exit 1
fi

echo "Attempting to run Python script specified by EXECUTOR_SCRIPT: [$EXECUTOR_SCRIPT]"

# Check if the target Python script exists in /app/
if [ ! -f "/app/$EXECUTOR_SCRIPT" ]; then
    echo "Error: Target Python script /app/$EXECUTOR_SCRIPT not found."
    echo "Contents of /app/ directory:"
    ls -la /app/
    exit 1
fi

exec python -u "$EXECUTOR_SCRIPT"