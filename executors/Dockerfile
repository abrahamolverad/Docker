# executors/Dockerfile
FROM python:3.11-slim # Or your preferred Python version

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY ./executors/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files from the executors directory into the image's /app directory
# This will include your .py scripts and the new entrypoint.sh
COPY ./executors/ /app/

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# You don't need a CMD here if the entrypoint handles the logic or
# if EXECUTOR_SCRIPT will always be provided.
# If you wanted a default script if EXECUTOR_SCRIPT wasn't set,
# your entrypoint.sh would need to handle that logic.