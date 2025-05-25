# executors/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY ./executors/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files from the executors directory into the image's /app directory
# This includes your .py scripts and entrypoint.sh
COPY ./executors/ /app/

# Install dos2unix, run it on entrypoint.sh, then clean up apt cache
# This forcefully converts line endings to Unix (LF)
RUN apt-get update && \
    apt-get install -y dos2unix --no-install-recommends && \
    dos2unix /app/entrypoint.sh && \
    apt-get purge -y --auto-remove dos2unix && \
    rm -rf /var/lib/apt/lists/*

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]