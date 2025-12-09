FROM python:3.11-slim

# Keep Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Turn off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install pip requirements
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# --- CRITICAL CHANGE HERE ---
# We removed the old "swing_GPT.py" command.
# We removed the flags (--rth, --interval) because the new bot is autonomous.
# Ensure your Python file is named 'main.py' or update the name below.
CMD ["python", "-u", "SHORTKING_LIVE_RISK_FULL_V7.py"]
