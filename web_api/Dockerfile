# web_api/Dockerfile
FROM python:3.11-slim
WORKDIR /app

# Copy only the requirements.txt from the web_api directory first
COPY ./web_api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the web_api application code
COPY ./web_api/ /app/

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
