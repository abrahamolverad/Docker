# meta_ai/Dockerfile
FROM python:3.11-slim
WORKDIR /app

# Copy only the requirements.txt from the meta_ai directory first
COPY ./meta_ai/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the meta_ai application code
COPY ./meta_ai/ /app/

CMD ["python", "strategy_router.py"]
