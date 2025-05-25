# scanners/Dockerfile
FROM python:3.11-slim
WORKDIR /app

# Copy only the requirements.txt from the scanners directory first
COPY ./scanners/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the rest of the scanners application code
COPY ./scanners/ /app/

CMD ["python", "daytrade_genie.py"]
