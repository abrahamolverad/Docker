FROM python:3.11-slim
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip

COPY ./swing_gpt/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./swing_gpt/ /app/

# Default: run continuous RTH loop every 5 minutes
CMD ["python", "-u", "swing_GPT.py", "--rth", "--interval", "300", "--finalists", "150", "--send", "40"]

