# ---- base image -----------------------------------------------------------
    FROM python:3.11-slim

    # ---- copy code ------------------------------------------------------------
    WORKDIR /app
    COPY . /app
    
    # ---- install Python deps --------------------------------------------------
    RUN pip install --no-cache-dir -r requirements.txt
    
    # ---- generate the fresh S&P-500 list --------------------------------------
    RUN python generate_sp500_list.py   # creates /app/sp500_symbols.txt
    
    # ---- runtime settings -----------------------------------------------------
    ENV PYTHONUNBUFFERED=1
    CMD ["python", "alpaca_base.py"]     # or the specific executor script
    