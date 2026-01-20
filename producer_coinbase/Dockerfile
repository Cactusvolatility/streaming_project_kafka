FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer_coinbase.py .

CMD ["python", "-u", "producer_coinbase.py"]