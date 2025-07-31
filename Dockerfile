FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config.py .

# Wait for services then start consumer
CMD ["python", "-m", "src.consumer"]
