FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY ingest_5years.py .

RUN pip install psycopg2-binary

CMD ["python", "ingest_5years.py"]