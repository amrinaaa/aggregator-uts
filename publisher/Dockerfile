# publisher/Dockerfile
FROM python:3.11-slim

# Set environment agar output log langsung muncul
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Tambahkan user non-root
RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Copy dan install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY publisher.py .

# Jalankan publisher
CMD ["python", "publisher.py"]
