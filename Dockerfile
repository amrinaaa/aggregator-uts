FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
WORKDIR /app

# buat user non-root
RUN adduser --disabled-password --gecos '' appuser

# buat direktori data
RUN mkdir /app/data && chown -R appuser:appuser /app/data

# Copy requirements & install dependencies (termasuk testing)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh source code
COPY src/ ./src/
COPY tests/ ./tests/

# Set permission
RUN chown -R appuser:appuser /app

USER appuser
EXPOSE 8080

# Jalankan FastAPI saat container aktif
CMD ["python", "-m", "src.main"]
