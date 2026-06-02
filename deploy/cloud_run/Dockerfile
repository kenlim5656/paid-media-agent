FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

COPY . .

# Cloud Run expects a web server; we use a minimal FastAPI wrapper
# that accepts POST /run?agent=watchdog|analyst|operator
RUN pip install --no-cache-dir fastapi uvicorn[standard]

EXPOSE 8080
CMD ["uvicorn", "deploy.cloud_run.app:app", "--host", "0.0.0.0", "--port", "8080"]
