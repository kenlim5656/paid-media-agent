# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

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
