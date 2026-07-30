# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml .
# [serve] pulls in fastapi + uvicorn — versions pinned in pyproject.toml,
# the single place dependencies are declared
RUN pip install --no-cache-dir -e .[serve]

COPY . .

# Run as a non-root user — the agent needs no filesystem writes at runtime
RUN useradd --create-home --uid 1001 agent && chown -R agent:agent /app
USER agent

EXPOSE 8080
CMD ["uvicorn", "deploy.cloud_run.app:app", "--host", "0.0.0.0", "--port", "8080"]
