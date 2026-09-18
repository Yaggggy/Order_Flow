# Pin images by digest in a deployment promotion workflow.
FROM python:3.12.11-slim-bookworm AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --create-home app
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY --chown=app:app orderflow /app/orderflow
COPY --chown=app:app services /app/services
USER app

FROM base AS runtime
EXPOSE 8000 9101 9102
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS development
USER root
COPY requirements-dev.lock /app/requirements-dev.lock
RUN pip install --no-cache-dir --require-hashes -r requirements-dev.lock
COPY --chown=app:app . /app
USER app
CMD ["sleep", "infinity"]
