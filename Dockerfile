FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN python -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && pip install --upgrade pip \
    && pip install .

ENV PATH="/opt/venv/bin:$PATH"

EXPOSE 8080
EXPOSE 9000

CMD ["uvicorn", "deft_indexer.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
