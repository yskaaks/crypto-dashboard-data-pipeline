FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

COPY poetry.lock pyproject.toml /app/

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

COPY . /app

# Run setup_aws_creds.py first, then main.py
CMD ["sh", "-c", "poetry run python setup_aws_creds.py && poetry run python etl_pipeline/main.py"]