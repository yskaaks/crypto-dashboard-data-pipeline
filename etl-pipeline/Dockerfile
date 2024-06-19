FROM python:3.8

WORKDIR /app
COPY . .
RUN pip install poetry
RUN poetry install
CMD ["poetry", "run", "python", "etl_pipeline/main.py"]
