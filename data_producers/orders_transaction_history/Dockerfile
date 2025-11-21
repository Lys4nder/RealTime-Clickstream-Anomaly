FROM python:3.13-slim

WORKDIR /app

COPY src/historic_dataset_generation.py /app/historic_dataset_generation.py

RUN pip install --no-cache-dir pandas numpy pyarrow

ENV OUTPUT_PATH=/data/Orders_Transaction_History.parquet

CMD ["python", "/app/historic_dataset_generation.py"]
