FROM python:3.12-slim
WORKDIR /app
COPY src ./src
COPY benchmarks ./benchmarks
COPY tests ./tests
COPY pyproject.toml ./
RUN pip install --no-cache-dir . pytest
ENV PYTHONPATH=/app/src
CMD ["python", "benchmarks/run_benchmark.py", "--ops", "10000", "--target-rate", "10000", "--output", "/data/benchmark-results.json", "--wal", "/data/engine.wal"]
