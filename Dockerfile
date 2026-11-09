FROM python:3.12-slim
WORKDIR /app
COPY src ./src
COPY benchmarks ./benchmarks
COPY examples ./examples
COPY tests ./tests
COPY pyproject.toml ./
RUN pip install --no-cache-dir . pytest
ENV PYTHONPATH=/app/src
CMD ["python", "examples/run_panel_analysis.py", "--teams", "24", "--periods", "12", "--bootstrap", "200", "--output", "/data/panel-analysis-results.json", "--csv", "/data/synthetic-panel.csv"]
