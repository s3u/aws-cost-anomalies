FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
RUN pip install -e ".[dev]"
COPY . .
RUN pip install -e ".[dev]"
ENTRYPOINT ["aws-cost-anomalies"]
