# AWS Cost Anomalies — Agent Guide

## Project Overview
CLI tool to detect AWS cost anomalies across root and linked accounts. Downloads CUR (Cost & Usage Reports) from S3, stores in DuckDB, analyzes trends, detects anomalies with z-score, and supports natural language queries via an agentic system powered by AWS Bedrock.

## Quick Start
```bash
# Development (with Rancher Desktop)
docker compose run --rm app          # dev shell
docker compose run --rm app pytest   # run tests
docker compose run --rm app ruff check src/ tests/  # lint

# Local development (without Docker)
uv venv --python 3.12 .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest
```

## Project Structure
- `src/aws_cost_anomalies/` — main package
  - `cli/` — Typer commands (ingest, trends, anomalies, query)
  - `config/` — YAML config loading
  - `ingestion/` — S3 CUR download + parquet loading
  - `storage/` — DuckDB connection + schema
  - `analysis/` — trend aggregation + z-score anomaly detection
  - `nlq/` — Bedrock-powered agentic NLQ system
    - `agent.py` — Agent loop: Bedrock Converse → tool dispatch → loop
    - `bedrock_client.py` — Boto3 bedrock-runtime wrapper
    - `tools.py` — Tool definitions (DuckDB, Cost Explorer, CloudWatch, Budgets, Organizations)
    - `executor.py` — SQL validation + safe execution
    - `prompts.py` — Agent system prompt + schema description
  - `utils/` — date helpers
- `tests/` — pytest tests organized by module

## CLI Commands
```
aws-cost-anomalies ingest    [--config] [--date YYYY-MM] [--full-refresh]
aws-cost-anomalies trends    [--config] [--days 14] [--group-by service|account|region] [--top 10]
aws-cost-anomalies anomalies [--config] [--days 14] [--sensitivity low|medium|high] [--group-by ...]
aws-cost-anomalies query     [--config] [--interactive] "question text"
```

## Key Architecture Decisions
- **DuckDB** for embedded OLAP: no external database needed, fast aggregations
- **Parquet-first**: CUR data loaded via DuckDB's `read_parquet()` with column mapping for both CUR v1 and v2 formats
- **Incremental ingestion**: tracks `assemblyId` per billing period to avoid re-ingesting unchanged data
- **Z-score anomaly detection**: modified z-score over rolling window with configurable sensitivity
- **Agentic NLQ**: Bedrock Converse API with tool-use loop — agent can query DuckDB, Cost Explorer, CloudWatch, Budgets, and Organizations
- **SQL safety**: NLQ executor validates all queries are read-only before execution

## Configuration
Copy `config.example.yaml` to `config.yaml`. Environment variables override config:
- `AWS_COST_DB_PATH` — DuckDB file path
- `AWS_COST_CACHE_DIR` — local parquet cache
- `AWS_BEDROCK_REGION` — AWS region for Bedrock (default: us-east-1)
- `AWS_PROFILE` — AWS credentials profile

## Testing
```bash
pytest                        # all tests
pytest tests/test_analysis/   # just analysis tests
pytest -v                     # verbose output
```

Tests use in-memory DuckDB with synthetic data fixtures. S3 tests use moto mocking. Bedrock/AWS API tests use mock responses.
