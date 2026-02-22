# AWS Cost Anomalies

Detect cost anomalies across your AWS root and linked accounts. Ingests Cost and Usage Report (CUR) data from S3, stores it locally in DuckDB, analyzes daily trends, detects anomalies using statistical methods, and supports natural language queries via an agentic system powered by AWS Bedrock.

## Features

- **CUR Ingestion** — Download and load CUR v1/v2 parquet files from S3 into DuckDB with incremental updates
- **Trend Analysis** — Daily cost trends grouped by service, account, or region with day-over-day changes
- **Anomaly Detection** — Z-score based detection with configurable sensitivity and rolling windows
- **Natural Language Queries** — Ask questions about your costs in plain English via an agentic system that uses DuckDB, Cost Explorer, CloudWatch, Budgets, and Organizations

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- AWS credentials configured (for CUR access and Bedrock)
- An S3 bucket with CUR data in parquet format

## Installation

```bash
git clone <repo-url>
cd aws-cost-anomalies
uv sync --extra dev
```

Or with pip:

```bash
pip install -e ".[dev]"
```

### Docker

```bash
# Build the container
docker compose build

# Open a dev shell
docker compose run --rm app

# Or run commands directly
docker compose run --rm app aws-cost-anomalies ingest
```

## Configuration

Copy the example config and edit:

```bash
cp config.example.yaml config.yaml
```

```yaml
s3:
  bucket: your-cur-bucket-name
  prefix: cur-reports              # S3 prefix where CUR reports are stored
  report_name: your-report-name    # CUR report name as configured in AWS
  region: us-east-1

database:
  path: ./data/costs.duckdb       # DuckDB file path
  cache_dir: ./data/cache         # Local cache for downloaded parquet files

anomaly:
  rolling_window_days: 14         # Days of history for baseline
  z_score_threshold: 2.5          # Default z-score threshold (overridden by --sensitivity)
  min_daily_cost: 1.0             # Ignore groups with daily cost below this

nlq:
  model: us.anthropic.claude-sonnet-4-20250514-v1:0  # Bedrock model ID
  max_tokens: 4096
  region: us-east-1               # Bedrock region
  max_agent_iterations: 10        # Max tool-use loops before stopping
```

### Environment Variable Overrides

| Variable | Overrides |
|----------|-----------|
| `AWS_COST_DB_PATH` | `database.path` |
| `AWS_COST_CACHE_DIR` | `database.cache_dir` |
| `AWS_BEDROCK_REGION` | `nlq.region` (Bedrock region) |

An `.env.example` file is provided as a template.

## Usage

### Ingest CUR Data

```bash
# Ingest all available billing periods
aws-cost-anomalies ingest

# Ingest a specific month
aws-cost-anomalies ingest --date 2025-01

# Force re-ingest everything
aws-cost-anomalies ingest --full-refresh

# With custom config
aws-cost-anomalies ingest --config /path/to/config.yaml
```

### View Trends

```bash
# Default: last 14 days, grouped by service, top 10
aws-cost-anomalies trends

# Last 30 days, grouped by account, top 5
aws-cost-anomalies trends --days 30 --group-by account --top 5

# Group by region
aws-cost-anomalies trends --group-by region
```

### Detect Anomalies

```bash
# Default: medium sensitivity, 14-day window, by service
aws-cost-anomalies anomalies

# High sensitivity (lower threshold, catches more)
aws-cost-anomalies anomalies --sensitivity high

# Low sensitivity (higher threshold, only major anomalies)
aws-cost-anomalies anomalies --sensitivity low --days 30

# Check by account
aws-cost-anomalies anomalies --group-by account
```

Sensitivity presets:
- **low** — z-score > 3.0 (fewer, more significant anomalies)
- **medium** — z-score > 2.5 (balanced, default)
- **high** — z-score > 2.0 (more anomalies, more noise)

### Natural Language Queries

Requires AWS credentials with Bedrock access.

```bash
# Ask a one-off question
aws-cost-anomalies query "What are my top 5 most expensive services?"

# Interactive REPL mode
aws-cost-anomalies query --interactive
```

The agent can query the local DuckDB database, call AWS Cost Explorer, check CloudWatch alarms, inspect Budgets, and look up Organization account names. Tool calls are displayed inline as the agent reasons.

## How It Works

### Ingestion

1. Lists billing period folders in your CUR S3 bucket
2. Downloads `manifest.json` for each period to get data file locations
3. Compares assembly IDs against previously ingested data to detect changes
4. Downloads new/updated parquet files to local cache
5. Loads into DuckDB with automatic CUR v1/v2 column mapping
6. Rebuilds pre-aggregated daily cost summary table

### Anomaly Detection

Uses a modified z-score algorithm over a rolling window:

1. For each group (service/account/region), fetches daily costs for the last N days
2. Uses all days except the most recent as the baseline
3. Computes `z_score = (current_day - mean) / stddev` against the baseline
4. Flags anomalies where `|z_score|` exceeds the sensitivity threshold
5. Classifies severity: critical (`|z| > 4`), warning (`|z| > 3`), info

### Data Schema

- **cost_line_items** — Raw CUR data with normalized column names (21 cost/usage columns)
- **daily_cost_summary** — Pre-aggregated daily totals by account, service, and region
- **ingestion_log** — Tracks ingested files for incremental updates

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run unit tests (128 tests, evals excluded by default)
uv run pytest

# Run with verbose output
uv run pytest -v

# Lint
uv run ruff check src/ tests/

# Auto-fix lint issues
uv run ruff check --fix src/ tests/
```

### Agent Correctness Evals

The project includes 10 correctness evals that run the NLQ agent against a deterministic DuckDB fixture and assert the answers are **numerically correct** — not just keyword matches. These require live AWS Bedrock credentials.

```bash
# Run evals (requires Bedrock credentials)
uv run pytest -m evals -v
```

Evals verify:
- Total spend matches expected values (±5% tolerance)
- Service/account/region rankings are correct
- Dollar amounts are accurate, not hallucinated
- No-data ranges produce "no data" answers, not fabricated costs
- Flat cost data is reported as stable, not trending

Evals are excluded from the default `pytest` run via the `addopts = "-m 'not evals'"` setting in `pyproject.toml`. Tests skip automatically if Bedrock credentials are unavailable.

### Generate Sample Data

A script is provided to create 90 days of realistic CUR parquet data for local testing:

```bash
uv run python scripts/generate_sample_data.py
```

### Project Structure

```
src/aws_cost_anomalies/
├── cli/           # Typer CLI commands + Rich formatting
├── config/        # YAML config loader with validation
├── ingestion/     # S3 client, manifest parser, parquet loader
├── storage/       # DuckDB connection + schema management
├── analysis/      # Trend aggregation + anomaly detection
├── nlq/           # Bedrock-powered agentic NLQ system
└── utils/         # Date helpers
```

See [docs/SYSTEM_DESIGN.md](docs/SYSTEM_DESIGN.md) for detailed architecture documentation.

## License

Private — internal use only.
