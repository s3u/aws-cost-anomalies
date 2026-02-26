# AWS Cost Anomalies

Detect cost anomalies across your AWS root and linked accounts. Ingests cost data from Cost Explorer API or CUR (S3), stores it locally in DuckDB, analyzes daily trends, detects anomalies using statistical methods, and supports natural language queries via an agentic system powered by AWS Bedrock.

## Features

- **Dual Ingestion** — Import cost data from the Cost Explorer API (quick, no S3 setup) or CUR v1/v2 parquet files from S3 (full detail). Both sources coexist in the same database.
- **Agent-Driven Workflow** — The agent can import data, query it, and call AWS APIs. On first run with an empty database, the agent offers to import Cost Explorer data automatically.
- **Trend Analysis** — Daily cost trends grouped by service, account, or region with day-over-day changes, plus flexible time-series queries with daily/weekly/monthly granularity
- **Anomaly Detection** — Robust median/MAD z-scores for point anomalies and Theil-Sen slope for gradual drift, with configurable sensitivity, rolling windows, and comprehensive anomaly explanations (baseline stats, ongoing detection, usage-type attribution)
- **Natural Language Queries** — Ask questions about your costs in plain English via an agentic system that uses DuckDB, Cost Explorer, CloudWatch, Budgets, and Organizations
- **MCP Server Support** — Extend the agent with any [Model Context Protocol](https://modelcontextprotocol.io/) server (e.g. CloudTrail for "who launched that instance?")

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- AWS credentials configured (for Cost Explorer API and/or Bedrock)
- For CUR ingestion: an S3 bucket with CUR data in parquet format (optional — Cost Explorer works without S3)

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
docker compose run --rm app aws-cost-anomalies ingest --source cost-explorer
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

cost_explorer:
  region: us-east-1               # AWS region for Cost Explorer API
  lookback_days: 14               # Default days to look back (max 365)

database:
  path: ./data/costs.duckdb       # DuckDB file path
  cache_dir: ./data/cache         # Local cache for downloaded parquet files

anomaly:
  rolling_window_days: 14         # Days of history for baseline
  z_score_threshold: 2.5          # Default z-score threshold (overridden by --sensitivity)
  min_daily_cost: 1.0             # Ignore groups with daily cost below this

agent:
  model: us.anthropic.claude-sonnet-4-20250514-v1:0  # Bedrock model ID
  max_tokens: 4096
  region: us-east-1               # Bedrock region
  max_agent_iterations: 10        # Max tool-use loops before stopping

  # Optional: extend the agent with MCP servers
  # mcp_servers:
  #   - name: cloudtrail
  #     command: uvx
  #     args: [awslabs.cloudtrail-mcp-server@latest]
  #     env_passthrough: [AWS_PROFILE, AWS_DEFAULT_REGION]
```

### Cross-Account Setup

If your cost data lives in a root/management account but you run Bedrock from a dev account, use named AWS profiles:

```yaml
aws_profile: "root-readonly"    # Used for CE, S3, CloudWatch, Budgets, Orgs

agent:
  profile: "dev-bedrock"        # Used for Bedrock only; falls back to aws_profile
```

- `aws_profile` applies to all AWS API calls **except** Bedrock (env override: `AWS_COST_PROFILE`)
- `agent.profile` applies to Bedrock only; if omitted, falls back to `aws_profile` (env override: `AWS_BEDROCK_PROFILE`)
- When both are empty (default), the standard credential chain is used (identical to previous behavior)
- `AWS_PROFILE` continues to work natively via boto3

### Environment Variable Overrides

| Variable | Overrides |
|----------|-----------|
| `AWS_COST_PROFILE` | `aws_profile` (cost-data AWS profile) |
| `AWS_BEDROCK_PROFILE` | `agent.profile` (Bedrock AWS profile) |
| `AWS_COST_DB_PATH` | `database.path` |
| `AWS_COST_CACHE_DIR` | `database.cache_dir` |
| `AWS_BEDROCK_REGION` | `agent.region` (Bedrock region) |
| `AWS_COST_EXPLORER_REGION` | `cost_explorer.region` |

An `.env.example` file is provided as a template.

## Usage

### Quick Start (No S3 Required)

The fastest way to get started is to use the agent — it will offer to import Cost Explorer data on first run:

```bash
# Start an interactive session — agent auto-detects empty DB
uv run aws-cost-anomalies query -i

# Or import Cost Explorer data directly via CLI
uv run aws-cost-anomalies ingest --source cost-explorer --days 30
```

### Ingest Data

```bash
# Import from Cost Explorer API (no S3 setup needed)
uv run aws-cost-anomalies ingest --source cost-explorer
uv run aws-cost-anomalies ingest --source cost-explorer --days 90

# Import CUR data from S3 (requires S3 config)
uv run aws-cost-anomalies ingest
uv run aws-cost-anomalies ingest --source cur --date 2025-01
uv run aws-cost-anomalies ingest --source cur --full-refresh
```

Both sources coexist — ingesting one does not replace the other. The `daily_cost_summary` table has a `data_source` column (`'cur'` or `'cost_explorer'`) to distinguish them.

### View Trends

```bash
# Default: last 14 days, grouped by service, top 10
uv run aws-cost-anomalies trends

# Last 30 days, grouped by account, top 5
uv run aws-cost-anomalies trends --days 30 --group-by account --top 5

# Only Cost Explorer data
uv run aws-cost-anomalies trends --source cost-explorer

# Only CUR data
uv run aws-cost-anomalies trends --source cur
```

### Detect Anomalies

```bash
# Default: medium sensitivity, 14-day window, by service
uv run aws-cost-anomalies anomalies

# High sensitivity (lower threshold, catches more)
uv run aws-cost-anomalies anomalies --sensitivity high

# Low sensitivity with specific data source
uv run aws-cost-anomalies anomalies --sensitivity low --days 30 --source cost-explorer

# Check by account
uv run aws-cost-anomalies anomalies --group-by account
```

Sensitivity presets:
- **low** — z-score > 3.0 (fewer, more significant anomalies)
- **medium** — z-score > 2.5 (balanced, default)
- **high** — z-score > 2.0 (more anomalies, more noise)

### Natural Language Queries

Requires AWS credentials with Bedrock access.

```bash
# Ask a one-off question
uv run aws-cost-anomalies query "What are my top 5 most expensive services?"

# Interactive REPL mode
uv run aws-cost-anomalies query --interactive
```

The agent can:
- **Query** the local DuckDB database (CUR and/or Cost Explorer data)
- **Import data** from Cost Explorer or CUR on demand
- **Call AWS APIs** — Cost Explorer (real-time), CloudWatch, Budgets, Organizations
- **Detect & explain anomalies** — statistical detection, comprehensive explanations with baseline stats and ongoing checks
- **Compare periods** — period-over-period comparison and line-item attribution of cost changes
- **Analyze trends** — time-series with daily/weekly/monthly granularity and summary stats
- **Auto-bootstrap** — on first run with an empty DB, offers to import Cost Explorer data

On first run with no data, the agent detects the empty database and offers to import Cost Explorer data. No separate `ingest` step is required.

#### MCP Server Integration

To extend the agent with external tools (e.g. CloudTrail), install the MCP extra and add servers to your config:

```bash
uv sync --extra mcp    # or: pip install "aws-cost-anomalies[mcp]"
```

Then add `mcp_servers` entries under `agent` in `config.yaml` (see [Configuration](#configuration) above). The agent will automatically discover MCP tools at startup and use them when relevant.

## How It Works

### Data Sources

The tool supports two data sources that coexist in the same database:

| Source | Setup | Detail Level |
|--------|-------|-------------|
| **Cost Explorer** | Just AWS credentials | Daily costs by service and account (unblended, blended, net amortized). No region, no usage amounts. |
| **CUR (S3)** | S3 bucket with CUR reports | Full detail: region, resource-level, usage types, line items. |

The `daily_cost_summary` table has a `data_source` column to distinguish them. CLI commands accept `--source` to filter by source.

### Ingestion

**Cost Explorer path:**
1. Calls `ce:GetCostAndUsage` with DAILY granularity, grouped by SERVICE and LINKED_ACCOUNT, fetching UnblendedCost, BlendedCost, and NetAmortizedCost
2. Paginates through all results
3. Maps CE service names to CUR product codes for consistency
4. Replaces existing Cost Explorer rows in `daily_cost_summary` (CUR data is preserved)

**CUR path:**
1. Lists billing period folders in your CUR S3 bucket
2. Downloads `manifest.json` for each period to get data file locations
3. Compares assembly IDs against previously ingested data to detect changes
4. Downloads new/updated parquet files to local cache
5. Loads into DuckDB with automatic CUR v1/v2 column mapping
6. Rebuilds pre-aggregated daily cost summary table (Cost Explorer data is preserved)

### Anomaly Detection

Uses two complementary techniques over a rolling window:

**Point anomalies** (sudden spikes/drops) — median/MAD modified z-scores:
1. For each group (service/account/region), fetches daily costs for the last N days
2. Builds a baseline from all days except the most recent
3. Computes a modified z-score using median and MAD (robust to baseline outliers)
4. Flags anomalies where `|z_score|` exceeds the sensitivity threshold
5. Classifies severity: critical (`|z| > 4`), warning (`|z| > 3`), info

**Trend anomalies** (gradual drift) — Theil-Sen slope estimation:
1. Computes the robust Theil-Sen slope over the full window
2. Flags when total drift exceeds the drift threshold (default 20%)

See [docs/ANOMALIES.md](docs/ANOMALIES.md) for full details.

### Data Schema

- **cost_line_items** — Raw CUR data with normalized column names (21 cost/usage columns)
- **daily_cost_summary** — Pre-aggregated daily totals by account, service, region, and data source (includes unblended, blended, and net amortized costs)
- **ingestion_log** — Tracks ingested files for incremental updates

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run unit tests (245 tests, evals excluded by default)
uv run pytest

# Run with verbose output
uv run pytest -v

# Lint
uv run ruff check src/ tests/

# Auto-fix lint issues
uv run ruff check --fix src/ tests/
```

### Agent Correctness Evals

The project includes 18 correctness evals that run the agent against a deterministic DuckDB fixture and assert the answers are **numerically correct** — not just keyword matches. These require live AWS Bedrock credentials.

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
uv run scripts/generate_sample_data.py
```

### Project Structure

```
src/aws_cost_anomalies/
├── cli/           # Typer CLI commands + Rich formatting
├── config/        # YAML config loader with validation
├── ingestion/     # S3 client, Cost Explorer client, manifest parser, parquet loader
├── storage/       # DuckDB connection + schema management
├── analysis/      # Trend aggregation, anomaly detection, cost attribution, explanations
├── agent/         # Bedrock-powered agentic system
└── utils/         # Date helpers
```

See [docs/SYSTEM_DESIGN.md](docs/SYSTEM_DESIGN.md) for detailed architecture documentation and [docs/ANOMALIES.md](docs/ANOMALIES.md) for how anomaly detection works.

## License

Private — internal use only.
