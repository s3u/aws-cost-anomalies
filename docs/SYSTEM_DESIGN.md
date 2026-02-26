# System Design — AWS Cost Anomalies

## Overview

AWS Cost Anomalies is a Python CLI tool that ingests AWS cost data from the Cost Explorer API or CUR (Cost & Usage Reports) from S3, stores it in an embedded DuckDB database, detects cost anomalies using robust statistical methods (median/MAD z-scores, Theil-Sen drift), and supports natural language queries via an agentic system powered by AWS Bedrock. All output is terminal-only via Rich tables.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          CLI Layer (Typer)                      │
│    ingest   │   trends   │   anomalies   │   query (agent)      │
├─────────────┼────────────┼───────────────┼──────────────────────┤
│  Ingestion  │       Analysis Layer       │     Agent Layer      │
│  Pipeline   │  trends.py │ anomalies.py  │ agent.py             │
│  s3_client  │            │               │ bedrock_client.py    │
│  manifest   │            │               │ tools.py             │
│  loader     │            │               │ executor.py          │
├─────────────┴────────────┴───────────────┴──────────────────────┤
│                     Storage Layer (DuckDB)                      │
│  database.py         schema.py                                  │
├─────────────────────────────────────────────────────────────────┤
│                     Config Layer (YAML + env)                   │
│  settings.py                                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Package Structure

```
src/aws_cost_anomalies/
├── cli/                  # Typer commands + Rich formatting
│   ├── app.py            # Typer app root, main() entry point
│   ├── ingest.py         # `ingest` command
│   ├── trends.py         # `trends` command
│   ├── anomalies.py      # `anomalies` command
│   ├── query.py          # `query` command (agent REPL)
│   └── formatting.py     # Rich table helpers
├── config/
│   └── settings.py       # YAML + env var config loader
├── ingestion/
│   ├── cost_explorer.py  # Cost Explorer API client
│   ├── s3_client.py      # S3 listing/downloading
│   ├── manifest.py       # CUR manifest.json parser
│   └── loader.py         # Parquet → DuckDB with column mapping
├── storage/
│   ├── database.py       # DuckDB connection factory
│   └── schema.py         # DDL, indexes, daily summary rebuild
├── analysis/
│   ├── trends.py         # Daily trend aggregation + time-series queries
│   ├── anomalies.py      # Z-score anomaly detection + historical scanning
│   ├── comparison.py     # Period-over-period cost comparison
│   ├── drilldown.py      # Cost spike drill-down by usage_type/resource
│   ├── attribution.py    # Line-item attribution between two periods
│   └── explainer.py      # Comprehensive anomaly explanation
├── agent/
│   ├── agent.py          # Agentic loop: Bedrock Converse → tool dispatch
│   ├── bedrock_client.py # Boto3 bedrock-runtime wrapper
│   ├── tools.py          # 14 tool definitions + executors
│   ├── executor.py       # SQL validation + safe execution
│   ├── prompts.py        # Agent system prompt + schema description
│   └── mcp_bridge.py     # MCP server integration
└── utils/
    └── dates.py          # Date range helpers
```

---

## Configuration System

**File:** `config/settings.py`

Config is resolved with this priority: **environment variables > YAML file > defaults**.

### Dataclasses

| Class | Key Fields | Defaults |
|-------|-----------|----------|
| `S3Config` | bucket, prefix, report_name, region | region=`us-east-1` |
| `DatabaseConfig` | path, cache_dir | `./data/costs.duckdb`, `./data/cache` |
| `AnomalyConfig` | rolling_window_days, z_score_threshold, min_daily_cost | 14, 2.5, 1.0 |
| `AgentConfig` | model, max_tokens, region, max_agent_iterations | `us.anthropic.claude-sonnet-4-20250514-v1:0`, 4096, `us-east-1`, 10 |

### Config Loading Flow

```
load_settings(config_path)
  ├── If config_path provided → read + parse YAML (ConfigError if missing)
  ├── Elif config.yaml exists in cwd → read + parse YAML
  ├── Else → use defaults
  ├── Apply env var overrides:
  │     AWS_COST_DB_PATH → database.path
  │     AWS_COST_CACHE_DIR → database.cache_dir
  │     AWS_BEDROCK_REGION → agent.region
  └── Validate types + ranges via _safe_int(), _safe_float()
```

### Validation

- `rolling_window_days` must be an integer >= 1
- `z_score_threshold` must be a float >= 0.1
- `min_daily_cost` must be a float >= 0.0
- `max_tokens` must be an integer >= 1
- All raise `ConfigError` with descriptive messages

---

## DuckDB Schema

**File:** `storage/schema.py`

### Tables

#### `cost_line_items` — Raw CUR line items (normalized columns)

| Column | Type | Description |
|--------|------|-------------|
| line_item_id | VARCHAR | Unique line item identifier |
| usage_start_date | TIMESTAMP | Start of usage period |
| usage_end_date | TIMESTAMP | End of usage period |
| billing_period_start | DATE | Billing period start |
| billing_period_end | DATE | Billing period end |
| payer_account_id | VARCHAR | Root/payer account ID |
| usage_account_id | VARCHAR | Linked account ID |
| product_code | VARCHAR | AWS service (e.g., AmazonEC2) |
| product_name | VARCHAR | Human-readable service name |
| region | VARCHAR | AWS region |
| availability_zone | VARCHAR | AZ within region |
| usage_type | VARCHAR | Usage type descriptor |
| operation | VARCHAR | API operation |
| resource_id | VARCHAR | Resource ARN |
| line_item_type | VARCHAR | Usage, Tax, Fee, Credit, etc. |
| unblended_cost | DOUBLE | Unblended cost |
| blended_cost | DOUBLE | Blended cost |
| net_unblended_cost | DOUBLE | Net unblended cost |
| usage_amount | DOUBLE | Usage quantity |
| currency_code | VARCHAR | Currency (e.g., USD) |
| line_item_description | VARCHAR | Line item description |
| _ingested_at | TIMESTAMP | Ingestion timestamp (auto) |
| _source_file | VARCHAR | Source S3 key |

#### `daily_cost_summary` — Pre-aggregated daily totals

| Column | Type | Description |
|--------|------|-------------|
| usage_date | DATE | Date of usage |
| usage_account_id | VARCHAR | Linked account ID |
| product_code | VARCHAR | AWS service |
| region | VARCHAR | AWS region |
| total_unblended_cost | DOUBLE | Sum of unblended costs |
| total_blended_cost | DOUBLE | Sum of blended costs |
| total_net_amortized_cost | DOUBLE | Sum of net amortized costs (from CE: NetAmortizedCost; from CUR: net_unblended_cost) |
| total_usage_amount | DOUBLE | Sum of usage amounts |
| line_item_count | BIGINT | Number of line items |
| data_source | VARCHAR | `'cur'` or `'cost_explorer'` |

Rebuilt from `cost_line_items` during CUR ingestion (preserves Cost Explorer rows). **Excludes**: Tax, Fee, Credit, Refund, BundledDiscount line item types.

#### `ingestion_log` — Tracks ingested files

| Column | Type | Description |
|--------|------|-------------|
| assembly_id | VARCHAR | CUR assembly ID |
| billing_period | VARCHAR | e.g., `20250101-20250201` |
| s3_key | VARCHAR | Source S3 key |
| rows_loaded | BIGINT | Number of rows loaded |
| ingested_at | TIMESTAMP | When ingested |

### Indexes

| Index | On | Purpose |
|-------|----|---------|
| idx_cli_usage_date | cost_line_items(usage_start_date) | Summary rebuild |
| idx_cli_billing_period | cost_line_items(billing_period_start) | Period deletion |
| idx_dcs_date | daily_cost_summary(usage_date) | Trend/anomaly queries |
| idx_dcs_product | daily_cost_summary(product_code) | Group-by queries |
| idx_dcs_account | daily_cost_summary(usage_account_id) | Group-by queries |
| idx_dcs_source | daily_cost_summary(data_source) | Source filtering |
| idx_il_period | ingestion_log(billing_period) | Incremental checks |

### `rebuild_daily_summary(conn)`

Deletes CUR rows from `daily_cost_summary` (preserving Cost Explorer rows), then re-aggregates from `cost_line_items` with `GROUP BY (date, account, service, region)`. Filters out non-usage line item types.

---

## Ingestion Pipeline

**Files:** `ingestion/s3_client.py`, `ingestion/manifest.py`, `ingestion/loader.py`, `cli/ingest.py`

### Cost Explorer Flow

```
uv run aws-cost-anomalies ingest --source cost-explorer [--days N]
  │
  ├── 1. Load config, open DuckDB, create tables
  ├── 2. Call ce:GetCostAndUsage (DAILY, grouped by SERVICE + LINKED_ACCOUNT)
  │      Metrics: UnblendedCost, BlendedCost, NetAmortizedCost
  ├── 3. Paginate, map CE service names → CUR product_code
  ├── 4. Filter zero-cost entries (all three metrics < 0.001)
  └── 5. insert_cost_explorer_summary() — replaces CE rows in date range, preserves CUR data
```

### CUR Flow

```
uv run aws-cost-anomalies ingest [--source cur] [--date YYYY-MM] [--full-refresh]
  │
  ├── 1. Load config, open DuckDB, create tables
  │
  ├── 2. CURBrowser.list_billing_periods()
  │      └── Paginate S3 under {prefix}/{report_name}/
  │          Filter folders matching YYYYMMDD-YYYYMMDD pattern
  │
  ├── 3. For each billing period:
  │      ├── CURBrowser.get_manifest(period)
  │      │     └── Find and parse *-Manifest.json or manifest.json
  │      │
  │      ├── Check ingestion_log for existing assembly_id
  │      │     ├── Same assembly_id → skip (already current)
  │      │     └── Different assembly_id → delete old data, re-ingest
  │      │
  │      ├── For each reportKey in manifest:
  │      │     ├── download_file() → local cache (skip if exists)
  │      │     ├── load_parquet_file() → INSERT into cost_line_items
  │      │     └── record_ingestion() → log to ingestion_log
  │      │
  │      └── Individual file failures don't abort the pipeline
  │
  └── 4. rebuild_daily_summary() → re-aggregate daily totals
```

### CUR Version Detection

**File:** `ingestion/loader.py`

AWS CUR exists in two column naming formats:

| CUR v1 (Legacy) | CUR v2 | Normalized |
|-----------------|--------|------------|
| `lineItem/UnblendedCost` | `line_item_unblended_cost` | `unblended_cost` |
| `lineItem/UsageStartDate` | `line_item_usage_start_date` | `usage_start_date` |
| `product/ProductCode` | `product_product_code` | `product_code` |

Detection: if any column name contains `/`, it's v1; otherwise v2.

The loader reads the parquet schema via DuckDB's `parquet_schema()` function, maps source columns to normalized target names, and fills missing optional columns with NULL. Three columns are **required**: `usage_start_date`, `line_item_type`, `unblended_cost`.

### S3 Error Handling

`CURBrowser` wraps all S3 errors into `S3Error` with user-friendly messages:

| AWS Error | User Message |
|-----------|-------------|
| NoCredentialsError | "Configure credentials via AWS_PROFILE..." |
| AccessDenied | "Check your IAM permissions include s3:ListBucket" |
| NoSuchBucket | "Bucket does not exist. Check config.yaml" |

### CUR Manifest

**File:** `ingestion/manifest.py`

`CURManifest` dataclass with fields:
- `assembly_id` — unique build identifier (required)
- `report_keys` — list of S3 keys for data files (must be non-empty)
- `billing_period` — derived from `billingPeriod.start/end` dates
- `is_parquet` — True if compression is "Parquet"

---

## Anomaly Detection

**File:** `analysis/anomalies.py`

### Algorithm: Median/MAD Z-Score + Theil-Sen Drift

Two complementary techniques:

**Point anomalies** (sudden spikes/drops):
```
For each dimension group (service, account, or region):
  1. Query daily costs for last N days from daily_cost_summary
  2. Require >= 3 data points (skip otherwise)
  3. Baseline = all days except the most recent
  4. current_cost = most recent day's cost
  5. Skip if current_cost < min_daily_cost ($1 default)
  6. z_score = 0.6745 * (current_cost - median(baseline)) / MAD(baseline)
  7. Special case: zero MAD → z_score = ±10.0 if cost differs
  8. Flag if |z_score| >= threshold
```

**Trend anomalies** (gradual drift):
```
For groups with >= 5 data points:
  1. Compute Theil-Sen slope over all days in the window
  2. drift_pct = (slope * num_days) / median
  3. Flag if |drift_pct| >= drift_threshold (default 20%)
```

See [ANOMALIES.md](ANOMALIES.md) for full details.

### Sensitivity Presets

| Preset | Z-Score Threshold |
|--------|-------------------|
| low | 3.0 |
| medium (default) | 2.5 |
| high | 2.0 |

### Severity Classification

| Absolute Z-Score | Severity |
|-----------------|----------|
| > 4.0 | critical |
| > 3.0 | warning |
| <= 3.0 | info |

### Direction

- `z_score > 0` → **spike** (cost increased)
- `z_score < 0` → **drop** (cost decreased)

Results are returned sorted by `|z_score|` descending.

---

## Trend Analysis

**File:** `analysis/trends.py`

### `get_daily_trends(conn, days, group_by, top_n)`

1. Queries top N groups by total cost within the window
2. Uses SQL `LAG()` window function to compute day-over-day change
3. Returns `TrendRow` objects with: date, group_value, total_cost, cost_change, pct_change

### `get_total_daily_costs(conn, days)`

Simple aggregation of total daily cost across all dimensions. Used by the `trends` command to show the overall daily spend line.

### `get_cost_trend(conn, date_start, date_end, group_by, filter_value, granularity)`

Flexible time-series query used by the agent's `get_cost_trend` tool:
1. Validates `group_by` against a whitelist (`product_code`, `usage_account_id`, `region`)
2. Validates `granularity` (`daily`, `weekly`, `monthly`) and uses `DATE_TRUNC` for aggregation
3. Supports optional `filter_value` (requires `group_by`)
4. Returns `CostTrendResult` with data points and summary stats (total, average, min, max)

---

## Period Comparison

**File:** `analysis/comparison.py`

### `compare_periods(conn, period_a_start/end, period_b_start/end, group_by, top_n)`

Compares costs between two date ranges using a `FULL OUTER JOIN` on `daily_cost_summary`. Categorizes results into movers (present in both periods), new (only in period B), and disappeared (only in period A).

---

## Cost Spike Drill-Down

**File:** `analysis/drilldown.py`

### `drill_down_cost_spike(conn, service, date_start, date_end, account_id, top_n)`

Breaks down CUR line items for a service by usage_type, operation, and resource_id. Requires CUR data in `cost_line_items`. Returns percentage-of-total breakdowns for each dimension.

---

## Cost Attribution

**File:** `analysis/attribution.py`

### `attribute_cost_change(conn, service, period_a_start/end, period_b_start/end, account_id, top_n)`

Compares two periods at the CUR line-item level for a specific service. Uses `FULL OUTER JOIN` queries on `cost_line_items` for both `usage_type` and `resource_id` dimensions. Categorizes items as movers, new, or disappeared. Validates dimensions against a whitelist for SQL safety.

---

## Anomaly Explanation

**File:** `analysis/explainer.py`

### `explain_anomaly(conn, service, anomaly_date, account_id, baseline_days)`

Builds a comprehensive anomaly narrative:
1. **Baseline stats**: queries N days before the anomaly date, computes median/min/max
2. **Magnitude**: anomaly cost vs baseline median, cost multiple
3. **Ongoing check**: examines up to 7 days after; elevated if cost > 1.5× median
4. **Usage-type attribution**: optional CUR query comparing baseline avg vs anomaly day per usage_type. Gracefully degrades if no CUR data (`has_cur_data=False`)
5. **Baseline flag**: `has_baseline=False` when no prior data exists, signaling unreliable comparisons

---

## Natural Language Query — Agentic System

**Files:** `agent/agent.py`, `agent/bedrock_client.py`, `agent/tools.py`, `agent/executor.py`, `agent/prompts.py`

### Architecture

The agent system uses an agentic loop powered by AWS Bedrock Converse API. Instead of one-shot text-to-SQL translation, the agent reasons, calls tools, and produces a formatted final answer.

### Agent Loop Flow

```
run_agent(question, db_conn, model, region, on_step) → AgentResponse
  │
  ├── Build messages, system prompt, tool_config
  ├── FOR up to max_agent_iterations:
  │     ├── bedrock.converse(messages, tools, system)
  │     ├── If stopReason == "end_turn" → return final answer
  │     └── If stopReason == "tool_use":
  │           ├── Execute each tool call via tools.execute_tool()
  │           ├── Call on_step() callback for CLI display
  │           └── Append tool results to messages, continue loop
  └── If loop exhausted → raise AgentError
```

### Tools

| Tool | AWS Client | Purpose |
|------|-----------|---------|
| `query_cost_database` | DuckDB | Query local cost data via SQL |
| `get_cost_explorer_data` | `ce` | Real-time cost data from AWS (unblended, blended, net amortized) |
| `get_cloudwatch_metrics` | `cloudwatch` | Metrics and billing alarms |
| `get_budget_info` | `budgets` | Budget vs actual spend |
| `get_organization_info` | `organizations` | Account names and structure |
| `detect_cost_anomalies` | DuckDB | Anomaly detection (median/MAD z-scores, Theil-Sen drift) |
| `ingest_cost_explorer_data` | `ce` | Import Cost Explorer data into local DB |
| `ingest_cur_data` | `s3` | Import CUR data from S3 into local DB |
| `compare_periods` | DuckDB | Period-over-period cost comparison by dimension |
| `drill_down_cost_spike` | DuckDB | Break down a service spike by usage_type/operation/resource (CUR) |
| `scan_anomalies_over_range` | DuckDB | Scan a historical date range for anomalies day-by-day |
| `attribute_cost_change` | DuckDB | Compare two periods at line-item level for a service (CUR) |
| `get_cost_trend` | DuckDB | Time-series with grouping, filtering, daily/weekly/monthly granularity |
| `explain_anomaly` | DuckDB | Comprehensive anomaly narrative: baseline, magnitude, ongoing, attribution |

Tool errors are returned as results (not raised), so the agent can adapt and try alternative approaches.

### Bedrock Client

`BedrockClient` wraps `boto3.client("bedrock-runtime")` with user-friendly error messages for: missing credentials, access denied, model not found, rate limits, quota exceeded.

### SQL Safety Validation

**20 forbidden patterns** checked via regex with word boundaries:

`INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, REPLACE, MERGE, GRANT, REVOKE, EXEC, EXECUTE, CALL, COPY, ATTACH, DETACH, PRAGMA, SET, LOAD, INSTALL`

These catch both direct forbidden queries and subquery injection attempts.

### System Prompt

The agent system prompt in `prompts.py` includes:
- Full schema description of all 3 tables with column types
- Available tools and their purposes
- Guidelines for tool selection and SQL best practices
- Formatting instructions for the final answer

### Error Handling

`AgentError` wraps Bedrock and tool failures:
- Missing AWS credentials → configure via AWS_PROFILE
- Access denied → check IAM permissions and model access
- Model not found → check model ID and region
- Rate limit / quota → wait and retry
- Max iterations exceeded → rephrase question

---

## CLI Commands

**Entry point:** `aws_cost_anomalies.cli.app:main`

All commands use the `--config` option to specify a YAML config file.

### `ingest`

```
uv run aws-cost-anomalies ingest [--config PATH] [--source cur|cost-explorer] [--date YYYY-MM] [--days N] [--full-refresh]
```

- `--source cost-explorer` — import from Cost Explorer API (no S3 needed)
- `--source cur` — import CUR data from S3 (default if no `--source`)
- `--days N` — lookback days for Cost Explorer (default from config)
- `--date YYYY-MM` — ingest a single billing month (CUR only)
- `--full-refresh` — re-ingest all periods (CUR only, ignores assembly_id cache)
- Shows Rich progress bars during download/load
- Reports skipped (already up-to-date) periods

### `trends`

```
uv run aws-cost-anomalies trends [--config PATH] [--days 14] [--group-by service|account|region] [--top 10] [--source cur|cost-explorer]
```

- Shows total daily cost table + grouped trend breakdown
- Day-over-day change with color coding (red for increases, green for decreases)

### `anomalies`

```
uv run aws-cost-anomalies anomalies [--config PATH] [--days 14]
    [--sensitivity low|medium|high] [--group-by service|account|region]
    [--source cur|cost-explorer] [--drift-threshold N]
```

- Displays anomalies table with severity colors (critical=red, warning=yellow, info=cyan)
- Shows filter settings after results

### `query`

```
uv run aws-cost-anomalies query [--config PATH] [--interactive] "question text"
uv run aws-cost-anomalies query --interactive  # REPL mode
```

- Uses an agentic loop via Bedrock to answer questions using multiple tools
- REPL mode: interactive loop with `exit`/`quit`/`q` to leave
- Shows tool calls and results inline as the agent reasons

---

## Output Formatting

**File:** `cli/formatting.py`

| Function | Behavior |
|----------|----------|
| `format_currency(value)` | `$1,234.56` or em-dash for None |
| `format_pct(value)` | `+25.0%` / `-10.5%` / `0.0%` or em-dash for None |
| `print_trends_table()` | Rich table with color-coded changes |
| `print_anomalies_table()` | Rich table with severity colors |
| `print_query_results()` | Generic Rich table for SQL results |
| `_format_cell(value)` | Floats as `1,234.56`, None as `NULL` |

---

## Error Handling Strategy

Custom exception hierarchy for user-friendly error messages:

| Exception | Module | Purpose |
|-----------|--------|---------|
| `ConfigError` | config.settings | Invalid YAML, bad types, out-of-range values |
| `S3Error` | ingestion.s3_client | AWS credentials, permissions, missing buckets |
| `AgentError` | agent.agent | Bedrock API / agent loop failures |
| `BedrockError` | agent.bedrock_client | Bedrock runtime errors |
| `UnsafeSQLError` | agent.executor | Forbidden SQL operations |

All CLI commands catch these exceptions and display colored error messages via Rich. Individual file/period failures during ingestion are non-fatal — the pipeline continues and reports errors inline.

---

## Empty Database Detection

All analysis commands (`trends`, `anomalies`, `query`) check if `daily_cost_summary` has any rows before proceeding. If empty, they display:

> No cost data found. Run **ingest** first to load CUR data.

This prevents confusing empty results or errors on a fresh install.

---

## Docker Development Environment

### Dockerfile

Python 3.12 slim image with git installed. Two-stage pip install: first `pyproject.toml` only (for layer caching), then full source copy.

### docker-compose.yml

| Volume Mount | Purpose |
|-------------|---------|
| `./src:/app/src` | Live code reload during development |
| `./tests:/app/tests` | Test files |
| `./config.yaml:/app/config.yaml:ro` | Read-only config |
| `~/.aws:/root/.aws:ro` | AWS credentials passthrough |
| `data:/data` | Persistent DuckDB + cache (named volume) |

| Environment Variable | Source |
|---------------------|--------|
| `AWS_PROFILE` | Host env, defaults to "default" |
| `AWS_BEDROCK_REGION` | Host env, defaults to "us-east-1" |
| `AWS_COST_DB_PATH` | `/data/costs.duckdb` |
| `AWS_COST_CACHE_DIR` | `/data/cache` |

**Usage:**
- Dev shell: `docker compose run --rm app`
- Direct command: `docker compose run --rm app aws-cost-anomalies ingest`

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| typer | >= 0.9.0 | CLI framework |
| rich | >= 13.0 | Terminal tables, progress bars, colors |
| duckdb | >= 1.0 | Embedded OLAP database |
| boto3 | >= 1.34 | AWS S3 access |
| pyyaml | >= 6.0 | YAML config parsing |
| pyarrow | >= 15.0 | Parquet file handling |
| numpy | >= 1.26 | Z-score computation (mean, std) |
| pytest | >= 8.0 | Testing (dev) |
| moto | >= 5.0 | S3 mocking (dev) |
| ruff | >= 0.5 | Linting (dev) |

---

## Key Design Decisions

1. **DuckDB over PostgreSQL/SQLite** — Columnar storage is ideal for OLAP aggregation queries on cost data. Embedded (no server), supports Parquet natively.

2. **Pre-aggregated daily summary** — Trades storage for query speed. Trend and anomaly queries run against the summary table, not raw line items.

3. **Incremental ingestion via assembly_id** — AWS redelivers billing periods with new assembly IDs when data changes. Comparing assembly IDs avoids redundant re-ingestion while catching updates.

4. **CUR v1/v2 dual support** — Column mapping dictionaries normalize both CUR formats into a single schema. Auto-detected from parquet column names.

5. **SQL safety via pattern matching** — Agent queries are validated with regex-based forbidden keyword detection rather than SQL parsing. Simple, effective, catches subquery injection.

6. **Non-fatal ingestion errors** — Individual file or period failures are logged but don't abort the entire pipeline. Partial ingestion is better than no ingestion.

7. **Config priority chain** — Env vars override YAML, which overrides defaults. Allows Docker/CI environments to override without editing config files.

---

## Extending the System

### Adding a new CLI command

1. Create `src/aws_cost_anomalies/cli/new_command.py`
2. Import `app` from `cli.app`, decorate your function with `@app.command()`
3. Add an import in `cli/app.py`'s `main()` function

### Adding a new dimension for grouping

1. Ensure the column exists in `daily_cost_summary` schema
2. Add it to `valid_groups` in `analysis/trends.py` and `analysis/anomalies.py`
3. Add a CLI option value mapping in the relevant command

### Changing the anomaly detection algorithm

Replace or extend `detect_anomalies()` in `analysis/anomalies.py`. The function takes a DuckDB connection and returns `list[Anomaly]`. The CLI layer is decoupled from the algorithm.

### Adding a new agent-queryable table

1. Add DDL in `storage/schema.py`
2. Update `SCHEMA_DESCRIPTION` in `agent/prompts.py`
3. Add population logic during ingestion if needed

### Adding a new agent tool

1. Add a `toolSpec` definition dict in `agent/tools.py`
2. Write an executor function following the pattern: `_execute_<name>(tool_input, context) -> dict`
3. Register the executor in `_EXECUTORS` and add the spec to `TOOL_DEFINITIONS`
4. Update the agent system prompt in `agent/prompts.py` to describe the new tool
5. Add tests in `tests/test_agent/test_tools.py` with mocked boto3
