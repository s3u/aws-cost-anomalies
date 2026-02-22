#!/usr/bin/env bash
# Refresh CUR data from S3 into DuckDB.
#
# Usage:
#   ./scripts/refresh-data.sh                   # incremental (new periods only)
#   ./scripts/refresh-data.sh --full-refresh     # re-ingest everything
#   ./scripts/refresh-data.sh --date 2025-01     # specific month only
set -euo pipefail

uv run aws-cost-anomalies ingest "$@"
