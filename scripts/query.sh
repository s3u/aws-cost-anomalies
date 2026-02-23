#!/usr/bin/env bash
# Launch the agent in interactive REPL mode.
#
# Usage:
#   ./scripts/query.sh                        # interactive REPL
#   ./scripts/query.sh "What is my total spend?"  # one-off question
set -euo pipefail

if [ $# -gt 0 ]; then
    uv run aws-cost-anomalies query "$*"
else
    uv run aws-cost-anomalies query --interactive
fi
