#!/usr/bin/env bash
# Launch the agent in interactive REPL mode.
#
# Usage:
#   ./scripts/query.sh                            # interactive REPL
#   ./scripts/query.sh -v                          # interactive, verbose
#   ./scripts/query.sh "What is my total spend?"   # one-off question
#   ./scripts/query.sh -v "What is my total spend?" # one-off, verbose
set -euo pipefail

verbose=""
if [ "${1:-}" = "-v" ] || [ "${1:-}" = "--verbose" ]; then
    verbose="--verbose"
    shift
fi

if [ $# -gt 0 ]; then
    uv run aws-cost-anomalies query $verbose "$*"
else
    uv run aws-cost-anomalies query $verbose --interactive
fi
