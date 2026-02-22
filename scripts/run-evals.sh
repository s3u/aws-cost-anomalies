#!/usr/bin/env bash
# Run the NLQ agent correctness evals.
# Requires AWS Bedrock credentials to be configured.
#
# Usage:
#   ./scripts/run-evals.sh          # run all 10 evals
#   ./scripts/run-evals.sh -k total # run evals matching "total"
set -euo pipefail

uv run pytest -m evals -v "$@"
