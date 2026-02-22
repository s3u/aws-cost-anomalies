# Anomaly Detection

The anomaly detector identifies unusual cost changes across AWS services,
accounts, or regions using a **modified z-score algorithm** over a rolling
time window.

## How it works

1. **Group daily costs** by a dimension (service, account, or region) from the
   `daily_cost_summary` table for the last N days (default 14).
2. **Build a baseline** from all days in the window except the most recent.
3. **Compute the z-score** of the most recent day against the baseline:
   `z = (current_cost - mean) / std_dev` (sample standard deviation, ddof=1).
4. **Flag as anomaly** if `|z-score|` meets the sensitivity threshold and the
   current cost exceeds the minimum daily cost filter.
5. **Classify severity** and direction (spike or drop).

Groups with fewer than 3 days of data are skipped — there isn't enough history
for a meaningful baseline.

### Zero-variance edge case

When the baseline has near-zero standard deviation (steady spending), a
special rule applies: if the current cost differs from the mean by more than
`min_daily_cost`, the z-score is set to +/-10.0. Otherwise the group is
skipped.

## Sensitivity levels

The `--sensitivity` flag controls how aggressively anomalies are flagged:

| Flag     | z-score threshold | Behavior                         |
|----------|-------------------|----------------------------------|
| `low`    | > 3.0             | Fewer alerts, only large changes |
| `medium` | > 2.5             | Balanced (default)               |
| `high`   | > 2.0             | More alerts, more noise          |

## Severity classification

Once flagged, each anomaly is graded by the absolute z-score:

| Severity   | Condition        |
|------------|------------------|
| `critical` | \|z-score\| > 4.0 |
| `warning`  | \|z-score\| > 3.0 |
| `info`     | everything else  |

The `direction` field is `"spike"` for positive z-scores (cost increase) and
`"drop"` for negative z-scores (cost decrease).

## Configuration

Defaults come from `config.yaml` under the `anomaly` key:

```yaml
anomaly:
  rolling_window_days: 14   # days of history for the baseline
  z_score_threshold: 2.5    # overridden by --sensitivity on the CLI
  min_daily_cost: 1.0       # ignore groups below this daily cost
```

## CLI usage

```
aws-cost-anomalies anomalies [OPTIONS]
```

| Option          | Default    | Description                             |
|-----------------|------------|-----------------------------------------|
| `--days`        | 14         | Rolling window size                     |
| `--sensitivity` | `medium`   | Detection sensitivity (low/medium/high) |
| `--group-by`    | `service`  | Grouping dimension (service/account/region) |
| `--config`      | config.yaml | Path to YAML config file              |

### Example

```bash
# Flag anomalies in the last 30 days, grouped by account, high sensitivity
aws-cost-anomalies anomalies --days 30 --group-by account --sensitivity high
```

## Key source files

| File | Role |
|------|------|
| `src/aws_cost_anomalies/analysis/anomalies.py` | Detection algorithm |
| `src/aws_cost_anomalies/cli/anomalies.py` | CLI command |
| `src/aws_cost_anomalies/config/settings.py` | Configuration (`AnomalyConfig`) |
| `src/aws_cost_anomalies/cli/formatting.py` | Rich table output |
| `tests/test_analysis/test_anomalies.py` | Tests |
