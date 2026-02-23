# Anomaly Detection

The anomaly detector identifies unusual cost changes across AWS services,
accounts, or regions using two complementary techniques over a rolling
time window:

1. **Point anomalies** — sudden spikes or drops detected via median/MAD
   modified z-scores (robust to baseline outliers).
2. **Trend anomalies** — gradual drift detected via Theil-Sen slope
   estimation (robust to individual outlier days).

## How it works

### Point anomaly detection

1. **Group daily costs** by one or more dimensions (service, account, region)
   from the `daily_cost_summary` table for the last N days (default 14).
2. **Build a baseline** from all days in the window except the most recent.
3. **Compute the modified z-score** using median and MAD (Median Absolute
   Deviation) instead of mean/std:
   ```
   MAD = median(|x_i - median(x)|)
   z = 0.6745 * (current_cost - median) / MAD
   ```
   The 0.6745 constant normalizes MAD to be comparable to standard deviation
   for normally distributed data.
4. **Flag as anomaly** if `|z-score|` meets the sensitivity threshold and the
   current cost exceeds the minimum daily cost filter.
5. **Classify severity** and direction (spike or drop).

#### Why median/MAD instead of mean/std?

Mean and standard deviation are sensitive to outliers. A single large spike in
the baseline inflates std, making subsequent anomalies harder to detect. Median
and MAD are robust — a single outlier day doesn't suppress future detections.

#### Zero-variance edge case

When the baseline has near-zero MAD (perfectly steady spending), a special rule
applies: if the current cost differs from the median by more than
`min_daily_cost`, the z-score is set to +/-10.0. Otherwise the group is
skipped.

### Trend/drift detection

After checking for point anomalies, the detector also looks for gradual drift:

1. **Compute the Theil-Sen slope** over all days in the window (including the
   current day). Theil-Sen takes the median of all pairwise slopes, making it
   robust to individual outlier days.
2. **Calculate drift percentage**: `drift_pct = (slope * num_days) / median`.
   This measures the total cost change over the window relative to the median
   daily cost.
3. **Flag as trend anomaly** if `|drift_pct|` exceeds the drift threshold
   (default 20%).

Groups with fewer than 5 days of data are skipped for drift detection — there
isn't enough history for a meaningful slope estimate. Groups with fewer than 3
days are skipped entirely.

## Multi-dimensional grouping

By default, anomalies are detected per-service. You can group by multiple
dimensions to catch anomalies that are invisible at the aggregate level.

For example, an EC2 spike in one account offset by a drop in another account
would be invisible when grouping by service alone. Grouping by
`service+account` reveals both.

Available groupings:

| `--group-by` value | Columns used |
|---------------------|-------------|
| `service` (default) | `product_code` |
| `account` | `usage_account_id` |
| `region` | `region` |
| `service+account` | `product_code`, `usage_account_id` |
| `service+region` | `product_code`, `region` |
| `account+region` | `usage_account_id`, `region` |

## Sensitivity levels

The `--sensitivity` flag controls how aggressively point anomalies are flagged:

| Flag     | z-score threshold | Behavior                         |
|----------|-------------------|----------------------------------|
| `low`    | > 3.0             | Fewer alerts, only large changes |
| `medium` | > 2.5             | Balanced (default)               |
| `high`   | > 2.0             | More alerts, more noise          |

## Severity classification

### Point anomalies

| Severity   | Condition         |
|------------|-------------------|
| `critical` | \|z-score\| > 4.0 |
| `warning`  | \|z-score\| > 3.0 |
| `info`     | everything else   |

Direction is `"spike"` for positive z-scores and `"drop"` for negative.

### Trend anomalies

| Severity   | Condition              |
|------------|------------------------|
| `critical` | \|drift_pct\| > 100%   |
| `warning`  | \|drift_pct\| > 50%    |
| `info`     | everything else        |

Direction is `"drift_up"` for positive drift and `"drift_down"` for negative.

## Sorting

Results are sorted by severity (critical first, then warning, then info),
with magnitude as a tiebreaker within each severity level. This avoids
mixing the incomparable scales of point z-scores and drift percentages.

## Configuration

Defaults come from `config.yaml` under the `anomaly` key:

```yaml
anomaly:
  rolling_window_days: 14   # days of history for the baseline
  z_score_threshold: 2.5    # overridden by --sensitivity on the CLI
  min_daily_cost: 1.0       # ignore groups below this daily cost
  drift_threshold_pct: 20.0 # percent drift over window to flag
```

## CLI usage

```
aws-cost-anomalies anomalies [OPTIONS]
```

| Option              | Default     | Description                                          |
|---------------------|-------------|------------------------------------------------------|
| `--days`            | 14          | Rolling window size                                  |
| `--sensitivity`     | `medium`    | Detection sensitivity (low/medium/high)              |
| `--group-by`        | `service`   | Grouping dimension (see table above)                 |
| `--drift-threshold` | from config | Drift threshold in percent (default: config or 20%)  |
| `--config`          | config.yaml | Path to YAML config file                             |

### Examples

```bash
# Flag anomalies in the last 30 days, grouped by account, high sensitivity
aws-cost-anomalies anomalies --days 30 --group-by account --sensitivity high

# Detect per-service-per-account anomalies with a 10% drift threshold
aws-cost-anomalies anomalies --group-by service+account --drift-threshold 10
```

## Key source files

| File | Role |
|------|------|
| `src/aws_cost_anomalies/analysis/anomalies.py` | Detection algorithm (median/MAD, Theil-Sen) |
| `src/aws_cost_anomalies/cli/anomalies.py` | CLI command |
| `src/aws_cost_anomalies/config/settings.py` | Configuration (`AnomalyConfig`) |
| `src/aws_cost_anomalies/cli/formatting.py` | Rich table output |
| `tests/test_analysis/test_anomalies.py` | Tests |
