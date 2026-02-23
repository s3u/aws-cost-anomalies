"""Fetch daily cost data from the AWS Cost Explorer API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


class CostExplorerError(Exception):
    """Raised for Cost Explorer API failures."""

    pass


@dataclass
class CostExplorerRow:
    """One day+service+account aggregation from Cost Explorer."""

    usage_date: date
    usage_account_id: str
    product_code: str
    total_unblended_cost: float
    total_blended_cost: float


# Map common Cost Explorer service names to CUR product_code values.
# Unknown services pass through as-is.
CE_SERVICE_TO_PRODUCT_CODE: dict[str, str] = {
    "Amazon Elastic Compute Cloud - Compute": "AmazonEC2",
    "Amazon Simple Storage Service": "AmazonS3",
    "Amazon Relational Database Service": "AmazonRDS",
    "Amazon DynamoDB": "AmazonDynamoDB",
    "AWS Lambda": "AWSLambda",
    "Amazon CloudFront": "AmazonCloudFront",
    "Amazon Elastic Container Service": "AmazonECS",
    "Amazon Elastic Kubernetes Service": "AmazonEKS",
    "Amazon ElastiCache": "AmazonElastiCache",
    "Amazon Redshift": "AmazonRedshift",
    "Amazon Kinesis": "AmazonKinesis",
    "Amazon SageMaker": "AmazonSageMaker",
    "Amazon Simple Notification Service": "AmazonSNS",
    "Amazon Simple Queue Service": "AmazonSQS",
    "AWS Key Management Service": "awskms",
    "Amazon Route 53": "AmazonRoute53",
    "Amazon API Gateway": "AmazonApiGateway",
    "AWS CloudTrail": "AWSCloudTrail",
    "Amazon CloudWatch": "AmazonCloudWatch",
    "AWS Config": "AWSConfig",
    "AWS Secrets Manager": "AWSSecretsManager",
    "Amazon Elastic File System": "AmazonEFS",
    "Amazon Elastic Block Store": "AmazonEBS",
    "AWS Step Functions": "AWSStepFunctions",
    "Amazon Athena": "AmazonAthena",
    "AWS Glue": "AWSGlue",
    "Amazon OpenSearch Service": "AmazonES",
    "Amazon GuardDuty": "AmazonGuardDuty",
    "AWS CodeBuild": "AWSCodeBuild",
    "Amazon Bedrock": "AmazonBedrock",
    "Tax": "Tax",
}


def _map_service_name(ce_service: str) -> str:
    """Map a Cost Explorer service display name to a CUR product_code."""
    return CE_SERVICE_TO_PRODUCT_CODE.get(ce_service, ce_service)


def fetch_cost_explorer_data(
    start_date: str,
    end_date: str,
    region: str = "us-east-1",
    on_page: Callable[[int, int], None] | None = None,
) -> list[CostExplorerRow]:
    """Fetch daily cost data from Cost Explorer grouped by SERVICE and LINKED_ACCOUNT.

    Args:
        start_date: Start date YYYY-MM-DD (inclusive).
        end_date: End date YYYY-MM-DD (exclusive).
        region: AWS region for the CE API endpoint.
        on_page: Optional callback(page_num, rows_so_far) for progress.

    Returns:
        List of CostExplorerRow, one per day+service+account combination.
        Zero-cost entries (|cost| < 0.001) are filtered out.

    Raises:
        CostExplorerError: On API or credential failures.
    """
    try:
        client = boto3.client("ce", region_name=region)
    except NoCredentialsError:
        raise CostExplorerError(
            "AWS credentials not found. Configure credentials to use "
            "Cost Explorer (e.g. AWS_PROFILE, IAM role, or env vars)."
        )

    rows: list[CostExplorerRow] = []
    page_num = 0
    next_token: str | None = None

    try:
        while True:
            kwargs: dict = {
                "TimePeriod": {
                    "Start": start_date,
                    "End": end_date,
                },
                "Granularity": "DAILY",
                "Metrics": ["UnblendedCost", "BlendedCost"],
                "GroupBy": [
                    {"Type": "DIMENSION", "Key": "SERVICE"},
                    {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
                ],
            }
            if next_token:
                kwargs["NextPageToken"] = next_token

            response = client.get_cost_and_usage(**kwargs)
            page_num += 1

            for period in response.get("ResultsByTime", []):
                usage_date = date.fromisoformat(
                    period["TimePeriod"]["Start"]
                )
                for group in period.get("Groups", []):
                    keys = group["Keys"]
                    service_name = keys[0] if len(keys) > 0 else "Unknown"
                    account_id = keys[1] if len(keys) > 1 else ""

                    unblended = float(
                        group["Metrics"]["UnblendedCost"]["Amount"]
                    )
                    blended = float(
                        group["Metrics"]["BlendedCost"]["Amount"]
                    )

                    # Filter zero-cost entries
                    if abs(unblended) < 0.001 and abs(blended) < 0.001:
                        continue

                    rows.append(
                        CostExplorerRow(
                            usage_date=usage_date,
                            usage_account_id=account_id,
                            product_code=_map_service_name(service_name),
                            total_unblended_cost=unblended,
                            total_blended_cost=blended,
                        )
                    )

            if on_page:
                on_page(page_num, len(rows))

            next_token = response.get("NextPageToken")
            if not next_token:
                break

    except ClientError as e:
        raise CostExplorerError(f"Cost Explorer API error: {e}")

    return rows
