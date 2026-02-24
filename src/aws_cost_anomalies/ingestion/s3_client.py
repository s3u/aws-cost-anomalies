"""S3 client for listing and downloading CUR files."""

from __future__ import annotations

import hashlib
from pathlib import Path

from botocore.exceptions import ClientError, NoCredentialsError

from aws_cost_anomalies.utils.aws import aws_session

from aws_cost_anomalies.ingestion.manifest import (
    CURManifest,
    parse_manifest,
)


class S3Error(Exception):
    """User-friendly S3 error wrapper."""

    pass


class CURBrowser:
    """Browse and download CUR files from S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str,
        report_name: str,
        region: str = "us-east-1",
        profile: str = "",
    ):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.report_name = report_name
        try:
            self.s3 = aws_session(profile).client("s3", region_name=region)
        except NoCredentialsError:
            raise S3Error(
                "AWS credentials not found. Configure credentials "
                "via AWS_PROFILE, environment variables, or "
                "~/.aws/credentials."
            )

    def _report_prefix(self) -> str:
        """Return the S3 prefix for this report."""
        parts = [self.prefix, self.report_name]
        return "/".join(p for p in parts if p)

    def list_billing_periods(self) -> list[str]:
        """List available billing period folders."""
        prefix = self._report_prefix() + "/"
        try:
            paginator = self.s3.get_paginator(
                "list_objects_v2"
            )
            periods: set[str] = set()
            for page in paginator.paginate(
                Bucket=self.bucket,
                Prefix=prefix,
                Delimiter="/",
            ):
                for cp in page.get("CommonPrefixes", []):
                    folder = (
                        cp["Prefix"].rstrip("/").split("/")[-1]
                    )
                    if len(folder) == 17 and folder[8] == "-":
                        periods.add(folder)
            return sorted(periods)
        except NoCredentialsError:
            raise S3Error(
                "AWS credentials not found. Configure "
                "credentials via AWS_PROFILE, environment "
                "variables, or ~/.aws/credentials."
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("AccessDenied", "403"):
                raise S3Error(
                    f"Access denied to s3://{self.bucket}/{prefix}. "
                    "Check your IAM permissions include "
                    "s3:ListBucket."
                )
            if code in ("NoSuchBucket",):
                raise S3Error(
                    f"Bucket '{self.bucket}' does not exist. "
                    "Check your config.yaml s3.bucket setting."
                )
            raise S3Error(
                f"S3 error listing billing periods: {e}"
            )

    def get_manifest(self, billing_period: str) -> CURManifest:
        """Download and parse the manifest for a billing period."""
        prefix = f"{self._report_prefix()}/{billing_period}/"
        try:
            paginator = self.s3.get_paginator(
                "list_objects_v2"
            )
            manifest_keys = []
            for page in paginator.paginate(
                Bucket=self.bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(
                        "-Manifest.json"
                    ) or key.endswith("/manifest.json"):
                        manifest_keys.append(key)
        except ClientError as e:
            raise S3Error(
                f"S3 error reading manifest for {billing_period}: "
                f"{e}"
            )

        if not manifest_keys:
            raise FileNotFoundError(
                f"No manifest found for {billing_period} in "
                f"s3://{self.bucket}/{prefix}. Verify the "
                f"report_name and billing period exist."
            )

        manifest_key = sorted(manifest_keys)[-1]
        try:
            resp = self.s3.get_object(
                Bucket=self.bucket, Key=manifest_key
            )
            return parse_manifest(resp["Body"].read())
        except ClientError as e:
            raise S3Error(
                f"Failed to download manifest "
                f"s3://{self.bucket}/{manifest_key}: {e}"
            )

    def download_file(
        self, s3_key: str, local_dir: str | Path
    ) -> Path:
        """Download a single file from S3 to local cache."""
        local_dir = Path(local_dir)
        # Use hash prefix to avoid collisions from path flattening
        key_hash = hashlib.sha256(s3_key.encode()).hexdigest()[:12]
        safe_name = s3_key.replace("/", "_")
        local_path = local_dir / f"{key_hash}_{safe_name}"
        if local_path.exists():
            return local_path
        local_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.s3.download_file(
                self.bucket, s3_key, str(local_path)
            )
        except ClientError as e:
            raise S3Error(
                f"Failed to download "
                f"s3://{self.bucket}/{s3_key}: {e}"
            )
        return local_path
