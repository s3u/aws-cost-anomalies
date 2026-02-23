"""AWS Bedrock runtime client for the agent loop."""

from __future__ import annotations

from botocore.exceptions import ClientError, NoCredentialsError

from aws_cost_anomalies.utils.aws import aws_session


class BedrockError(Exception):
    """User-friendly error from the Bedrock integration."""

    pass


class BedrockClient:
    """Thin wrapper around boto3 bedrock-runtime Converse API."""

    def __init__(self, region: str = "us-east-1", profile: str = ""):
        try:
            self.client = aws_session(profile).client(
                "bedrock-runtime", region_name=region
            )
        except NoCredentialsError:
            raise BedrockError(
                "AWS credentials not found. Configure credentials "
                "via AWS_PROFILE, environment variables, or "
                "~/.aws/credentials."
            )

    def converse(
        self,
        model_id: str,
        messages: list[dict],
        system: list[dict] | None = None,
        tool_config: dict | None = None,
        max_tokens: int = 4096,
    ) -> dict:
        """Call the Bedrock Converse API.

        Returns the full Converse response dict.
        Raises BedrockError with user-friendly messages.
        """
        kwargs: dict = {
            "modelId": model_id,
            "messages": messages,
            "inferenceConfig": {"maxTokens": max_tokens},
        }
        if system:
            kwargs["system"] = system
        if tool_config:
            kwargs["toolConfig"] = tool_config

        try:
            return self.client.converse(**kwargs)
        except NoCredentialsError:
            raise BedrockError(
                "AWS credentials not found. Configure credentials "
                "via AWS_PROFILE, environment variables, or "
                "~/.aws/credentials."
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            message = e.response["Error"]["Message"]

            if code in ("AccessDeniedException", "403"):
                raise BedrockError(
                    f"Access denied to Bedrock model '{model_id}'. "
                    "Ensure your IAM role has "
                    "bedrock:InvokeModel permission and the model "
                    "is enabled in your AWS account. "
                    f"Details: {message}"
                )

            if code == "ResourceNotFoundException":
                raise BedrockError(
                    f"Bedrock model '{model_id}' not found. "
                    "Check the model ID in your config and ensure "
                    "the model is available in your region."
                )

            if code == "ThrottlingException":
                raise BedrockError(
                    "Bedrock API rate limit reached. "
                    "Please wait a moment and try again."
                )

            if code == "ServiceQuotaExceededException":
                raise BedrockError(
                    "Bedrock service quota exceeded. "
                    "Request a quota increase in the AWS console "
                    "or try again later."
                )

            if code == "ValidationException":
                raise BedrockError(
                    f"Bedrock request validation error: {message}"
                )

            raise BedrockError(
                f"Bedrock API error ({code}): {message}"
            )
        except Exception as e:
            if "Could not connect" in str(e) or "EndpointConnectionError" in type(e).__name__:
                raise BedrockError(
                    "Could not connect to Bedrock API. "
                    "Check your internet connection and "
                    "AWS region configuration."
                )
            raise BedrockError(f"Unexpected Bedrock error: {e}")
