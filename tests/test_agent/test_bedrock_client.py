"""Tests for the Bedrock runtime client (mocked boto3)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from aws_cost_anomalies.agent.bedrock_client import (
    BedrockClient,
    BedrockError,
)


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "Converse",
    )


class TestBedrockClientInit:
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_creates_client(self, mock_session_cls):
        client = BedrockClient(region="us-west-2")
        mock_session_cls.assert_called_once_with()
        mock_session_cls.return_value.client.assert_called_once_with(
            "bedrock-runtime", region_name="us-west-2"
        )
        assert client.client is mock_session_cls.return_value.client.return_value

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_creates_client_with_profile(self, mock_session_cls):
        client = BedrockClient(region="us-west-2", profile="dev")
        mock_session_cls.assert_called_once_with(profile_name="dev")
        mock_session_cls.return_value.client.assert_called_once_with(
            "bedrock-runtime", region_name="us-west-2"
        )
        assert client.client is mock_session_cls.return_value.client.return_value

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_missing_credentials(self, mock_session_cls):
        mock_session_cls.return_value.client.side_effect = NoCredentialsError()
        with pytest.raises(BedrockError, match="credentials"):
            BedrockClient()


class TestConverse:
    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_basic_converse(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "Hello"}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5},
        }
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        response = client.converse(
            model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
            messages=[
                {
                    "role": "user",
                    "content": [{"text": "Hi"}],
                }
            ],
        )

        assert response["stopReason"] == "end_turn"
        mock_runtime.converse.assert_called_once()
        call_kwargs = mock_runtime.converse.call_args[1]
        assert call_kwargs["modelId"] == "us.anthropic.claude-sonnet-4-20250514-v1:0"
        assert call_kwargs["inferenceConfig"] == {"maxTokens": 4096}

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_with_system_and_tools(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.return_value = {"stopReason": "end_turn", "output": {"message": {}}}
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        system = [{"text": "You are a helper."}]
        tool_config = {"tools": [{"toolSpec": {"name": "test"}}]}
        client.converse(
            model_id="test-model",
            messages=[{"role": "user", "content": [{"text": "q"}]}],
            system=system,
            tool_config=tool_config,
            max_tokens=2048,
        )

        call_kwargs = mock_runtime.converse.call_args[1]
        assert call_kwargs["system"] == system
        assert call_kwargs["toolConfig"] == tool_config
        assert call_kwargs["inferenceConfig"] == {"maxTokens": 2048}

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_access_denied(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.side_effect = _client_error(
            "AccessDeniedException", "Not authorized"
        )
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        with pytest.raises(BedrockError, match="Access denied"):
            client.converse(
                model_id="test",
                messages=[{"role": "user", "content": [{"text": "q"}]}],
            )

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_model_not_found(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.side_effect = _client_error(
            "ResourceNotFoundException"
        )
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        with pytest.raises(BedrockError, match="not found"):
            client.converse(
                model_id="bad-model",
                messages=[{"role": "user", "content": [{"text": "q"}]}],
            )

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_throttling(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.side_effect = _client_error(
            "ThrottlingException"
        )
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        with pytest.raises(BedrockError, match="rate limit"):
            client.converse(
                model_id="test",
                messages=[{"role": "user", "content": [{"text": "q"}]}],
            )

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_quota_exceeded(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.side_effect = _client_error(
            "ServiceQuotaExceededException"
        )
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        with pytest.raises(BedrockError, match="quota exceeded"):
            client.converse(
                model_id="test",
                messages=[{"role": "user", "content": [{"text": "q"}]}],
            )

    @patch("aws_cost_anomalies.utils.aws.boto3.Session")
    def test_validation_error(self, mock_session_cls):
        mock_runtime = MagicMock()
        mock_runtime.converse.side_effect = _client_error(
            "ValidationException", "Bad request"
        )
        mock_session_cls.return_value.client.return_value = mock_runtime

        client = BedrockClient()
        with pytest.raises(BedrockError, match="validation error"):
            client.converse(
                model_id="test",
                messages=[{"role": "user", "content": [{"text": "q"}]}],
            )
