"""Tests for the NLQ agent loop (mocked Bedrock)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from aws_cost_anomalies.nlq.agent import (
    AgentError,
    AgentResponse,
    AgentStep,
    run_agent,
)
from aws_cost_anomalies.storage.schema import create_tables


@pytest.fixture
def db_conn():
    """In-memory DuckDB with schema and sample data."""
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-15', '111111111111', 'AmazonEC2', "
        "'us-east-1', 1500.50, 1400.00, 100, 50)"
    )
    conn.execute(
        "INSERT INTO daily_cost_summary VALUES "
        "('2025-01-15', '111111111111', 'AmazonS3', "
        "'us-east-1', 250.75, 240.00, 5000, 20)"
    )
    return conn


def _bedrock_text_response(text: str) -> dict:
    """Simulate a Bedrock Converse end_turn response."""
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"text": text}],
            }
        },
        "stopReason": "end_turn",
        "usage": {"inputTokens": 100, "outputTokens": 50},
    }


def _bedrock_tool_use_response(
    tool_name: str, tool_input: dict, tool_use_id: str = "tu_1"
) -> dict:
    """Simulate a Bedrock Converse tool_use response."""
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": tool_use_id,
                            "name": tool_name,
                            "input": tool_input,
                        }
                    }
                ],
            }
        },
        "stopReason": "tool_use",
        "usage": {"inputTokens": 80, "outputTokens": 40},
    }


class TestDirectAnswer:
    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_direct_text_answer(self, MockClient, db_conn):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_text_response(
            "Your top service is EC2 at $1,500.50."
        )
        MockClient.return_value = mock_client

        response = run_agent("top services?", db_conn)

        assert isinstance(response, AgentResponse)
        assert "EC2" in response.answer
        assert response.steps == []
        assert response.input_tokens == 100
        assert response.output_tokens == 50


class TestToolCallFlow:
    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_tool_call_then_answer(self, MockClient, db_conn):
        mock_client = MagicMock()

        # First call: tool use (query database)
        # Second call: final text answer
        mock_client.converse.side_effect = [
            _bedrock_tool_use_response(
                "query_cost_database",
                {
                    "sql": (
                        "SELECT product_code, "
                        "ROUND(SUM(total_unblended_cost), 2) AS total "
                        "FROM daily_cost_summary "
                        "GROUP BY product_code "
                        "ORDER BY total DESC"
                    )
                },
            ),
            _bedrock_text_response(
                "Your top services are:\n"
                "1. AmazonEC2 — $1,500.50\n"
                "2. AmazonS3 — $250.75"
            ),
        ]
        MockClient.return_value = mock_client

        response = run_agent("top services?", db_conn)

        assert "EC2" in response.answer
        assert len(response.steps) == 1
        assert response.steps[0].tool_name == "query_cost_database"
        assert response.steps[0].tool_result is not None
        assert response.steps[0].tool_result["row_count"] == 2

        # Verify token accumulation across two calls
        assert response.input_tokens == 180  # 80 + 100
        assert response.output_tokens == 90  # 40 + 50

    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_multiple_tool_calls(self, MockClient, db_conn):
        mock_client = MagicMock()

        mock_client.converse.side_effect = [
            # First: query the database
            _bedrock_tool_use_response(
                "query_cost_database",
                {"sql": "SELECT COUNT(*) AS cnt FROM daily_cost_summary"},
                "tu_1",
            ),
            # Second: another query
            _bedrock_tool_use_response(
                "query_cost_database",
                {
                    "sql": (
                        "SELECT product_code "
                        "FROM daily_cost_summary "
                        "GROUP BY product_code"
                    )
                },
                "tu_2",
            ),
            # Third: final answer
            _bedrock_text_response("Found 2 rows and 2 services."),
        ]
        MockClient.return_value = mock_client

        response = run_agent("summary?", db_conn)

        assert len(response.steps) == 2
        assert response.answer == "Found 2 rows and 2 services."


class TestErrorHandling:
    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_bedrock_error_raises_agent_error(
        self, MockClient, db_conn
    ):
        from aws_cost_anomalies.nlq.bedrock_client import (
            BedrockError,
        )

        mock_client = MagicMock()
        mock_client.converse.side_effect = BedrockError(
            "Access denied"
        )
        MockClient.return_value = mock_client

        with pytest.raises(AgentError, match="Access denied"):
            run_agent("test", db_conn)

    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_credentials_error(self, MockClient, db_conn):
        from aws_cost_anomalies.nlq.bedrock_client import (
            BedrockError,
        )

        MockClient.side_effect = BedrockError(
            "AWS credentials not found"
        )

        with pytest.raises(AgentError, match="credentials"):
            run_agent("test", db_conn)

    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_max_iterations_exceeded(self, MockClient, db_conn):
        mock_client = MagicMock()

        # Always return a tool_use — agent should eventually bail
        mock_client.converse.return_value = (
            _bedrock_tool_use_response(
                "query_cost_database",
                {"sql": "SELECT 1"},
            )
        )
        MockClient.return_value = mock_client

        with pytest.raises(AgentError, match="iterations"):
            run_agent("loop forever", db_conn, max_iterations=3)

    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_tool_error_returned_to_agent(
        self, MockClient, db_conn
    ):
        """Tool errors are returned as results, not raised."""
        mock_client = MagicMock()

        # First call: use a bad SQL query
        # Second call: agent adapts with correct query
        mock_client.converse.side_effect = [
            _bedrock_tool_use_response(
                "query_cost_database",
                {"sql": "SELECT * FROM nonexistent"},
                "tu_1",
            ),
            _bedrock_text_response(
                "Sorry, that table doesn't exist."
            ),
        ]
        MockClient.return_value = mock_client

        response = run_agent("test", db_conn)

        # The agent should have recovered
        assert len(response.steps) == 1
        assert "error" in response.steps[0].tool_result


class TestOnStepCallback:
    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_callback_invoked(self, MockClient, db_conn):
        mock_client = MagicMock()
        mock_client.converse.side_effect = [
            _bedrock_tool_use_response(
                "query_cost_database",
                {"sql": "SELECT 1 AS x"},
            ),
            _bedrock_text_response("Done."),
        ]
        MockClient.return_value = mock_client

        steps_seen: list[AgentStep] = []

        def on_step(step: AgentStep):
            steps_seen.append(step)

        run_agent("test", db_conn, on_step=on_step)

        assert len(steps_seen) == 1
        assert steps_seen[0].tool_name == "query_cost_database"
        assert steps_seen[0].tool_result is not None


class TestConversationMemory:
    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_response_includes_messages(self, MockClient, db_conn):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_text_response(
            "EC2 is the most expensive."
        )
        MockClient.return_value = mock_client

        response = run_agent("top services?", db_conn)

        assert len(response.messages) == 2
        # First message is the user question
        assert response.messages[0]["role"] == "user"
        assert response.messages[0]["content"][0]["text"] == "top services?"
        # Second message is the assistant answer
        assert response.messages[1]["role"] == "assistant"

    @patch("aws_cost_anomalies.nlq.agent.BedrockClient")
    def test_history_passed_to_next_call(self, MockClient, db_conn):
        mock_client = MagicMock()
        mock_client.converse.return_value = _bedrock_text_response(
            "EC2 is the most expensive."
        )
        MockClient.return_value = mock_client

        # First question
        resp1 = run_agent("top services?", db_conn)

        # Second question with history from first
        mock_client.converse.return_value = _bedrock_text_response(
            "us-east-1 is the most expensive region."
        )
        resp2 = run_agent(
            "how about by region?", db_conn, history=resp1.messages
        )

        # Response messages should contain the full conversation
        # (user1, assistant1, user2, assistant2)
        assert len(resp2.messages) == 4
        assert resp2.messages[0]["content"][0]["text"] == "top services?"
        assert resp2.messages[2]["content"][0]["text"] == "how about by region?"
