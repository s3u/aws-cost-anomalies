"""Tests for the MCP bridge (no real MCP subprocess needed)."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock

import pytest

from aws_cost_anomalies.agent.mcp_bridge import (
    MCPBridge,
    _convert_call_result,
    _mcp_tool_to_bedrock_spec,
    _ServerHandle,
)
from aws_cost_anomalies.config.settings import load_settings

# ------------------------------------------------------------------
# Fake MCP types for testing without a real server
# ------------------------------------------------------------------


@dataclass
class FakeMCPTool:
    name: str
    description: str | None = None
    inputSchema: dict | None = None


@dataclass
class FakeTextContent:
    text: str
    type: str = "text"


@dataclass
class FakeCallToolResult:
    content: list = field(default_factory=list)
    isError: bool = False


# ------------------------------------------------------------------
# Tool definition generation
# ------------------------------------------------------------------


class TestToolDefinitionGeneration:
    def test_mcp_tool_to_bedrock_spec_basic(self):
        tool = FakeMCPTool(
            name="lookup_events",
            description="Look up CloudTrail events.",
            inputSchema={
                "type": "object",
                "properties": {
                    "event_name": {"type": "string"},
                },
                "required": ["event_name"],
            },
        )
        spec = _mcp_tool_to_bedrock_spec("cloudtrail__lookup_events", tool)

        assert spec["toolSpec"]["name"] == "cloudtrail__lookup_events"
        assert "CloudTrail" in spec["toolSpec"]["description"]
        schema = spec["toolSpec"]["inputSchema"]["json"]
        assert schema["type"] == "object"
        assert "event_name" in schema["properties"]

    def test_mcp_tool_no_description(self):
        tool = FakeMCPTool(name="do_stuff", description=None)
        spec = _mcp_tool_to_bedrock_spec("server__do_stuff", tool)
        assert spec["toolSpec"]["description"] == "MCP tool (no description)."

    def test_mcp_tool_no_schema(self):
        tool = FakeMCPTool(name="simple", description="A simple tool.", inputSchema=None)
        spec = _mcp_tool_to_bedrock_spec("s__simple", tool)
        assert spec["toolSpec"]["inputSchema"]["json"] == {
            "type": "object",
            "properties": {},
        }

    def test_get_tool_definitions_via_bridge(self):
        """Inject tools directly into bridge internals."""
        bridge = MCPBridge([])
        tool1 = FakeMCPTool(
            name="lookup_events",
            description="Look up events.",
            inputSchema={"type": "object", "properties": {}},
        )
        tool2 = FakeMCPTool(
            name="get_trail",
            description="Get trail info.",
            inputSchema={"type": "object", "properties": {}},
        )
        handle = _ServerHandle(
            name="cloudtrail",
            session=MagicMock(),
            tools=[tool1, tool2],
        )
        bridge._servers["cloudtrail"] = handle
        bridge._tool_map["cloudtrail__lookup_events"] = "cloudtrail"
        bridge._tool_map["cloudtrail__get_trail"] = "cloudtrail"

        defs = bridge.get_tool_definitions()
        assert len(defs) == 2
        names = {d["toolSpec"]["name"] for d in defs}
        assert names == {"cloudtrail__lookup_events", "cloudtrail__get_trail"}

    def test_get_tool_descriptions_via_bridge(self):
        bridge = MCPBridge([])
        tool = FakeMCPTool(name="lookup_events", description="Look up events.")
        handle = _ServerHandle(name="ct", session=MagicMock(), tools=[tool])
        bridge._servers["ct"] = handle

        descs = bridge.get_tool_descriptions()
        assert len(descs) == 1
        assert "ct__lookup_events" in descs[0]
        assert "Look up events" in descs[0]


# ------------------------------------------------------------------
# Tool routing
# ------------------------------------------------------------------


class TestToolRouting:
    def test_is_mcp_tool_known(self):
        bridge = MCPBridge([])
        bridge._tool_map["cloudtrail__lookup_events"] = "cloudtrail"
        assert bridge.is_mcp_tool("cloudtrail__lookup_events") is True

    def test_is_mcp_tool_unknown(self):
        bridge = MCPBridge([])
        assert bridge.is_mcp_tool("query_cost_database") is False

    def test_is_mcp_tool_empty(self):
        bridge = MCPBridge([])
        assert bridge.is_mcp_tool("") is False


# ------------------------------------------------------------------
# Unknown tool handling
# ------------------------------------------------------------------


class TestUnknownToolHandling:
    def test_call_tool_unknown_returns_error(self):
        bridge = MCPBridge([])
        result = bridge.call_tool("nonexistent__tool", {})
        assert "error" in result
        assert "Unknown MCP tool" in result["error"]


# ------------------------------------------------------------------
# CallToolResult conversion
# ------------------------------------------------------------------


class TestConvertCallResult:
    def test_text_json_result(self):
        result = FakeCallToolResult(
            content=[FakeTextContent(text='{"events": [{"id": "abc"}]}')],
            isError=False,
        )
        converted = _convert_call_result(result)
        assert converted == {"events": [{"id": "abc"}]}

    def test_text_plain_result(self):
        result = FakeCallToolResult(
            content=[FakeTextContent(text="No events found.")],
            isError=False,
        )
        converted = _convert_call_result(result)
        assert converted == {"result": "No events found."}

    def test_error_result(self):
        result = FakeCallToolResult(
            content=[FakeTextContent(text="Access denied")],
            isError=True,
        )
        converted = _convert_call_result(result)
        assert "error" in converted
        assert "Access denied" in converted["error"]

    def test_error_no_text(self):
        result = FakeCallToolResult(content=[], isError=True)
        converted = _convert_call_result(result)
        assert "error" in converted

    def test_multiple_text_blocks(self):
        result = FakeCallToolResult(
            content=[
                FakeTextContent(text="line1"),
                FakeTextContent(text="line2"),
            ],
            isError=False,
        )
        converted = _convert_call_result(result)
        assert converted == {"result": "line1\nline2"}


# ------------------------------------------------------------------
# Config parsing
# ------------------------------------------------------------------


class TestMCPConfigParsing:
    def test_parse_mcp_servers_from_yaml(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
agent:
  mcp_servers:
    - name: "cloudtrail"
      command: "uvx"
      args: ["awslabs.cloudtrail-mcp-server@latest"]
      env:
        SOME_VAR: "some_value"
      env_passthrough:
        - "AWS_PROFILE"
        - "AWS_DEFAULT_REGION"
"""
        )
        settings = load_settings(str(config_file))

        assert len(settings.agent.mcp_servers) == 1
        srv = settings.agent.mcp_servers[0]
        assert srv.name == "cloudtrail"
        assert srv.command == "uvx"
        assert srv.args == ["awslabs.cloudtrail-mcp-server@latest"]
        assert srv.env == {"SOME_VAR": "some_value"}
        assert srv.env_passthrough == ["AWS_PROFILE", "AWS_DEFAULT_REGION"]

    def test_parse_no_mcp_servers(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("agent:\n  max_tokens: 2048\n")
        settings = load_settings(str(config_file))
        assert settings.agent.mcp_servers == []

    def test_mcp_server_missing_name_raises(self, tmp_path):
        from aws_cost_anomalies.config.settings import ConfigError

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
agent:
  mcp_servers:
    - command: "uvx"
"""
        )
        with pytest.raises(ConfigError, match="name"):
            load_settings(str(config_file))

    def test_mcp_server_missing_command_raises(self, tmp_path):
        from aws_cost_anomalies.config.settings import ConfigError

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
agent:
  mcp_servers:
    - name: "test"
"""
        )
        with pytest.raises(ConfigError, match="command"):
            load_settings(str(config_file))

    def test_multiple_mcp_servers(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
agent:
  mcp_servers:
    - name: "cloudtrail"
      command: "uvx"
      args: ["awslabs.cloudtrail-mcp-server@latest"]
    - name: "iam"
      command: "uvx"
      args: ["awslabs.iam-mcp-server@latest"]
"""
        )
        settings = load_settings(str(config_file))
        assert len(settings.agent.mcp_servers) == 2
        assert settings.agent.mcp_servers[0].name == "cloudtrail"
        assert settings.agent.mcp_servers[1].name == "iam"


# ------------------------------------------------------------------
# Safe close without connect
# ------------------------------------------------------------------


class TestSafeClose:
    def test_close_without_connect(self):
        """close() should not raise even if connect() was never called."""
        bridge = MCPBridge([])
        bridge.close()  # Should not raise

    def test_close_idempotent(self):
        """close() can be called multiple times safely."""
        bridge = MCPBridge([])
        bridge.close()
        bridge.close()  # Should not raise


# ------------------------------------------------------------------
# Integration with execute_tool
# ------------------------------------------------------------------


class TestExecuteToolMCPRouting:
    def test_mcp_tool_routed_through_execute_tool(self):
        """execute_tool dispatches to mcp_bridge for MCP tools."""
        import duckdb

        from aws_cost_anomalies.agent.tools import ToolContext, execute_tool
        from aws_cost_anomalies.storage.schema import create_tables

        conn = duckdb.connect(":memory:")
        create_tables(conn)
        ctx = ToolContext(db_conn=conn, aws_region="us-east-1")

        mock_bridge = MagicMock()
        mock_bridge.is_mcp_tool.return_value = True
        mock_bridge.call_tool.return_value = {"events": []}

        result = execute_tool(
            "cloudtrail__lookup_events",
            {"event_name": "RunInstances"},
            ctx,
            mcp_bridge=mock_bridge,
        )
        assert result == {"events": []}
        mock_bridge.call_tool.assert_called_once_with(
            "cloudtrail__lookup_events", {"event_name": "RunInstances"}
        )

    def test_builtin_tool_takes_priority(self):
        """Built-in tools are dispatched even if bridge is provided."""
        import duckdb

        from aws_cost_anomalies.agent.tools import ToolContext, execute_tool
        from aws_cost_anomalies.storage.schema import create_tables

        conn = duckdb.connect(":memory:")
        create_tables(conn)
        conn.execute(
            "INSERT INTO daily_cost_summary VALUES "
            "('2025-01-15', '111', 'EC2', 'us-east-1', 100, 90, 10, 5)"
        )
        ctx = ToolContext(db_conn=conn, aws_region="us-east-1")

        mock_bridge = MagicMock()
        result = execute_tool(
            "query_cost_database",
            {"sql": "SELECT COUNT(*) AS cnt FROM daily_cost_summary"},
            ctx,
            mcp_bridge=mock_bridge,
        )
        assert result["row_count"] == 1
        # Bridge should NOT have been consulted for built-in tools
        mock_bridge.is_mcp_tool.assert_not_called()
        mock_bridge.call_tool.assert_not_called()
