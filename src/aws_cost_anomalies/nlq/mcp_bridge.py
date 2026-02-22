"""Bridge between the sync NLQ agent loop and async MCP servers."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import Tool as MCPTool

from aws_cost_anomalies.config.settings import MCPServerConfigEntry

logger = logging.getLogger(__name__)


@dataclass
class _ServerHandle:
    """Internal state for a connected MCP server."""

    name: str
    session: ClientSession
    tools: list[MCPTool] = field(default_factory=list)


class MCPBridge:
    """Manages MCP server subprocesses and provides a sync interface.

    Runs an asyncio event loop on a background thread so the sync
    agent loop can call MCP servers without going async itself.
    """

    def __init__(self, server_configs: list[MCPServerConfigEntry]) -> None:
        self._configs = server_configs
        self._servers: dict[str, _ServerHandle] = {}
        self._tool_map: dict[str, str] = {}  # prefixed_name -> server_name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        # Populated by _connect_all(); holds context managers we must clean up
        self._cm_stacks: list[Any] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> int:
        """Start MCP servers and discover tools. Returns tool count."""
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True
        )
        self._thread.start()

        total = asyncio.run_coroutine_threadsafe(
            self._connect_all(), self._loop
        ).result()
        return total

    async def _connect_all(self) -> int:
        total = 0
        for cfg in self._configs:
            try:
                count = await self._connect_server(cfg)
                total += count
                logger.info(
                    "MCP server '%s': %d tools discovered", cfg.name, count
                )
            except Exception:
                logger.exception(
                    "Failed to connect MCP server '%s'", cfg.name
                )
        return total

    async def _connect_server(self, cfg: MCPServerConfigEntry) -> int:
        env = dict(os.environ)
        env.update(cfg.env)
        for key in cfg.env_passthrough:
            val = os.environ.get(key)
            if val is not None:
                env[key] = val

        params = StdioServerParameters(
            command=cfg.command,
            args=cfg.args,
            env=env,
        )

        # stdio_client and session are async context managers.
        # We enter them and keep them alive until close().
        read_write_cm = stdio_client(params)
        read_stream, write_stream = await read_write_cm.__aenter__()
        self._cm_stacks.append(read_write_cm)

        session_cm = ClientSession(read_stream, write_stream)
        session = await session_cm.__aenter__()
        self._cm_stacks.append(session_cm)

        await session.initialize()
        result = await session.list_tools()

        handle = _ServerHandle(name=cfg.name, session=session, tools=result.tools)
        self._servers[cfg.name] = handle

        for tool in result.tools:
            prefixed = f"{cfg.name}__{tool.name}"
            self._tool_map[prefixed] = cfg.name

        return len(result.tools)

    def close(self) -> None:
        """Shut down all MCP servers and the background event loop."""
        if self._loop is None:
            return

        async def _cleanup() -> None:
            for cm in reversed(self._cm_stacks):
                try:
                    await cm.__aexit__(None, None, None)
                except Exception:
                    logger.debug("Error closing MCP context manager", exc_info=True)

        try:
            asyncio.run_coroutine_threadsafe(
                _cleanup(), self._loop
            ).result(timeout=10)
        except Exception:
            logger.debug("Error during MCP cleanup", exc_info=True)

        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)

        self._servers.clear()
        self._tool_map.clear()
        self._cm_stacks.clear()
        self._loop = None
        self._thread = None

    # ------------------------------------------------------------------
    # Tool discovery
    # ------------------------------------------------------------------

    def get_tool_definitions(self) -> list[dict]:
        """Return Bedrock Converse toolSpec dicts for all MCP tools."""
        definitions: list[dict] = []
        for handle in self._servers.values():
            for tool in handle.tools:
                prefixed = f"{handle.name}__{tool.name}"
                spec = _mcp_tool_to_bedrock_spec(prefixed, tool)
                definitions.append(spec)
        return definitions

    def get_tool_descriptions(self) -> list[str]:
        """Return human-readable descriptions for the system prompt."""
        descriptions: list[str] = []
        for handle in self._servers.values():
            for tool in handle.tools:
                prefixed = f"{handle.name}__{tool.name}"
                desc = tool.description or "No description."
                descriptions.append(f"- **{prefixed}** — {desc}")
        return descriptions

    # ------------------------------------------------------------------
    # Tool routing and execution
    # ------------------------------------------------------------------

    def is_mcp_tool(self, name: str) -> bool:
        """Check if a tool name belongs to an MCP server."""
        return name in self._tool_map

    def call_tool(self, name: str, arguments: dict) -> dict:
        """Execute an MCP tool call synchronously. Returns a dict."""
        server_name = self._tool_map.get(name)
        if server_name is None:
            return {"error": f"Unknown MCP tool: {name}"}

        handle = self._servers[server_name]
        # Strip the server prefix to get the original tool name
        original_name = name[len(server_name) + 2 :]  # skip "name__"

        if self._loop is None:
            return {"error": "MCP bridge is not connected"}

        future = asyncio.run_coroutine_threadsafe(
            handle.session.call_tool(original_name, arguments),
            self._loop,
        )
        try:
            result = future.result(timeout=60)
        except Exception as e:
            return {"error": f"MCP tool call failed: {e}"}

        return _convert_call_result(result)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _mcp_tool_to_bedrock_spec(prefixed_name: str, tool: MCPTool) -> dict:
    """Convert an MCP Tool to a Bedrock Converse toolSpec dict."""
    input_schema = tool.inputSchema if tool.inputSchema else {"type": "object", "properties": {}}
    return {
        "toolSpec": {
            "name": prefixed_name,
            "description": tool.description or "MCP tool (no description).",
            "inputSchema": {"json": input_schema},
        }
    }


def _convert_call_result(result: Any) -> dict:
    """Convert an MCP CallToolResult to a plain dict."""
    if result.isError:
        texts = []
        for block in result.content:
            if hasattr(block, "text"):
                texts.append(block.text)
        return {"error": " ".join(texts) if texts else "MCP tool returned an error"}

    texts = []
    for block in result.content:
        if hasattr(block, "text"):
            texts.append(block.text)

    combined = "\n".join(texts)

    # Try to parse as JSON for structured results
    try:
        return json.loads(combined)
    except (json.JSONDecodeError, ValueError):
        return {"result": combined}
