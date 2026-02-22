"""Agentic NLQ loop — Bedrock Converse with tool dispatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

import duckdb

if TYPE_CHECKING:
    from aws_cost_anomalies.nlq.mcp_bridge import MCPBridge

from aws_cost_anomalies.nlq.bedrock_client import (
    BedrockClient,
    BedrockError,
)
from aws_cost_anomalies.nlq.prompts import AGENT_SYSTEM_PROMPT
from aws_cost_anomalies.nlq.tools import (
    TOOL_DEFINITIONS,
    ToolContext,
    execute_tool,
)


class AgentError(Exception):
    """User-friendly error from the agent loop."""

    pass


@dataclass
class AgentStep:
    """One step in the agent's reasoning — a tool call or result."""

    tool_name: str
    tool_input: dict
    tool_result: dict | None = None


@dataclass
class AgentResponse:
    """Final output from the agent."""

    answer: str
    steps: list[AgentStep] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    messages: list[dict] = field(default_factory=list)


def run_agent(
    question: str,
    db_conn: duckdb.DuckDBPyConnection,
    model: str = "us.anthropic.claude-sonnet-4-20250514-v1:0",
    region: str = "us-east-1",
    max_tokens: int = 4096,
    max_iterations: int = 10,
    on_step: Callable[[AgentStep], None] | None = None,
    history: list[dict] | None = None,
    mcp_bridge: MCPBridge | None = None,
) -> AgentResponse:
    """Run the agentic NLQ loop.

    Sends the user question to Bedrock, executes tool calls in a
    loop, and returns the final text answer.

    Args:
        question: User's natural language question.
        db_conn: DuckDB connection for SQL tool.
        model: Bedrock model ID.
        region: AWS region for Bedrock and AWS API tools.
        max_tokens: Max tokens per Converse call.
        max_iterations: Safety limit on agent loop iterations.
        on_step: Optional callback invoked after each tool execution.
        history: Prior conversation messages for multi-turn context.

    Returns:
        AgentResponse with the final answer, steps, token usage,
        and updated messages for follow-up questions.

    Raises:
        AgentError: On Bedrock failures or if the loop is exhausted.
    """
    try:
        client = BedrockClient(region=region)
    except BedrockError as e:
        raise AgentError(str(e))

    context = ToolContext(db_conn=db_conn, aws_region=region)

    system_text = AGENT_SYSTEM_PROMPT
    all_tools = list(TOOL_DEFINITIONS)

    if mcp_bridge is not None:
        mcp_defs = mcp_bridge.get_tool_definitions()
        if mcp_defs:
            all_tools.extend(mcp_defs)
            mcp_descs = mcp_bridge.get_tool_descriptions()
            if mcp_descs:
                system_text += (
                    "\n\n## External MCP Tools\n\n"
                    "The following tools are provided by external MCP servers. "
                    "Use them when the built-in tools cannot answer the question "
                    "(e.g. CloudTrail for who launched resources, IAM for "
                    "permissions).\n\n" + "\n".join(mcp_descs)
                )

    system = [{"text": system_text}]
    tool_config = {"tools": all_tools}
    if history:
        messages: list[dict] = list(history)
    else:
        messages = []
    messages.append(
        {"role": "user", "content": [{"text": question}]}
    )

    steps: list[AgentStep] = []
    total_input_tokens = 0
    total_output_tokens = 0

    for _iteration in range(max_iterations):
        try:
            response = client.converse(
                model_id=model,
                messages=messages,
                system=system,
                tool_config=tool_config,
                max_tokens=max_tokens,
            )
        except BedrockError as e:
            raise AgentError(str(e))

        # Accumulate token usage
        usage = response.get("usage", {})
        total_input_tokens += usage.get("inputTokens", 0)
        total_output_tokens += usage.get("outputTokens", 0)

        stop_reason = response.get("stopReason", "end_turn")
        output = response.get("output", {})
        assistant_message = output.get("message", {})

        # Append the assistant's message to conversation
        messages.append(assistant_message)

        # If end_turn, extract final text answer
        if stop_reason == "end_turn":
            answer_parts = []
            for block in assistant_message.get("content", []):
                if "text" in block:
                    answer_parts.append(block["text"])
            return AgentResponse(
                answer="\n".join(answer_parts),
                steps=steps,
                input_tokens=total_input_tokens,
                output_tokens=total_output_tokens,
                messages=messages,
            )

        # If tool_use, execute each tool call
        if stop_reason == "tool_use":
            tool_results: list[dict] = []

            for block in assistant_message.get("content", []):
                if "toolUse" not in block:
                    continue

                tool_use = block["toolUse"]
                tool_name = tool_use["name"]
                tool_input = tool_use.get("input", {})
                tool_use_id = tool_use["toolUseId"]

                step = AgentStep(
                    tool_name=tool_name,
                    tool_input=tool_input,
                )

                # Execute the tool
                result = execute_tool(
                    tool_name, tool_input, context,
                    mcp_bridge=mcp_bridge,
                )
                step.tool_result = result

                if on_step:
                    on_step(step)

                steps.append(step)

                tool_results.append(
                    {
                        "toolResult": {
                            "toolUseId": tool_use_id,
                            "content": [
                                {"json": result}
                            ],
                        }
                    }
                )

            # Append tool results as a user message
            messages.append(
                {"role": "user", "content": tool_results}
            )
            continue

        # Unexpected stop reason — treat as final answer
        answer_parts = []
        for block in assistant_message.get("content", []):
            if "text" in block:
                answer_parts.append(block["text"])
        if answer_parts:
            return AgentResponse(
                answer="\n".join(answer_parts),
                steps=steps,
                input_tokens=total_input_tokens,
                output_tokens=total_output_tokens,
                messages=messages,
            )
        break

    raise AgentError(
        f"Agent did not produce a final answer after "
        f"{max_iterations} iterations. The question may be "
        f"too complex — try rephrasing."
    )
