"""System prompts for the agent."""

from pathlib import Path

_PROMPT_DIR = Path(__file__).parent


def _load_prompt(filename: str) -> str:
    return (_PROMPT_DIR / filename).read_text()


AGENT_SYSTEM_PROMPT = _load_prompt("system_prompt.md")
