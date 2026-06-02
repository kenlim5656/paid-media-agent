# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Shared agentic loop. Each agent subclass declares its tools and system prompt.
Uses the Anthropic tool-use loop with prompt caching on the system turn.
"""
import json
import structlog
import anthropic
from config import settings

log = structlog.get_logger()


class BaseAgent:
    name: str = "base"
    system_prompt: str = ""
    tools: list[dict] = []

    def __init__(self) -> None:
        self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    def run(self, task: str) -> str:
        messages: list[dict] = [{"role": "user", "content": task}]
        log.info("agent.start", agent=self.name, task=task[:120])

        while True:
            response = self.client.messages.create(
                model=settings.claude_model,
                max_tokens=4096,
                system=[
                    {
                        "type": "text",
                        "text": self.system_prompt,
                        "cache_control": {"type": "ephemeral"},  # cache system prompt
                    }
                ],
                tools=self.tools,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                final = next(
                    (b.text for b in response.content if hasattr(b, "text")), ""
                )
                log.info("agent.done", agent=self.name, chars=len(final))
                return final

            # Process tool calls
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                result = self._dispatch(block.name, block.input)
                log.info("tool.call", agent=self.name, tool=block.name, ok=result.get("ok", True))
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result),
                })

            messages.append({"role": "user", "content": tool_results})

    def _dispatch(self, tool_name: str, inputs: dict) -> dict:
        handler = getattr(self, f"_tool_{tool_name}", None)
        if handler is None:
            return {"error": f"Unknown tool: {tool_name}"}
        try:
            return handler(**inputs)
        except Exception as exc:
            log.error("tool.error", agent=self.name, tool=tool_name, error=str(exc))
            return {"error": str(exc)}
