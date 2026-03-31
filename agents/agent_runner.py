"""
Shared agent execution loop with message trimming and token tracking.
All 4 agentic nodes (data_acquirer, data_preparer, output_producer, reviewer)
delegate to run_agent_loop instead of duplicating the ~30-line pattern.
"""
import logging

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from agents.message_utils import trim_messages
from agents.token_tracker import TokenTracker

log = logging.getLogger("agent_runner")


def run_agent_loop(
    llm,
    tools: list,
    system_prompt: str,
    initial_message: str,
    max_iterations: int,
    stage_name: str = "agent",
    max_cost_usd: float = 5.0,
) -> list:
    """
    Run the standard tool-calling loop.

    Args:
        llm: LangChain chat model with tools already bound via .bind_tools()
        tools: Tool objects (used to build the dispatch map)
        system_prompt: Content for the SystemMessage
        initial_message: Content for the first HumanMessage
        max_iterations: Max LLM calls before giving up
        stage_name: Name used in log messages
        max_cost_usd: Circuit breaker — raises if estimated cost exceeds this

    Returns:
        Final message list
    """
    tool_map = {t.name: t for t in tools}

    # Infer model name for cost tracking
    model_name = ""
    try:
        model_name = (
            llm.model if hasattr(llm, "model")
            else llm.model_name if hasattr(llm, "model_name")
            else ""
        )
    except Exception:
        pass
    tracker = TokenTracker(max_cost_usd=max_cost_usd, model=model_name)

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=initial_message),
    ]

    for iteration in range(max_iterations):
        messages = trim_messages(messages)
        response = llm.invoke(messages)
        tracker.record(response)
        messages.append(response)

        if not response.tool_calls:
            log.info(f"{stage_name} finished after {iteration + 1} iterations — {tracker.summary()}")
            break

        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_id = tool_call["id"]
            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                try:
                    result = tool_map[tool_name].invoke(tool_call["args"])
                except Exception as e:
                    result = f"ERROR executing {tool_name}: {e}"
            content = str(result)
            if len(content) > 4000:
                content = content[:4000] + f"\n[TRUNCATED — {len(str(result)):,} chars total]"
            messages.append(ToolMessage(content=content, tool_call_id=tool_id))
    else:
        log.warning(
            f"{stage_name} hit MAX_ITERATIONS ({max_iterations}) without finishing — "
            f"results may be incomplete. {tracker.summary()}"
        )

    return messages
