"""
Shared agent execution loop with message trimming and token tracking.
All 4 agentic nodes (data_acquirer, data_preparer, output_producer, reviewer)
delegate to run_agent_loop instead of duplicating the ~30-line pattern.
"""
import logging

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from rich.console import Console

from agents.message_utils import trim_messages
from agents.token_tracker import TokenTracker

log = logging.getLogger("agent_runner")
_console = Console()


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
        _console.print(f"  [dim]iter {iteration + 1}/{max_iterations}[/dim] — calling LLM...")
        messages = trim_messages(messages)
        response = llm.invoke(messages)
        tracker.record(response)
        messages.append(response)

        if not response.tool_calls:
            _console.print(f"  [green]✓[/green] Agent done after {iteration + 1} iterations — {tracker.summary()}")
            log.info(f"{stage_name} finished after {iteration + 1} iterations — {tracker.summary()}")
            break

        for tool_call in response.tool_calls:
            tool_name = tool_call["name"]
            tool_id = tool_call["id"]
            # Print a short summary of args (truncate long values)
            args = tool_call.get("args", {})
            args_preview = ", ".join(
                f"{k}={repr(v)[:60]}{'…' if len(repr(v)) > 60 else ''}"
                for k, v in args.items()
                if k != "script_content"  # script bodies are too long to show
            )
            _console.print(f"    [cyan]→[/cyan] {tool_name}({args_preview})")
            if tool_name not in tool_map:
                result = f"ERROR: Unknown tool '{tool_name}'"
            else:
                try:
                    result = tool_map[tool_name].invoke(tool_call["args"])
                except Exception as e:
                    result = f"ERROR executing {tool_name}: {e}"
            # Print result summary
            if isinstance(result, dict):
                success = result.get("success", result.get("returncode") == 0)
                if "success" in result or "returncode" in result:
                    if success:
                        _console.print(f"    [green]✓[/green] success")
                    else:
                        err = (result.get("stderr") or "")[:120].strip()
                        _console.print(f"    [red]✗[/red] failed — {err}")
            content = str(result)
            if len(content) > 4000:
                content = content[:4000] + f"\n[TRUNCATED — {len(str(result)):,} chars total]"
            messages.append(ToolMessage(content=content, tool_call_id=tool_id))
    else:
        _console.print(f"  [yellow]⚠[/yellow]  {stage_name} hit MAX_ITERATIONS ({max_iterations}) — {tracker.summary()}")
        log.warning(
            f"{stage_name} hit MAX_ITERATIONS ({max_iterations}) without finishing — "
            f"results may be incomplete. {tracker.summary()}"
        )

    return messages
