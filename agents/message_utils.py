"""
Message history trimming — sliding window to prevent unbounded token growth.
Keeps the SystemMessage and first HumanMessage intact; slides the rest.
"""
import logging
from langchain_core.messages import BaseMessage, ToolMessage

log = logging.getLogger("message_utils")

_MAX_MESSAGES = 40  # system + initial + ~38 tool-call/response pairs


def trim_messages(messages: list[BaseMessage], max_messages: int = _MAX_MESSAGES) -> list[BaseMessage]:
    """
    Trim a message list to prevent unbounded token growth.

    Strategy:
    - Always keep: messages[0] (SystemMessage) and messages[1] (first HumanMessage)
    - If len > max_messages: drop the oldest middle messages
    - Never start tail on a ToolMessage (would orphan a tool call result)
    """
    if len(messages) <= max_messages:
        return messages

    head = messages[:2]
    tail = messages[-(max_messages - 2):]

    # Don't start on a ToolMessage — its paired AIMessage was dropped
    while tail and isinstance(tail[0], ToolMessage):
        tail = tail[1:]

    n_dropped = len(messages) - len(head) - len(tail)
    if n_dropped > 0:
        log.debug(f"Trimmed {n_dropped} messages from history (kept {len(head)+len(tail)} of {len(messages)})")

    return head + tail
