"""
Tests for message_utils — verifies trimming behaviour:
  - No trim when message count is below the limit
  - Trim keeps first 2 messages (system + initial human)
  - Trim never starts with a ToolMessage
"""
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agents.message_utils import trim_messages


def _make_messages(n_tool_pairs: int) -> list:
    """Build: [System, Human, (AI+tool_call, ToolMessage) × n_tool_pairs]"""
    msgs = [
        SystemMessage(content="system"),
        HumanMessage(content="initial"),
    ]
    for i in range(n_tool_pairs):
        ai = AIMessage(content="", tool_calls=[{"name": "read_file", "id": f"t{i}", "args": {}}])
        tool = ToolMessage(content=f"result {i}", tool_call_id=f"t{i}")
        msgs.extend([ai, tool])
    return msgs


def test_no_trim_when_under_limit():
    msgs = _make_messages(5)  # 2 + 10 = 12 messages, well under 40
    result = trim_messages(msgs, max_messages=40)
    assert result is msgs  # same object, no copy


def test_trim_at_limit():
    msgs = _make_messages(20)  # 2 + 40 = 42 messages
    result = trim_messages(msgs, max_messages=40)
    assert len(result) <= 40


def test_first_two_messages_always_kept():
    msgs = _make_messages(25)  # well over 40
    result = trim_messages(msgs, max_messages=40)
    assert isinstance(result[0], SystemMessage)
    assert isinstance(result[1], HumanMessage)
    assert result[0].content == "system"
    assert result[1].content == "initial"


def test_does_not_start_tail_on_tool_message():
    """After trimming, result[2] must never be a ToolMessage (orphaned result)."""
    msgs = _make_messages(25)
    result = trim_messages(msgs, max_messages=40)
    if len(result) > 2:
        assert not isinstance(result[2], ToolMessage), (
            "Trim left a ToolMessage as the first message after the head — "
            "its paired AIMessage was dropped, which would confuse the LLM."
        )


def test_single_pair_still_works():
    msgs = _make_messages(1)  # 4 messages total
    result = trim_messages(msgs, max_messages=40)
    assert result is msgs


def test_exact_limit_no_trim():
    msgs = _make_messages(19)  # 2 + 38 = 40 messages exactly
    result = trim_messages(msgs, max_messages=40)
    assert result is msgs


def test_trim_reduces_length():
    msgs = _make_messages(30)  # 2 + 60 = 62 messages
    result = trim_messages(msgs, max_messages=40)
    assert len(result) < len(msgs)
    assert len(result) <= 40
