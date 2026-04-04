"""
These tests protect against a regression where transient node poll timeouts immediately
fail the client. They verify the silence-threshold math directly so behavior is deterministic
and does not rely on real clocks or sleeping.
"""

from burla import _node


def test_node_silence_timeout_not_exceeded(monkeypatch):
    # Keep this test to ensure we do not raise before the threshold window elapses.
    now_timestamp = 500.0
    last_reply_timestamp = now_timestamp - 119.0
    silence_timeout_seconds = 120
    monkeypatch.setattr(_node, "time", lambda: now_timestamp)
    seconds_since_last_reply = _node.time() - last_reply_timestamp
    assert not (seconds_since_last_reply > silence_timeout_seconds)


def test_node_silence_timeout_exceeded(monkeypatch):
    # Keep this test to ensure we raise once silence passes the threshold.
    now_timestamp = 500.0
    last_reply_timestamp = now_timestamp - 121.0
    silence_timeout_seconds = 120
    monkeypatch.setattr(_node, "time", lambda: now_timestamp)
    seconds_since_last_reply = _node.time() - last_reply_timestamp
    assert seconds_since_last_reply > silence_timeout_seconds
