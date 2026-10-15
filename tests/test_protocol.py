"""Unit tests for the wire protocol.

These are the pure functions the phone and bridge both depend on, so they get
the most cases: malformed input, unsupported types, and the event-translation
rules that decide what the phone actually renders.
"""

from __future__ import annotations

import json

import pytest

from bridge import protocol as p


class TestDecode:
    def test_parses_well_formed_frame(self):
        assert p.decode('{"type":"ping"}') == {"type": "ping"}

    def test_rejects_invalid_json(self):
        with pytest.raises(p.ProtocolError):
            p.decode("not json")

    def test_rejects_json_array(self):
        # A bare array parses fine as JSON but is not a frame.
        with pytest.raises(p.ProtocolError):
            p.decode("[1,2,3]")

    def test_rejects_missing_type(self):
        with pytest.raises(p.ProtocolError):
            p.decode('{"token":"x"}')

    def test_rejects_non_string_type(self):
        with pytest.raises(p.ProtocolError):
            p.decode('{"type":123}')

    def test_rejects_empty_type(self):
        with pytest.raises(p.ProtocolError):
            p.decode('{"type":""}')


class TestRequire:
    def test_returns_value_when_present(self):
        assert p.require({"sessionID": "ses_1"}, "sessionID") == "ses_1"

    @pytest.mark.parametrize("frame", [{}, {"sessionID": None}, {"sessionID": ""}])
    def test_raises_when_missing_or_blank(self, frame):
        with pytest.raises(p.ProtocolError):
            p.require(frame, "sessionID")


class TestEncode:
    def test_round_trips(self):
        frame = {"type": p.OUTPUT_DELTA, "delta": "hi"}
        assert p.decode(p.encode(frame)) == frame

    def test_is_compact_json(self):
        assert p.encode({"type": p.PONG}) == '{"type":"pong"}'


class TestTranslateEvent:
    def test_text_delta_becomes_output_delta(self):
        frame = p.translate_opencode_event(
            {
                "type": "message.part.delta",
                "properties": {
                    "sessionID": "ses_1",
                    "messageID": "msg_1",
                    "field": "text",
                    "delta": "hello",
                },
            }
        )
        assert frame == {
            "type": p.OUTPUT_DELTA,
            "sessionID": "ses_1",
            "messageID": "msg_1",
            "delta": "hello",
        }

    def test_non_text_field_delta_dropped(self):
        # opencode also deltas tool-call JSON; that must not reach the phone.
        event = {
            "type": "message.part.delta",
            "properties": {"field": "input", "delta": '{"x":1}'},
        }
        assert p.translate_opencode_event(event) is None

    def test_empty_delta_dropped(self):
        event = {
            "type": "message.part.delta",
            "properties": {"field": "text", "delta": ""},
        }
        assert p.translate_opencode_event(event) is None

    def test_unfinished_part_dropped(self):
        # Mid-stream part updates would duplicate text already sent as deltas.
        event = {
            "type": "message.part.updated",
            "properties": {"part": {"type": "text", "time": {"start": 1}}},
        }
        assert p.translate_opencode_event(event) is None

    def test_finished_text_part_becomes_message_done(self):
        event = {
            "type": "message.part.updated",
            "properties": {
                "sessionID": "ses_1",
                "part": {"type": "text", "messageID": "msg_1", "time": {"end": 2}},
            },
        }
        assert p.translate_opencode_event(event) == {
            "type": p.MESSAGE_DONE,
            "sessionID": "ses_1",
            "messageID": "msg_1",
        }

    def test_finished_tool_part_dropped(self):
        event = {
            "type": "message.part.updated",
            "properties": {"part": {"type": "tool", "time": {"end": 2}}},
        }
        assert p.translate_opencode_event(event) is None

    def test_status_event(self):
        event = {
            "type": "session.status",
            "properties": {"sessionID": "ses_1", "status": {"type": "busy"}},
        }
        assert p.translate_opencode_event(event) == {
            "type": p.STATUS,
            "sessionID": "ses_1",
            "status": "busy",
        }

    def test_idle_event_carries_reason(self):
        event = {"type": "session.idle", "properties": {"sessionID": "ses_1"}}
        assert p.translate_opencode_event(event) == {
            "type": p.MESSAGE_DONE,
            "sessionID": "ses_1",
            "messageID": None,
            "reason": "idle",
        }

    @pytest.mark.parametrize(
        "event",
        [
            {"type": "server.heartbeat"},
            {"type": "session.diff", "properties": {"diff": []}},
            {"type": "lsp.diagnostics"},
            {},
        ],
    )
    def test_uninteresting_events_dropped(self, event):
        assert p.translate_opencode_event(event) is None

    def test_event_with_null_properties_does_not_raise(self):
        assert p.translate_opencode_event({"type": "session.idle"}) is not None


class TestSessionSummary:
    def test_extracts_phone_fields(self):
        session = {
            "id": "ses_1",
            "title": "Fix CI",
            "directory": "C:\\work",
            "time": {"updated": 123},
            "model": {"id": "m", "providerID": "p"},
            "permission": [{"permission": "bash", "action": "deny"}],
        }
        assert p.session_summary(session) == {
            "id": "ses_1",
            "title": "Fix CI",
            "directory": "C:\\work",
            "updated": 123,
            "model": "m",
            "provider": "p",
        }

    def test_tolerates_missing_fields(self):
        summary = p.session_summary({})
        assert summary["id"] is None
        assert summary["title"] == "(untitled)"


class TestHistoryFrame:
    def test_flattens_roles(self):
        messages = [
            {"info": {"role": "user"}, "parts": [{"type": "text", "text": "hello"}]},
            {"info": {"role": "assistant"}, "parts": [{"type": "text", "text": "hi there"}]},
        ]
        frame = p.history_frame("ses_1", messages)
        assert frame["type"] == p.SESSIONS
        assert frame["kind"] == "history"
        assert frame["lines"] == ["you: hello", "opencode: hi there"]

    def test_skips_empty_and_tool_only_messages(self):
        messages = [
            {"info": {"role": "user"}, "parts": []},
            {
                "info": {"role": "assistant"},
                "parts": [{"type": "tool", "state": {"output": "junk"}}],
            },
        ]
        assert p.history_frame("ses_1", messages)["lines"] == []

    def test_joins_multiple_text_parts(self):
        messages = [
            {
                "info": {"role": "assistant"},
                "parts": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}],
            }
        ]
        assert p.history_frame("ses_1", messages)["lines"] == ["opencode: ab"]
