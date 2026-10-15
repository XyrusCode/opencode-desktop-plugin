"""Unit tests for the wire protocol.

These are the pure functions the phone and bridge both depend on, so they get
the most cases: malformed input, unsupported types, and the event-translation
rules that decide what the phone actually renders.
"""

from __future__ import annotations

import json

import pytest

from bridge import protocol as p


# --- frame constants -------------------------------------------------------

class TestConstants:
    def test_model_selection_frame_exists(self):
        assert p.SELECT_MODEL == "select_model"

    def test_permission_reply_frame_exists(self):
        assert p.REPLY_PERMISSION == "reply_permission"

    def test_part_frame_exists(self):
        assert p.PART == "part"

    def test_history_frame_exists(self):
        assert p.HISTORY == "history"


# --- decode / require / encode ---------------------------------------------


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


# --- opencode event translation --------------------------------------------


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
        # opencode also deltas tool-call JSON; that must not reach the phone as a
        # text chunk.
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

    def test_unfinished_text_part_dropped(self):
        # Mid-stream part updates would duplicate text already sent as deltas.
        event = {
            "type": "message.part.updated",
            "properties": {"part": {"type": "text", "time": {"start": 1}}},
        }
        assert p.translate_opencode_event(event) is None

    def test_finished_text_part_becomes_message_done(self):
        # A completed text/reasoning part only signals end-of-turn; its content
        # already arrived as deltas.
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

    def test_finished_tool_part_forwarded_as_full_part(self):
        # Tool calls and results must reach the phone as structured objects so it
        # can render them as cards, not drop them as before.
        tool_part = {
            "type": "tool",
            "id": "prt_1",
            "sessionID": "ses_1",
            "messageID": "msg_1",
            "callID": "call_1",
            "tool": "bash",
            "state": {"result": "ok"},
        }
        event = {
            "type": "message.part.updated",
            "properties": {"sessionID": "ses_1", "part": tool_part | {"time": {"start": 1, "end": 2}}},
        }
        assert p.translate_opencode_event(event) == {
            "type": p.PART,
            "sessionID": "ses_1",
            "messageID": "msg_1",
            "part": tool_part | {"time": {"start": 1, "end": 2}},
        }

    def test_finished_file_and_snapshot_parts_forwarded(self):
        # File uploads and snapshot diffs are what turn opencode into the Claude
        # desktop view: forward them whole.
        for part_type, payload in [
            ("file", {"filename": "a.txt", "mime": "text/plain", "url": "data:,hi"}),
            ("snapshot", {"hash": "abc", "files": ["a.txt"]}),
            ("patch", {"hash": "abc", "files": ["a.txt"]}),
        ]:
            event = {
                "type": "message.part.updated",
                "properties": {
                    "sessionID": "ses_1",
                    "messageID": "msg_1",
                    "part": {"type": part_type, "id": "prt_2", **payload, "time": {"start": 1, "end": 2}},
                },
            }
            frame = p.translate_opencode_event(event)
            assert frame is not None
            assert frame["type"] == p.PART
            assert frame["part"]["type"] == part_type

    def test_unfinished_non_text_part_dropped(self):
        # Only finalized parts are forwarded; a half-built tool call is not useful.
        event = {
            "type": "message.part.updated",
            "properties": {"part": {"type": "tool", "time": {"start": 1}}},
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

    def test_permission_event_forwarded_as_prompt(self):
        # Push fast-path: if opencode emits a permission event on the stream it
        # is forwarded as a permission_prompt frame. The poller is the floor, but
        # a push event is delivered immediately.
        event = {
            "type": "permission.requested",
            "properties": {
                "id": "per_1",
                "sessionID": "ses_1",
                "permission": "bash",
                "patterns": ["rm -rf /"],
                "tool": {"callID": "c1", "messageID": "msg_1"},
            },
        }
        frame = p.translate_opencode_event(event)
        assert frame is not None
        assert frame["type"] == p.PERMISSION_PROMPT
        assert frame["requestID"] == "per_1"
        assert frame["permission"] == "bash"

    def test_question_event_forwarded_as_prompt(self):
        event = {
            "type": "question.requested",
            "properties": {
                "id": "que_1",
                "sessionID": "ses_1",
                "questions": [{"header": "Pick", "question": "pick one", "options": [{"label": "a"}]}],
            },
        }
        frame = p.translate_opencode_event(event)
        assert frame is not None
        assert frame["type"] == p.QUESTION_PROMPT


# --- frame builders --------------------------------------------------------


class TestBuilders:
    def test_providers_frame_trims_models(self):
        providers = [
            {
                "id": "openai",
                "name": "OpenAI",
                "models": {
                    "gpt-4o": {
                        "id": "gpt-4o",
                        "name": "GPT-4o",
                        "providerID": "openai",
                        "cost": {"input": 3, "output": 6},
                        "limit": {"context": 128000, "output": 16000},
                    }
                },
            }
        ]
        frame = p.providers_frame(providers)
        assert frame["type"] == p.PROVIDERS
        m = frame["providers"][0]["models"][0]
        # The phone only needs identity + cost/limit, not the whole provider blob.
        assert set(m) == {"id", "name", "providerID", "cost", "limit"}

    def test_agents_frame(self):
        agents = [{"name": "build", "description": "default", "mode": "primary", "native": True}]
        frame = p.agents_frame(agents)
        assert frame["type"] == p.AGENTS
        a = frame["agents"][0]
        assert a["name"] == "build"
        assert a["description"] == "default"

    def test_projects_frame_keeps_icon_url(self):
        projects = [
            {
                "id": "proj_1",
                "name": "marcuscassel.org",
                "worktree": "/Users/Xyrus/Desktop/XyrusCode/marcuscassel.org",
                "vcs": "git",
                "icon": {"url": "data:image/png;base64,AAAA"},
            }
        ]
        frame = p.projects_frame(projects)
        assert frame["type"] == p.PROJECTS
        pr = frame["projects"][0]
        assert pr["id"] == "proj_1"
        assert pr["worktree"] == projects[0]["worktree"]
        assert pr["icon"]["url"].startswith("data:image/png")

    def test_permission_prompt_frame(self):
        request = {
            "id": "per_1",
            "sessionID": "ses_1",
            "permission": "bash",
            "patterns": ["rm -rf /"],
            "tool": {"callID": "c1", "messageID": "msg_1"},
            "always": [],
        }
        frame = p.permission_prompt_frame(request)
        assert frame["type"] == p.PERMISSION_PROMPT
        assert frame["requestID"] == "per_1"
        assert frame["permission"] == "bash"
        assert frame["tool"] == {"callID": "c1", "messageID": "msg_1"}

    def test_question_prompt_frame(self):
        request = {
            "id": "que_1",
            "sessionID": "ses_1",
            "tool": {"callID": "c1", "messageID": "msg_1"},
            "questions": [
                {
                    "header": "Pick",
                    "question": "pick one",
                    "multiple": False,
                    "options": [{"label": "a"}, {"label": "b"}],
                }
            ],
        }
        frame = p.question_prompt_frame(request)
        assert frame["type"] == p.QUESTION_PROMPT
        assert frame["requestID"] == "que_1"
        q = frame["questions"][0]
        assert q["header"] == "Pick"
        assert [o["label"] for o in q["options"]] == ["a", "b"]

    def test_model_frame(self):
        frame = p.model_frame("ses_1", model_id="gpt-4o", provider_id="openai")
        assert frame == {
            "type": p.MODEL,
            "sessionID": "ses_1",
            "modelID": "gpt-4o",
            "providerID": "openai",
        }


# --- session summary + history --------------------------------------------


class TestSessionSummary:
    def test_extracts_phone_fields(self):
        session = {
            "id": "ses_1",
            "title": "Fix CI",
            "directory": "C:\\work",
            "time": {"updated": 123},
            "model": {"id": "m", "providerID": "p"},
            "agent": "build",
            "permission": [{"permission": "bash", "action": "deny"}],
        }
        assert p.session_summary(session) == {
            "id": "ses_1",
            "title": "Fix CI",
            "directory": "C:\\work",
            "updated": 123,
            "model": "m",
            "provider": "p",
            "agent": "build",
        }

    def test_tolerates_missing_fields(self):
        summary = p.session_summary({})
        assert summary["id"] is None
        assert summary["title"] == "(untitled)"
        assert summary.get("agent") is None


class TestHistoryFrame:
    def test_structures_role_and_parts(self):
        # Rich history: every part is forwarded whole so the phone can render
        # text, tool calls, diffs and errors instead of flat strings.
        messages = [
            {"info": {"role": "user"}, "parts": [{"type": "text", "text": "build a button"}]},
            {
                "info": {"role": "assistant"},
                "parts": [
                    {"type": "text", "text": "ok"},
                    {"type": "tool", "tool": "bash", "state": {"result": "ok"}},
                ],
            },
        ]
        frame = p.history_frame("ses_1", messages)
        assert frame == {
            "type": p.HISTORY,
            "sessionID": "ses_1",
            "messages": [
                {"role": "user", "parts": [{"type": "text", "text": "build a button"}]},
                {"role": "assistant", "parts": [{"type": "text", "text": "ok"}, {"type":"tool", "tool":"bash", "state":{"result":"ok"}}]},
            ],
        }

    def test_skips_messages_with_no_parts(self):
        frame = p.history_frame("ses_1", [{"info": {"role": "user"}, "parts": []}])
        # The envelope is still valid; the phone just gets no lines to render.
        assert frame["messages"] == []

    def test_carries_session_id(self):
        frame = p.history_frame("ses_1", [])
        assert frame["sessionID"] == "ses_1"
        assert frame["messages"] == []
