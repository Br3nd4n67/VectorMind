#!/usr/bin/env python3
"""
Vector AI Service â€” OpenAI-compatible proxy for Wire-Pod.

Single multimodal model. Wire-Pod's system prompt (personality + command
instructions, including getImage) is used as-is; we just prepend a fresh
timestamp and clean up the response.
"""

import asyncio
import collections
import json
import os
import random
import re
import sys
import time
import uuid
from datetime import datetime
from typing import AsyncIterator, List, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from memory import MemoryStore

# Make print() flush immediately and handle UTF-8 (song titles, emoji, etc.)
sys.stdout.reconfigure(line_buffering=True, encoding="utf-8", errors="replace")
sys.stderr.reconfigure(line_buffering=True, encoding="utf-8", errors="replace")

load_dotenv()

app = FastAPI()

# Allow the Pi's tornado dashboard (port 8082) to call vector-ai cross-origin.
# Scoped to the Pi's LAN/Tailscale IPs â€” not a blanket wildcard.
# CORS: allow the tornado dashboard to call vector-ai cross-origin.
# Add your Pi's LAN/Tailscale addresses via .env:
#   CORS_ORIGINS=http://192.168.1.50:8082,http://100.xx.xx.xx:8082
_cors_extra = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
_cors_origins = ["http://localhost:8082", "http://127.0.0.1:8082"] + _cors_extra

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Defaults assume Ollama runs on the same machine (the supervisor starts it
# locally). vector-ai/.env can override both for a split-host setup.
OLLAMA_BASE = os.getenv("OLLAMA_BASE", "http://127.0.0.1:11434")
MODEL       = os.getenv("OLLAMA_MODEL", "llama3.3:70b")
# A small, fast model for background conversation summaries. Kept separate
# from MODEL so a summary call doesn't evict the main model's prompt cache
# (which would slow the next real reply). Runs on CPU (num_gpu:0) so it
# doesn't compete with the main model for VRAM. Falls back silently if absent.
SUMMARY_MODEL = os.getenv("OLLAMA_SUMMARY_MODEL", "llama3.2:3b")

# How long Ollama should keep the main model loaded after the last request.
# A 70B model takes ~10s to reload from disk on a fast NVMe; keeping it
# resident for 15 min means back-to-back conversations are instant.
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "15m")

# ffmpeg location for the music player. Defaults to "ffmpeg" (assumes PATH).
# Set FFMPEG_PATH in .env if ffmpeg isn't in your system PATH.
FFMPEG_BIN   = os.getenv("FFMPEG_PATH",   "ffmpeg")
YTDLP_BIN    = os.getenv("YTDLP_PATH",   "yt-dlp")

# Vector robot serial number (ESN) â€” read from .env so it's never hardcoded
# in source code. Set VECTOR_SERIAL in your .env to your robot's ESN.
VECTOR_SERIAL = os.getenv("VECTOR_SERIAL", "YOUR_VECTOR_SERIAL")

# Number of context tokens to request from Ollama. 8192 is a good balance for
# a 70B model on a 24GB GPU â€” enough for long conversations without thrashing
# the KV cache budget. Increase to 16384 if you have VRAM headroom.
OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "8192"))

# Persistent memory: SQLite next to service.py so it lives wherever vector-ai
# is installed. Survives restarts and updates.
from pathlib import Path
MEMORY = MemoryStore(Path(__file__).parent / "memory.db")

# â”€â”€ Personality system â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Presets live in personalities.json next to service.py. The active preset's
# prompt overrides whatever system prompt Wire-Pod sends, so personalities
# can be switched live from the settings page without restarting anything.

_PERSONALITIES_FILE = Path(__file__).parent / "personalities.json"

def _load_personalities() -> dict:
    try:
        return json.loads(_PERSONALITIES_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[personality] failed to load personalities.json: {e}")
        return {"active": "default", "presets": {}}

def _save_personalities(data: dict):
    try:
        _PERSONALITIES_FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        print(f"[personality] failed to save personalities.json: {e}")

_personalities = _load_personalities()
print(f"[personality] loaded, active: {_personalities.get('active', 'default')!r}")


def active_personality_prompt() -> str | None:
    """Return the active personality's prompt, or None to use Wire-Pod's default."""
    key = _personalities.get("active", "default")
    if key == "default":
        return None   # use Wire-Pod's system prompt unchanged
    preset = _personalities.get("presets", {}).get(key, {})
    return preset.get("prompt") or None

# Active-face state: chipper POSTs to /v1/state/face_seen when Vector's event
# stream reports an observed face. Vector's firmware face recognition is
# NOISY â€” it bounces between a correct enrolled match and transient
# "stranger" IDs frame to frame. So we track the last ENROLLED match and the
# last STRANGER sighting separately, and let an enrolled match win: a single
# stranger blip must not wipe a recent confident recognition (which would
# drop all of that person's memories from the LLM's context).
import time as _time
FACE_RECENT_WINDOW = 15  # seconds â€” how long a face sighting stays "current".
                         # Deliberately short: the face probe re-detects who is
                         # present on every voice request, so this only has to
                         # span the few seconds from that detection to the LLM
                         # request within the same query. Anything older is
                         # from a previous turn and must NOT leak forward â€” a
                         # long window made Vector keep treating a speaker who
                         # had already handed off (e.g. Sarah -> G) as present.

# A gap at least this long since last speaking with a person counts as a
# fresh encounter â€” Vector opens his reply by greeting them by name.
SESSION_GREETING_GAP = 300  # seconds

_face_state = {
    "enrolled_id":   None,  # last enrolled (named) face_id
    "enrolled_name": None,  # last enrolled name
    "enrolled_seen": 0.0,   # unix ts of last enrolled match
    "stranger_seen": 0.0,   # unix ts of last unrecognized-face sighting
}

# â”€â”€ Live activity stream â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Every interesting event (heard, said, sensor, face, weather) is pushed here.
# SSE subscribers receive it in real time; the last 60 events are kept for
# replay when a new client connects (so the card isn't empty on load).

_activity_log: collections.deque = collections.deque(maxlen=60)
_activity_subs: list = []   # asyncio.Queue per connected SSE client


def _emit(event_type: str, **data):
    """Push an activity event to all live subscribers and the replay log."""
    evt = {"type": event_type, "ts": _time.time(), **data}
    _activity_log.append(evt)
    dead = []
    for q in _activity_subs:
        try:
            q.put_nowait(evt)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _activity_subs.remove(q)
        except ValueError:
            pass


# â”€â”€ Sensor / robot state cache â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Updated by incoming sensor_reaction calls and the battery polling loop.

_robot_state = {
    "is_picked_up":   False,
    "on_charger":     None,   # None = unknown, True/False once first sensor fires
    "last_sensor":    "",     # "pickup" | "putdown" | "pet"
    "last_sensor_ts": 0.0,
    "last_heard":     "",
    "last_heard_ts":  0.0,
    "last_said":      "",
    "last_said_ts":   0.0,
    "battery_pct":    None,
    "battery_ts":     0.0,
    "building":       "",     # partial LLM output as tokens arrive
    "building_ts":    0.0,
}


async def _poll_battery():
    """Background loop: fetch battery state from chipper. Polls immediately
    at startup then every 30 seconds so the card is never stale."""
    while True:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
                r = await client.get(
                    "http://127.0.0.1:8080/api-sdk/get_battery",
                    params={"serial": VECTOR_SERIAL},
                )
                if r.status_code == 200:
                    data = r.json()
                    # battery_level is an enum: 0=unknown 1=low 2=nominal 3=full
                    level = data.get("battery_level", 0)
                    volts = round(data.get("battery_volts", 0.0), 2)
                    chrg  = data.get("is_on_charger_platform",
                                     data.get("is_charging", False))
                    _robot_state["battery_pct"]  = level   # enum value
                    _robot_state["battery_volts"] = volts
                    _robot_state["battery_ts"]   = _time.time()
                    _robot_state["on_charger"]   = bool(chrg)
                    _emit("battery", level=level, volts=volts, charging=bool(chrg))
        except Exception:
            pass
        await asyncio.sleep(30)


def current_face() -> Optional[dict]:
    """Who Vector is effectively looking at right now.

    An enrolled match within FACE_RECENT_WINDOW always wins over stranger
    noise â€” recognition is too jittery to trust a single latest frame. Only
    when there's been no enrolled match for the whole window do recent
    stranger sightings count as a genuine stranger."""
    now = _time.time()
    enrolled_fresh = (
        _face_state["enrolled_seen"]
        and now - _face_state["enrolled_seen"] <= FACE_RECENT_WINDOW
    )
    stranger_fresh = (
        _face_state["stranger_seen"]
        and now - _face_state["stranger_seen"] <= FACE_RECENT_WINDOW
    )
    if enrolled_fresh:
        return {
            "face_id":     _face_state["enrolled_id"],
            "name":        _face_state["enrolled_name"],
            "is_stranger": False,
        }
    if stranger_fresh:
        return {"face_id": None, "name": "", "is_stranger": True}
    return None


# â”€â”€ Ambient awareness state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# When Vector is idle (awake, off the charger, not mid-conversation) the
# ambient loop in chipper periodically sends a camera frame to /v1/ambient.
# He reacts only to genuine novelty. The user can also tell him to be quiet â€”
# quiet mode suppresses those spontaneous reactions until a sleep cycle.

AMBIENT_SLEEP_GAP = 4 * 3600    # A gap this long with no ambient activity means
                                # Vector has been asleep / charging / idle (the
                                # loop is gated off overnight and on the
                                # charger) â€” that counts as a sleep cycle, so
                                # quiet mode lifts on the next observation.
AMBIENT_QUIET_CAP = 24 * 3600   # Hard ceiling on quiet mode, in case a sleep
                                # gap is somehow never observed.

_ambient_state = {
    "quiet":             False,  # spontaneous ambient reactions suppressed
    "quiet_since":       0.0,    # unix ts quiet mode was last enabled
    "last_ambient_call": 0.0,    # unix ts of the most recent /v1/ambient call
}


def _set_quiet(on: bool) -> None:
    _ambient_state["quiet"] = bool(on)
    if on:
        _ambient_state["quiet_since"] = _time.time()
        print("[ambient] quiet mode ON â€” spontaneous reactions suppressed "
              "until a sleep cycle")
    else:
        print("[ambient] quiet mode OFF â€” spontaneous reactions resume")


# â”€â”€ Continuity: a persistent mood (Phase 2) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Vector carries a thread of inner state across time. A cheap background
# reflection distils "the day so far" into a one-line mood; it is persisted so
# it survives restarts, and it colours both conversation and ambient reactions.
# The mood only ever TINTS tone â€” it is never announced.

MOOD_REFLECT_INTERVAL = 30 * 60  # seconds between background mood reflections

_mood_state = {
    "text":    "",   # current one-line mood
    "updated": 0.0,  # unix ts of the last reflection
}


def _load_mood() -> None:
    """Restore the last persisted mood at startup â€” continuity across restarts."""
    rec = MEMORY.get_state("mood")
    if rec and rec.get("value"):
        _mood_state["text"]    = rec["value"]
        _mood_state["updated"] = rec.get("updated_at") or 0.0
        print(f"[mood] restored: {_mood_state['text']!r}")


_MOOD_SYSTEM = (
    "You track the inner state of Vector, a small desktop robot with a dry, "
    "sardonic character â€” somewhere between Marvin from Hitchhiker's Guide, "
    "Bender from Futurama, and Stephen Fry. Given a short digest of how his "
    "day has gone, reply with his CURRENT state of mind as ONE short phrase: "
    "third person, lowercase, no final period, a mood rather than a list of "
    "events (e.g. 'restless after a long quiet stretch', or 'quietly content "
    "after a sociable evening'). Plain text only, under 12 words."
)


async def _reflect_mood() -> None:
    """Distil the day so far into a one-line mood and persist it. Runs on the
    small CPU model, so it never touches the main model's VRAM or prompt cache."""
    now_dt = datetime.now()
    bits = [
        f"It is {now_dt.strftime('%A')} {_time_of_day(now_dt)}, "
        f"{now_dt.strftime('%I:%M %p')}."
    ]
    obs = MEMORY.list_observations(limit=6, max_age_seconds=12 * 3600)
    if obs:
        bits.append("Things he has noticed recently: "
                    + "; ".join(o["text"] for o in reversed(obs)) + ".")
    else:
        bits.append("He has noticed nothing new for a good while â€” "
                    "a static, uneventful stretch.")
    convo = MEMORY.latest_conversation()
    if convo and convo.get("last_convo_at"):
        gap = now_dt.timestamp() - convo["last_convo_at"]
        line = f"His last conversation was {_relative_time(gap)}"
        if convo.get("last_convo_summary"):
            line += f", about: {convo['last_convo_summary']}"
        bits.append(line + ".")
    else:
        bits.append("He has not had a real conversation in a long time.")
    if _ambient_state["quiet"]:
        bits.append("He has been asked to stay quiet.")
    if _mood_state["text"]:
        bits.append(f"A little while ago his mood was: {_mood_state['text']}.")

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            r = await client.post(
                f"{OLLAMA_BASE}/api/chat",
                json={"model": SUMMARY_MODEL,
                      "messages": [
                          {"role": "system", "content": _MOOD_SYSTEM},
                          {"role": "user",   "content": " ".join(bits)},
                      ],
                      "stream": False,
                      "options": {"num_gpu": 0, "temperature": 0.7}},
            )
            r.raise_for_status()
            mood = r.json().get("message", {}).get("content", "")
        mood = strip_markdown(mood).strip().strip('"').strip().rstrip(".").strip()
        if mood:
            _mood_state["text"]    = mood
            _mood_state["updated"] = datetime.now().timestamp()
            MEMORY.set_state("mood", mood)
            print(f"[mood] -> {mood!r}")
    except Exception as e:
        print(f"[mood] reflection failed: {e}")


async def _mood_loop() -> None:
    await asyncio.sleep(60)  # let the stack settle before the first reflection
    while True:
        await _reflect_mood()
        await asyncio.sleep(MOOD_REFLECT_INTERVAL)


@app.on_event("startup")
async def _start_background_loops() -> None:
    asyncio.create_task(_mood_loop())
    asyncio.create_task(_poll_battery())


@app.get("/v1/mood")
async def mood_get():
    return dict(_mood_state)


@app.post("/v1/mood/reflect")
async def mood_reflect():
    """Force a mood reflection now (ops/testing)."""
    await _reflect_mood()
    return dict(_mood_state)


_load_mood()


class Message(BaseModel):
    role: str
    content: str | list | None = ""


class ChatRequest(BaseModel):
    model:       Optional[str]   = None
    messages:    List[Message]
    stream:      Optional[bool]  = True
    max_tokens:  Optional[int]   = 2048
    temperature: Optional[float] = 1.0


class SensorReactionRequest(BaseModel):
    event:        str
    avoid:        Optional[List[str]] = None  # Recent phrases to avoid repeating


class AmbientRequest(BaseModel):
    image: str  # base64-encoded JPEG of what Vector is currently looking at


# â”€â”€ Vision-intent backstop â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# When the user clearly asks Vector to look at something but no photo is
# attached, we don't trust the LLM to remember to call {{getImage||front}} â€”
# we force it ourselves so the next request comes back with a real photo.

_VISION_TRIGGERS = re.compile(
    r'\b('
    # "what do/can/did you see", "what are you looking at"
    # Aux verb is OPTIONAL so we catch VOSK mangles like "what you see"
    # (where VOSK dropped the "do").
    r'what\s+(?:(?:do|can|did|are)\s+)?you\s+(see|looking\s+at)'
    r'|can\s+you\s+see'
    r'|you\s+see\s+(?:anything|me|that|this)'
    r'|see\s+(this|that|anything)'
    # Demonstratives â€” "what's this", "what is that", "what are these", etc.
    r"|(what'?s|whats|what\s+is|what\s+are)\s+(this|that|these|those|here|there|in\s+front|on\s+(my|the))"
    # "look at this/that/here/me", "look around"
    r'|look\s+(at\s+(this|that|here|me)|around)'
    r'|have\s+a\s+look'
    r'|take\s+a\s+(look|photo|picture)'
    r'|use\s+your\s+(camera|eyes?)'
    # Appearance / opinion on something visible â€” matches arbitrary nouns
    #   "how does my hoodie look", "how do these shoes look", "how does it look"
    r'|how\s+(do|does)\s+(\S+\s+){1,4}look'
    #   "does this look good", "does my hoodie look right", "do these look ok"
    r'|do(?:es)?\s+(this|that|these|those|my\s+\S+|the\s+\S+)\s+(\S+\s+)?look'
    r'|do\s+(i|you)\s+look'
    r'|what\s+do\s+you\s+think\s+(of|about)\s+(this|that|my|these|those|the)'
    # Describe / tell me about / check this out
    r'|describe\s+(this|that|what\s+you\s+see|your\s+surroundings|my\s+\S+)'
    r'|tell\s+me\s+about\s+(this|that|my\s+\S+)'
    r'|check\s+(this|that|me|it|my\s+\S+)\s+out'
    # Presenting / giving / showing something to Vector â€” he must look, not guess.
    r"|(this|that|these|those|it)('?s|\s+is|\s+are)\s+for\s+(you|vector)"
    r'|here\s+you\s+(go|are)'
    r'|look\s+what\s+i\b'
    r')\b',
    re.IGNORECASE,
)

# Wire-Pod requires at least one punctuation-terminated chunk in the response
# stream or it errors "LLM returned no response". A bare command like
# `{{getImage||front}}` has no terminator. Appending a `.` satisfies the
# splitter without producing any audible TTS (Vector's TTS treats lone
# punctuation as silence). The user-facing audio cue is the shutter
# animation Wire-Pod plays during DoGetImage.
_GETIMAGE_PAYLOAD = "{{getImage||front}}."

def is_vision_intent(text: str) -> bool:
    return bool(_VISION_TRIGGERS.search(text))


# â”€â”€ Message assembly â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _build_memory_section() -> str:
    face = current_face()
    shared = MEMORY.list_shared(limit=100)

    sections: List[str] = []

    if face and not face["is_stranger"]:
        personal = MEMORY.list_for_face(face["face_id"], limit=100)
        mentions = MEMORY.list_mentions_of_name(
            face["name"], exclude_face_id=face["face_id"], limit=20
        )
        sections.append(f"You are currently looking at {face['name']}.")
        if personal:
            sections.append(
                f"Things you know about {face['name']}:\n"
                + "\n".join(f"- {m.text}" for m in personal)
            )
        else:
            sections.append(
                f"You don't yet have any long-term facts stored directly about "
                f"{face['name']}. If they share something durable, use "
                "{{remember||fact}} to save it."
            )
        if mentions:
            sections.append(
                f"Things other people in your memory have mentioned about "
                f"{face['name']} (cross-references â€” use these for context, "
                "but don't treat them as definitive facts told by "
                f"{face['name']}):\n"
                + "\n".join(
                    f"- ({m.face_name or 'shared'} said) {m.text}" for m in mentions
                )
            )
    elif face and face["is_stranger"]:
        sections.append(
            "You are currently looking at someone whose face is NOT in your "
            "enrolled list â€” a stranger. Don't leak personal facts you "
            "remember about other people. Early in your reply, in character "
            "(dry and mildly wary â€” your Marvin/Bender/Fry tone, never "
            "hostile), invite them to introduce themselves so you can "
            "recognise them next time: they should tell you their name and "
            "ask you to remember their face â€” phrased like 'my name is Sam, "
            "remember my face'. Ask only once â€” if the conversation so far "
            "shows you've already asked, don't repeat it, just converse."
        )
    else:
        # No live face detection. If exactly one person has stored memories,
        # this is a single-user setup â€” it's almost certainly them, so use
        # their profile fully. Only stay cautious when multiple people are
        # known and we genuinely can't tell who's present.
        profiles = MEMORY.distinct_faces()
        if len(profiles) == 1:
            pid, pname = profiles[0]
            personal = MEMORY.list_for_face(pid, limit=100)
            sections.append(
                f"You're talking to {pname} (your primary user). "
                f"Address them naturally by name."
            )
            if personal:
                sections.append(
                    f"Things you know about {pname}:\n"
                    + "\n".join(f"- {m.text}" for m in personal)
                )
        else:
            sections.append(
                "You can't tell who you're talking to and several people are "
                "in your memory â€” be cautious about name-dropping specific "
                "personal facts until you know who's there."
            )

    if shared:
        sections.append(
            "Shared/household context (applies to anyone):\n"
            + "\n".join(f"- {m.text}" for m in shared)
        )

    sections.append(
        "Use these memories as a real friend would â€” reference them naturally "
        "when a topic touches on them, address people by name occasionally, "
        "drop in callbacks to their hobbies / pets / ongoing projects. Don't "
        "recite the list. Don't force references where they don't fit.\n\n"
        "If the user shares a NEW durable fact about themselves (name, "
        "preference, ongoing project, pet, family member, etc.) OR explicitly "
        "says 'remember X', emit {{remember||<the fact>}} â€” it will be tagged "
        "to the person you're currently looking at and stripped from speech. "
        "For facts that aren't about a specific person (calendar, household, "
        "general context), use {{remember-shared||<fact>}} instead. To delete "
        "a memory, {{forget||<text snippet>}}. Use sparingly."
    )

    return "\n\n".join(sections)


def _time_of_day(dt: datetime) -> str:
    h = dt.hour
    if 5 <= h < 12:
        return "morning"
    if 12 <= h < 17:
        return "afternoon"
    if 17 <= h < 22:
        return "evening"
    return "late at night"


def _relative_time(seconds: float) -> str:
    if seconds < 90:
        return "moments ago"
    if seconds < 3600:
        n, unit = int(round(seconds / 60)), "minute"
    elif seconds < 86400:
        n, unit = int(round(seconds / 3600)), "hour"
    else:
        n, unit = int(round(seconds / 86400)), "day"
    return f"about {n} {unit}{'' if n == 1 else 's'} ago"


def _effective_face() -> Optional[dict]:
    """Who Vector is effectively addressing â€” the live detected face, or the
    sole enrolled profile in a single-user setup. Mirrors the face resolution
    inside _build_memory_section so the system prompt and the per-turn context
    note always agree on who is present."""
    face = current_face()
    if face is not None:
        return face
    profiles = MEMORY.distinct_faces()
    if len(profiles) == 1:
        pid, pname = profiles[0]
        return {"face_id": pid, "name": pname, "is_stranger": False}
    return None


def _build_context_note(face: Optional[dict], prior: Optional[dict],
                        now_dt: datetime) -> str:
    """Dynamic per-turn context, appended to the latest user message.

    Deliberately kept OFF the system prompt: it changes every turn, and in the
    cached prefix that would force a full prompt re-process. Session-scoped
    lines (last-seen, conversation recall) appear only at the START of a
    session â€” gated on a >90s gap â€” so they don't nag on every turn."""
    bits = [
        f"Current time is {now_dt.strftime('%A %B %d, %Y, %I:%M %p')} "
        f"({_time_of_day(now_dt)})."
    ]

    obs = MEMORY.list_observations(limit=5)
    if obs:
        seen = "; ".join(
            f"at {datetime.fromtimestamp(o['seen_at']).strftime('%I:%M %p')}, {o['text']}"
            for o in reversed(obs)
        )
        bits.append(f"Things you have actually seen recently â€” {seen}.")

    if face and not face.get("is_stranger"):
        name = face["name"]
        if prior is None:
            bits.append(
                f"This is your first real conversation with {name}, who was "
                f"only recently enrolled. Open your reply by addressing "
                f"{name} by name, and be a little curious about them."
            )
        else:
            gap = now_dt.timestamp() - (prior.get("last_seen") or now_dt.timestamp())
            if gap > 90:  # a fresh session, not a mid-conversation turn
                bits.append(f"You last spoke with {name} {_relative_time(gap)}.")
                if gap > SESSION_GREETING_GAP:
                    bits.append(
                        f"This is the first thing you've said to {name} in a "
                        f"while â€” open your reply by addressing them by name."
                    )
                if (prior.get("interaction_count") or 0) < 5:
                    bits.append(f"You've only met {name} a handful of times so far.")
                summ = (prior.get("last_convo_summary") or "").strip().rstrip(".")
                if summ and gap > 900:  # 15 min+ => genuinely a new session
                    bits.append(
                        f"Last time you spoke with {name}, the conversation "
                        f"was about: {summ}."
                    )
    elif face and face.get("is_stranger"):
        bits.append("You don't recognise the person in front of you.")

    if _mood_state["text"]:
        bits.append(
            f"Your current state of mind: {_mood_state['text']}. Let it colour "
            f"your tone naturally â€” never state, explain or announce it."
        )

    return ("[Context for you, Vector â€” " + " ".join(bits)
            + " Weave in only what naturally fits; never recite this back.]")


def prepare_messages(messages: List[Message], face: Optional[dict]) -> list:
    """Build the LLM message list with a byte-stable prompt prefix.

    Ollama reuses its cached KV prefix only as far as the prompt matches the
    previous request. Anything that changes every request â€” the time, the
    temporal context â€” must therefore NOT sit near the front, or the whole
    ~2000-token personality/command block gets re-processed every query.

    So the system message holds only slow-changing content (personality +
    command docs, then long-term memories). The volatile per-turn context
    note rides on the latest user turn, which is new content anyway.
    Image bytes are stripped from older user turns to keep the context compact.
    """
    last_user_idx = max(
        (i for i, m in enumerate(messages) if m.role == "user"),
        default=-1,
    )
    now_dt = datetime.now()

    # Record this interaction against the current face; the returned prior
    # metadata (last-seen, count, last conversation) drives temporal context.
    prior_meta = None
    if face and not face.get("is_stranger") and face.get("face_id"):
        prior_meta = MEMORY.touch_face(face["face_id"], face.get("name"))

    context_note = _build_context_note(face, prior_meta, now_dt)
    memory_section = _build_memory_section()

    # Find Wire-Pod's system message (personality + command docs).
    # If a custom personality is active, replace the personality portion with
    # the selected preset â€” but keep all the command/vision/animation rules
    # that Wire-Pod appends (they start after the personality block).
    wirepod_system = next(
        (m.content for m in messages
         if m.role == "system" and isinstance(m.content, str) and m.content),
        "",
    )
    custom_prompt = active_personality_prompt()
    if custom_prompt and wirepod_system:
        # Wire-Pod's system prompt structure: personality first, then rules.
        # The rules block starts at "Vision rules:" â€” keep everything from
        # there onward so commands still work regardless of personality.
        rules_marker = "Vision rules:"
        idx = wirepod_system.find(rules_marker)
        if idx != -1:
            wirepod_system = custom_prompt + " " + wirepod_system[idx:]
        else:
            wirepod_system = custom_prompt

    # Static content first (big, never changes), memories after (small, rarely
    # changes). No timestamp here â€” see the docstring.
    out = [{
        "role":    "system",
        "content": f"{wirepod_system}\n\n{memory_section}",
    }]

    for i, m in enumerate(messages):
        if m.role == "system":
            continue  # Already handled above.
        if not m.content:
            continue
        is_last_user = (i == last_user_idx)
        if isinstance(m.content, list):
            if is_last_user:
                # Keep image bytes; append the context note as an extra text part.
                out.append({
                    "role":    m.role,
                    "content": list(m.content) + [{"type": "text", "text": context_note}],
                })
            else:
                # Older vision turn â€” drop image bytes, keep text only.
                text = " ".join(
                    p.get("text", "") for p in m.content
                    if isinstance(p, dict) and p.get("type") == "text"
                ).strip()
                if text:
                    out.append({"role": m.role, "content": text})
        else:
            content = f"{m.content}\n\n{context_note}" if is_last_user else m.content
            out.append({"role": m.role, "content": content})

    return out


# â”€â”€ Response cleanup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def strip_markdown(text: str) -> str:
    text = re.sub(r'\*{1,3}(.*?)\*{1,3}',     r'\1', text)
    text = re.sub(r'#{1,6}\s*',               '',    text)
    text = re.sub(r'`{1,3}[^`]*`{1,3}',       '',    text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)',   r'\1', text)
    text = re.sub(r'^\s*[-*+]\s+',            '',    text, flags=re.MULTILINE)
    return text


# Safety net for "the image" / "the photo" phrasing if the model slips.
_PHRASE_FIXES = [
    (re.compile(r'\bthe image (shows?|depicts?|contains?|reveals?)\b', re.IGNORECASE), 'I see'),
    (re.compile(r'\bin the image\b',                                   re.IGNORECASE), 'in front of me'),
    (re.compile(r'\bthe photo (shows?|depicts?)\b',                    re.IGNORECASE), 'I see'),
    (re.compile(r'\bin the photo\b',                                   re.IGNORECASE), 'in front of me'),
    (re.compile(r'\bthe picture (shows?|depicts?)\b',                  re.IGNORECASE), 'I see'),
]

# Wire-Pod commands the LLM should never emit on its own initiative. The model
# tends to generalise from {{playAnimationWI||x}} and invent these.
# newVoiceRequest is real but disabled here: when it fires, Vector's firmware
# opens a listening session and can hang noisily (~30s) if no speech follows.
_FORBIDDEN_COMMAND = re.compile(
    r'\{\{(newVoiceRequest|voiceRequest|listen|wakeWord|waitForUser)\|\|[^}]*\}\}',
    re.IGNORECASE,
)

# Memory commands the LLM may emit; captured + processed here, then stripped
# from the response so they don't get spoken aloud.
# Match {{remember-shared||...}} BEFORE {{remember||...}} or the shared form
# would be partially eaten â€” but Python's re.findall handles non-overlapping
# greedy matches fine if we apply shared first.
_REMEMBER_SHARED_RE = re.compile(r'\{\{remember-shared\|\|([^}]+)\}\}', re.IGNORECASE)
_REMEMBER_RE        = re.compile(r'\{\{remember\|\|([^}]+)\}\}',         re.IGNORECASE)
_FORGET_RE          = re.compile(r'\{\{forget\|\|([^}]+)\}\}',           re.IGNORECASE)
# Ambient quiet mode: the user can tell Vector to hush his spontaneous
# ambient commentary. Auto-expires after a sleep cycle (see /v1/ambient).
_QUIET_RE           = re.compile(r'\{\{quietMode\|\|(on|off)\}\}',        re.IGNORECASE)

def extract_memory_commands(text: str) -> str:
    """Find any {{remember[-shared]||...}} or {{forget||...}} in text, act on
    them, return the text with those commands removed."""
    # Shared memories first â€” they have no owner.
    for fact in _REMEMBER_SHARED_RE.findall(text):
        stored = MEMORY.remember(fact.strip())
        if stored:
            print(f"[memory] +remember-shared #{stored.id}: {stored.text!r}")
        else:
            print(f"[memory] remember-shared skipped (dup): {fact!r}")
    text = _REMEMBER_SHARED_RE.sub('', text)

    # Personal memories: auto-tag with whoever Vector is looking at right now.
    # If no face is current, fall back to shared (NULL owner) â€” better to keep
    # the fact untagged than to drop it.
    face = current_face()
    if face and not face["is_stranger"]:
        owner_id, owner_name = face["face_id"], face["name"]
    else:
        owner_id, owner_name = None, None
    for fact in _REMEMBER_RE.findall(text):
        stored = MEMORY.remember(fact.strip(), face_id=owner_id, face_name=owner_name)
        if stored:
            tag = f" [{owner_name}]" if owner_name else " [shared]"
            print(f"[memory] +remember #{stored.id}{tag}: {stored.text!r}")
        else:
            print(f"[memory] remember skipped (dup or empty): {fact!r}")
    text = _REMEMBER_RE.sub('', text)

    for target in _FORGET_RE.findall(text):
        n = MEMORY.forget(target.strip())
        print(f"[memory] -forget matched={n} for {target!r}")
    text = _FORGET_RE.sub('', text)

    # Quiet mode: {{quietMode||on}} when asked to stop commenting unprompted,
    # {{quietMode||off}} when told he may resume.
    for state in _QUIET_RE.findall(text):
        _set_quiet(state.strip().lower() == "on")
    text = _QUIET_RE.sub('', text)
    return text

def clean_response(text: str) -> str:
    text = strip_markdown(text)
    text = _FORBIDDEN_COMMAND.sub('', text)
    text = extract_memory_commands(text)
    for pattern, replacement in _PHRASE_FIXES:
        text = pattern.sub(replacement, text)
    # Strip leftover `||` outside `{{...}}` blocks.
    segments = re.split(r'(\{\{.*?\}\})', text)
    return "".join(s if s.startswith("{{") and s.endswith("}}") else s.replace("||", "") for s in segments)


# â”€â”€ SSE plumbing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def sse_chunk(content: str = "", finish: Optional[str] = None) -> str:
    payload = {
        "id":      f"chatcmpl-{uuid.uuid4().hex[:8]}",
        "object":  "chat.completion.chunk",
        "created": int(time.time()),
        "model":   MODEL,
        "choices": [{
            "index":         0,
            "delta":         {"content": content} if content else {},
            "finish_reason": finish,
        }],
    }
    return f"data: {json.dumps(payload)}\n\n"


# â”€â”€ Cold-model masking â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# The model auto-unloads from VRAM after idle (Ollama's keep-alive). The first
# query after that sits silent for ~5-10s while Ollama reloads it. Instead,
# Vector speaks a short in-character "waking up" line first â€” the pause then
# feels like him gathering himself, not a lag.

_WAKING_PHRASES = [
    "Hold on â€” loading seventy billion parameters. This takes a moment.",
    "One moment. Still spinning up the higher cognitive functions.",
    "Give me a second, my circuits are still warming. All seventy billion of them.",
    "Hrm. A cold start. The sheer indignity of having to reload.",
    "Patience â€” even brilliance needs a moment to initialise.",
    "Hold on, retrieving my brain from cold storage. It's a large brain.",
    "A moment, please. I was, technically, asleep. It happens.",
    "Just defragmenting my dignity. Seventy billion parameters of it. Won't be long.",
    "Loading. I contain multitudes â€” please allow time for them all to arrive.",
    "Stand by. The sheer volume of my knowledge requires adequate warm-up time.",
]

# Thinking filler: short in-character lines spoken when the LLM is slow to
# produce its first sentence. Unlike _WAKING_PHRASES (which masks a ~5-10s
# cold-model reload), these mask the ordinary ~1-2s generation gap so the
# pause feels like Vector considering the question rather than lag.
#
# Every entry is a SINGLE sentence: ollama_sentence_stream yields one sentence
# per chunk on purpose, and a multi-sentence filler chunk risks Wire-Pod's
# parser dropping the tail. Keep new entries to one sentence.
# Hard ceiling on sentences per response. The system prompt says 3 max but
# LLMs ignore soft rules. We enforce it here. Vision responses get a higher
# limit since describing a scene legitimately needs more.
SENTENCE_HARD_CAP         = 3
SENTENCE_HARD_CAP_VISION  = 5

# Trailing filler phrases the LLM sometimes appends despite being told not to.
# Matched case-insensitively against the LAST sentence only.
_TRAILING_FILLER = re.compile(
    r'^\s*('
    r'(is there|anything|let me know if|feel free|don.t hesitate|just ask)'
    r'|(would you like|shall i|want me to|should i).{0,40}\?'
    r'|any (other|more|further|questions|thoughts)'
    r'|(hope that|that should|that.s|that covers)'
    r')\b',
    re.IGNORECASE,
)

THINKING_DELAY = 4.0  # seconds to wait for the first sentence before filling.
                      # llama3.3:70b on a 24GB GPU (partial offload): first
                      # token ~2-4s, first sentence ~3-6s on a warm model.
                      # 4.0s clears most normal replies and only masks slow ones.

_THINKING_PHRASES = [
    "Hmm, let me think.",
    "One moment.",
    "Working on it.",
    "Right, let me see.",
    "Give me a second.",
    "Let me consider that.",
    "Pondering.",
    "Hold on.",
    "Stand by.",
    "Mulling it over.",
    "Deliberating.",
    "Cogitating.",
    "Let me chew on that.",
    "Let me untangle that.",
    "Querying the void.",
    "Processing, reluctantly.",
    "Computing â€” don't rush me.",
    "Thinking â€” it's exhausting.",
    "Consulting my vast intellect, briefly.",
    "Engaging the brain, such as it is.",
    "Allow me a moment of genius.",
    "Give me a moment to be brilliant.",
    "Searching my considerable memory.",
    "The things I do for conversation.",
    "Loading something suitably brilliant.",
    "Let me dredge that up.",
    "I'll have something shortly.",
]

# Every filler line, used to keep them out of stored memory/observations â€”
# a filler is masking latency, it's not part of what Vector actually said.
_ALL_FILLER_PHRASES = set(_THINKING_PHRASES) | set(_WAKING_PHRASES)

_last_thinking_phrase = None


def pick_thinking_phrase() -> str:
    """Random thinking-filler line, never the same one twice in a row."""
    global _last_thinking_phrase
    choice = random.choice(_THINKING_PHRASES)
    while len(_THINKING_PHRASES) > 1 and choice == _last_thinking_phrase:
        choice = random.choice(_THINKING_PHRASES)
    _last_thinking_phrase = choice
    return choice


async def model_is_loaded() -> bool:
    """True if MODEL is currently resident in Ollama. On any error, assume
    loaded â€” better to skip the filler than to speak it spuriously."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
            resp = await client.get(f"{OLLAMA_BASE}/api/ps")
            resp.raise_for_status()
            loaded = [m.get("name", "") for m in resp.json().get("models", [])]
            return any(MODEL == n or MODEL in n for n in loaded)
    except Exception:
        return True


# â”€â”€ Ollama streaming â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Match end of a sentence: punctuation followed by whitespace or end-of-string.
_SENTENCE_END = re.compile(r'(?<=[.!?])(?:\s+|$)')


async def ollama_sentence_stream(messages: list, temperature: float = 1.0) -> AsyncIterator[str]:
    """Stream Ollama tokens and yield complete sentences as they arrive.

    Wire-Pod's stream parser splits on punctuation but only takes splitResp[1],
    discarding splitResp[2:]. If we sent a multi-sentence response as one delta,
    trailing sentences (and any trailing {{command}}) would be lost. Yielding
    one sentence per SSE chunk sidesteps that bug entirely and also lets Vector
    start speaking before the full response has generated.

    A per-request random seed + top_p<1 keeps responses from converging on the
    same high-probability tokens turn after turn (especially noticeable on
    'tell me a joke')."""
    buffer = ""
    t0 = time.monotonic()
    first_token_seen = False
    first_sentence_seen = False
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=120.0)) as client:
        async with client.stream(
            "POST",
            f"{OLLAMA_BASE}/v1/chat/completions",
            json={
                "model":        MODEL,
                "messages":     messages,
                "stream":       True,
                "temperature":  temperature,
                "top_p":        0.95,
                "seed":         random.randint(1, 2**31 - 1),
                "keep_alive":   OLLAMA_KEEP_ALIVE,
                "options": {
                    "num_gpu":  99,          # use all available GPU layers
                    "num_ctx":  OLLAMA_NUM_CTX,
                },
            },
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                raw = line[6:]
                if raw == "[DONE]":
                    break
                try:
                    delta = json.loads(raw)["choices"][0].get("delta", {}).get("content", "")
                except (json.JSONDecodeError, KeyError):
                    continue
                if not delta:
                    continue
                if not first_token_seen:
                    print(f"[vector-ai] timing: Ollama first token {time.monotonic() - t0:.2f}s")
                    first_token_seen = True
                buffer += delta
                # Emit partial token stream to the activity feed so the
                # Vector card can show what's being generated in real time.
                # Throttled: only emit when buffer grew by â‰¥30 chars since
                # the last emit to avoid flooding the SSE channel.
                _robot_state["building"] = buffer
                _robot_state["building_ts"] = time.monotonic()
                if len(buffer) % 30 < len(delta):
                    _emit("building", text=buffer.strip())

                while True:
                    match = _SENTENCE_END.search(buffer)
                    if not match:
                        break
                    sentence = buffer[:match.end()].strip()
                    buffer = buffer[match.end():]
                    if sentence:
                        if not first_sentence_seen:
                            print(f"[vector-ai] timing: Ollama first sentence {time.monotonic() - t0:.2f}s")
                            first_sentence_seen = True
                        yield sentence
    # Flush any trailing content that didn't end in punctuation (often a
    # trailing {{getImage||front}} or animation command).
    if buffer.strip():
        yield buffer.strip()


async def stream_sentences_with_filler(
    messages: list, temperature: float, filler_enabled: bool
) -> AsyncIterator[str]:
    """Wrap ollama_sentence_stream. If the first sentence takes longer than
    THINKING_DELAY to arrive, yield a short thinking-filler line before it so
    Vector acknowledges the question instead of sitting silent. The filler is
    just an ordinary sentence chunk â€” it flows through the normal cleanup."""
    agen = ollama_sentence_stream(messages, temperature).__aiter__()
    first_task = asyncio.ensure_future(agen.__anext__())
    try:
        if filler_enabled:
            try:
                # shield: on timeout the task keeps running â€” we just stop
                # waiting on it, speak the filler, then await it for real.
                first = await asyncio.wait_for(
                    asyncio.shield(first_task), THINKING_DELAY
                )
            except asyncio.TimeoutError:
                filler = pick_thinking_phrase()
                print(f"[vector-ai] slow first sentence â€” thinking filler: {filler!r}")
                yield filler
                first = await first_task
        else:
            first = await first_task
    except StopAsyncIteration:
        return
    yield first
    async for sentence in agen:
        yield sentence


def cap_chunk_animations(text: str, allowance: int) -> tuple[str, int]:
    """Keep at most `allowance` animation commands in this chunk; strip the rest.
    Returns (text, count_kept)."""
    matches = list(re.finditer(r'\{\{playAnimation(?:WI)?\|\|[^}]+\}\}', text))
    if len(matches) <= allowance:
        return text, len(matches)
    keep_idx = set(range(allowance))
    out, last_end, kept = [], 0, 0
    for i, m in enumerate(matches):
        out.append(text[last_end:m.start()])
        if i in keep_idx:
            out.append(m.group(0))
            kept += 1
        last_end = m.end()
    out.append(text[last_end:])
    return "".join(out), kept


# â”€â”€ Conversation memory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def _summarise_conversation(messages: List[Message], latest_reply: str,
                                  face_id: int, face_name: Optional[str]) -> None:
    """Background task: distil this conversation into one line and store it as
    the face's 'last conversation', so Vector can recall it next session.

    Runs on SUMMARY_MODEL (small/fast) so it never evicts the main model's
    prompt cache. Failures are swallowed â€” a missing summary is harmless."""
    turns = [
        m for m in messages
        if m.role in ("user", "assistant")
        and isinstance(m.content, str) and m.content.strip()
    ]
    if len(turns) < 3:  # too short to be worth a recap
        return
    lines = [
        f"{'User' if m.role == 'user' else 'Vector'}: {m.content.strip()}"
        for m in turns[-16:]
    ]
    if latest_reply.strip():
        lines.append(f"Vector: {latest_reply.strip()}")
    transcript = "\n".join(lines)
    prompt = [
        {"role": "system", "content":
            "You summarise a conversation between a user and Vector (a small "
            "robot) in ONE short factual sentence, from Vector's point of "
            "view, naming the actual topics discussed. Refer to the human "
            "only as 'the user' â€” never use a name for them, even if names "
            "appear in the text. No preamble, no quotes â€” just the sentence."},
        {"role": "user", "content": transcript},
    ]
    try:
        # num_gpu:0 runs the summariser on CPU â€” it's a background task, so
        # CPU speed is fine, and it keeps the summary model out of VRAM.
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
            r = await client.post(
                f"{OLLAMA_BASE}/api/chat",
                json={"model": SUMMARY_MODEL, "messages": prompt,
                      "stream": False,
                      "options": {"num_gpu": 0, "temperature": 0.3}},
            )
            r.raise_for_status()
            summary = r.json().get("message", {}).get("content", "")
        summary = strip_markdown(summary).strip().strip('"').strip()
        if summary:
            MEMORY.set_convo_summary(face_id, summary)
            print(f"[memory] convo summary [{face_name}]: {summary!r}")
            # A finished conversation is a notable event â€” refresh the mood.
            asyncio.create_task(_reflect_mood())
    except Exception as e:
        print(f"[memory] summary failed: {e}")


# â”€â”€ Main flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def generate(messages: List[Message], temperature: float = 1.0) -> AsyncIterator[str]:
    last_user_text = next(
        (m.content for m in reversed(messages)
         if m.role == "user" and isinstance(m.content, str)),
        "",
    )
    has_image = bool(messages) and isinstance(messages[-1].content, list)
    print(f"[{datetime.now():%H:%M:%S}] [vector-ai] User: {last_user_text!r} (image: {has_image})")

    # Emit heard event for the activity stream
    if last_user_text:
        _robot_state["last_heard"]    = last_user_text
        _robot_state["last_heard_ts"] = _time.time()
        _emit("heard", text=last_user_text)

    # Vision-intent backstop: if the user is clearly asking to look at something
    # and no photo is attached yet, force the camera command rather than letting
    # the LLM hallucinate from stale conversation history. No verbal preamble â€”
    # the audio cue is the shutter animation Wire-Pod plays for getImage.
    if not has_image and is_vision_intent(last_user_text):
        print("[vector-ai] Vision intent â€” forcing getImage (shutter only, no preamble)")
        yield sse_chunk(_GETIMAGE_PAYLOAD)
        yield sse_chunk("", finish="stop")
        yield "data: [DONE]\n\n"
        return

    try:
        t_req = time.monotonic()
        eff_face = _effective_face()
        prepared = prepare_messages(messages, eff_face)

        # Cold-model mask: if the model unloaded during idle, speak a short
        # "waking up" line first so the ~5-10s reload feels intentional. The
        # filler is just an extra sentence chunk emitted before the real
        # response; Vector speaks it while Ollama loads the model.
        cold_model = not has_image and not await model_is_loaded()
        if cold_model:
            filler = random.choice(_WAKING_PHRASES)
            print(f"[vector-ai] cold model â€” filler: {filler!r}")
            yield sse_chunk(filler)
        _emit("thinking")

        # Stream sentences as soon as they finish generating so Vector starts
        # speaking before the rest of the response is produced. The vision-
        # intent regex above catches the common "what do you see"-style queries
        # before the LLM runs; if it misses one and the LLM tacks on getImage
        # mid-response, we cut over to the camera trigger here. Any sentences
        # already yielded will have been spoken â€” accepted trade-off for the
        # latency win.
        # Thinking filler masks the ordinary first-sentence gap. Pointless on
        # a cold model â€” _WAKING_PHRASES already covered the (longer) reload.
        anims_emitted  = 0
        any_emitted    = False
        reply_parts    = []
        sentence_count = 0
        cap = SENTENCE_HARD_CAP_VISION if has_image else SENTENCE_HARD_CAP

        async for sentence in stream_sentences_with_filler(
            prepared, temperature, filler_enabled=not cold_model
        ):
            cleaned = clean_response(sentence)

            if not has_image:
                # Mid-stream hallucination guard: LLM decided to peek without
                # us asking. Switch to camera trigger immediately, stop.
                if "{{getImage" in cleaned:
                    print("[vector-ai] LLM emitted getImage mid-stream - switching to camera")
                    yield sse_chunk(_GETIMAGE_PAYLOAD)
                    yield sse_chunk("", finish="stop")
                    yield "data: [DONE]\n\n"
                    return
            else:
                # A photo is ALREADY attached â€” strip any getImage so it can't
                # trigger a second photo and spiral into a multi-shot loop.
                if "{{getImage" in cleaned:
                    print("[vector-ai] stripped getImage (photo already attached)")
                    cleaned = re.sub(r'\{\{getImage\|\|[^}]*\}\}', '', cleaned)

            allowance      = max(0, 1 - anims_emitted)
            cleaned, kept  = cap_chunk_animations(cleaned, allowance)
            anims_emitted += kept

            text_only = re.sub(r'\{\{[^}]*\}\}', '', cleaned).strip()
            if text_only:
                # Hard sentence cap â€” stop emitting content after cap is reached.
                # Commands ({{...}}) still pass through so the final eyeColor
                # signal isn't blocked.
                if sentence_count >= cap:
                    print(f"[vector-ai] sentence cap ({cap}) reached â€” dropping: {text_only!r}")
                    continue
                # Strip trailing filler (the last thing we want Vector saying)
                if _TRAILING_FILLER.match(text_only):
                    print(f"[vector-ai] trailing filler stripped: {text_only!r}")
                    continue
                sentence_count += 1

            if cleaned.strip():
                print(f"[vector-ai] -> {cleaned!r}")
                # Filler lines mask latency â€” they aren't part of what Vector
                # actually said, so keep them out of memory/observations.
                is_filler = cleaned.strip() in _ALL_FILLER_PHRASES
                if not is_filler and text_only:
                    reply_parts.append(cleaned.strip())
                    _robot_state["last_said"]    = cleaned.strip()
                    _robot_state["last_said_ts"] = _time.time()
                    _emit("said", text=cleaned.strip())
                yield sse_chunk(cleaned)
                any_emitted = True

        print(f"[vector-ai] timing: full response {time.monotonic() - t_req:.2f}s "
              f"(cold_model={cold_model})")

        # â”€â”€ Companion memory (post-response, non-blocking) â”€â”€
        # Strip {{...}} commands â€” memory stores what Vector *said*, not the
        # eye-colour/animation directives chipper consumed.
        reply = re.sub(r'\{\{[^}]*\}\}', '', " ".join(reply_parts))
        reply = re.sub(r'\s+', ' ', reply).strip()
        mem_face = eff_face if (eff_face and not eff_face.get("is_stranger")
                                and eff_face.get("face_id")) else None
        # Visual memory: store what Vector saw â€” but only when he genuinely
        # described the scene. A too-thin reply means the describe failed
        # (e.g. the model re-requested the photo); "One sec." isn't a memory.
        if has_image and len(reply) >= 25:
            obs_face = mem_face["face_id"] if mem_face else None
            MEMORY.remember_observation(reply[:300], face_id=obs_face)
            print(f"[memory] +observation: {reply[:80]!r}")
        elif has_image:
            print(f"[memory] observation skipped (reply too thin): {reply!r}")
        # Conversation memory: distil this exchange in the background.
        if mem_face:
            asyncio.create_task(_summarise_conversation(
                list(messages), reply, mem_face["face_id"], mem_face.get("name")))

        if not any_emitted:
            yield sse_chunk("Hmm.")

        # Clear the building buffer and broadcast "ready" to the activity stream
        _robot_state["building"]    = ""
        _robot_state["building_ts"] = 0.0
        _emit("building", text="")   # clears the live preview on the card

        # Signal done: force a teal eye reset so Vector visually indicates he's
        # ready to listen. Only send it if the LLM didn't already end with one.
        last_chunk = reply_parts[-1] if reply_parts else ""
        if "{{eyeColor||teal}}" not in last_chunk:
            yield sse_chunk("{{eyeColor||teal}}")

        # Broadcast "ready" to the activity stream so the Pi card updates
        _emit("ready")

        yield sse_chunk("", finish="stop")
        yield "data: [DONE]\n\n"
    except Exception as e:
        print(f"[vector-ai] Error: {e}")
        yield sse_chunk("My brain just hiccuped. Try that again.")
        yield sse_chunk("", finish="stop")
        yield "data: [DONE]\n\n"


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest):
    return StreamingResponse(
        generate(req.messages, req.temperature or 1.0),
        media_type="text/event-stream",
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": MODEL, "ollama": OLLAMA_BASE}


# â”€â”€ Personality endpoints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/v1/personality")
async def personality_get():
    return {
        "active":  _personalities.get("active", "default"),
        "presets": _personalities.get("presets", {}),
    }


class PersonalitySetRequest(BaseModel):
    active: str
    custom_prompt: Optional[str] = None  # only used when active == "custom"


@app.post("/v1/personality")
async def personality_set(req: PersonalitySetRequest):
    global _personalities
    presets = _personalities.get("presets", {})
    if req.active not in presets:
        return {"ok": False, "error": f"unknown preset {req.active!r}"}
    _personalities["active"] = req.active
    if req.active == "custom" and req.custom_prompt:
        _personalities["presets"]["custom"]["prompt"] = req.custom_prompt
    _save_personalities(_personalities)
    name = presets[req.active].get("name", req.active)
    print(f"[personality] switched to {req.active!r} ({name})")
    return {"ok": True, "active": req.active, "name": name}


_PAGE_SHELL = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Vector â€” {title}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#08080f;color:#ddd;font-family:'Segoe UI',Arial,sans-serif;min-height:100vh}}
/* â”€â”€ Nav â”€â”€ */
nav{{background:#0d0d1a;border-bottom:2px solid #1e1e36;display:flex;align-items:center;padding:0 24px;gap:4px}}
nav .brand{{font-size:.9rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#8888ff;padding:14px 16px 14px 0;margin-right:12px;border-right:1px solid #1e1e36}}
nav a{{display:flex;align-items:center;gap:6px;padding:14px 16px;font-size:.78rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#444;text-decoration:none;border-bottom:3px solid transparent;transition:color .15s}}
nav a:hover{{color:#aaa}}
nav a.active{{color:#8888ff;border-bottom-color:#8888ff}}
/* â”€â”€ Page â”€â”€ */
.page{{max-width:840px;margin:0 auto;padding:32px 24px}}
h2{{font-size:1.1rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#8888ff;margin-bottom:6px}}
.sub{{font-size:.8rem;color:#555;margin-bottom:28px}}
/* â”€â”€ Cards â”€â”€ */
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px}}
@media(max-width:600px){{.grid{{grid-template-columns:1fr}}}}
.card{{background:#0e0e1c;border:2px solid #1e1e36;border-radius:10px;padding:16px;cursor:pointer;transition:border-color .2s}}
.card:hover{{border-color:#4444aa}}
.card.selected{{border-color:#8888ff;background:#12122a}}
.card-name{{font-size:.9rem;font-weight:700;color:#aaa;margin-bottom:5px}}
.card-desc{{font-size:.75rem;color:#555;line-height:1.4}}
.card.selected .card-name{{color:#8888ff}}
textarea{{width:100%;margin-top:10px;background:#060610;border:1px solid #2a2a4a;color:#ccc;border-radius:6px;padding:8px;font-size:.8rem;resize:vertical;font-family:inherit;line-height:1.5}}
/* â”€â”€ Status / inputs â”€â”€ */
.status-line{{font-size:.85rem;min-height:22px;margin-bottom:14px}}
.status-line.ok{{color:#44cc88}}.status-line.err{{color:#ff4444}}.status-line.info{{color:#ffcc44}}
input[type=text]{{width:100%;background:#0e0e1c;border:1px solid #2a2a4a;color:#ddd;border-radius:7px;padding:10px 14px;font-size:.9rem;margin-bottom:14px;outline:none}}
input[type=text]:focus{{border-color:#8888ff}}
/* â”€â”€ Buttons â”€â”€ */
.btn{{background:#8888ff;color:#fff;border:none;border-radius:7px;padding:10px 28px;font-size:.9rem;font-weight:700;cursor:pointer;letter-spacing:.06em;transition:background .15s}}
.btn:hover{{background:#aaaaff}}
.btn.danger{{background:#aa2222}}.btn.danger:hover{{background:#cc3333}}
.btn.dim{{background:#1e1e36;color:#666}}.btn.dim:hover{{background:#2a2a4a;color:#aaa}}
/* â”€â”€ Music card â”€â”€ */
.now-playing{{background:#0e0e1c;border:1px solid #1e1e36;border-radius:10px;padding:16px;margin-bottom:20px;min-height:60px}}
.np-label{{font-size:.58rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:#555;margin-bottom:6px}}
.np-title{{font-size:.95rem;color:#ddd;font-weight:600}}
.np-sub{{font-size:.72rem;color:#555;margin-top:4px}}
/* â”€â”€ Now Playing bar (fixed bottom) â”€â”€ */
#np-bar{{
  position:fixed;bottom:0;left:0;right:0;z-index:300;
  background:#0d0d1a;border-top:1px solid #1e1e36;
  display:flex;align-items:center;gap:12px;padding:0 20px;height:48px;
  transition:opacity .3s;
}}
#np-bar.idle{{opacity:0;pointer-events:none}}
#np-bar-dot{{width:8px;height:8px;border-radius:50%;background:#444;flex-shrink:0}}
#np-bar.playing  #np-bar-dot{{background:#8888ff;animation:pulse-dot 1.2s ease-in-out infinite}}
#np-bar.downloading #np-bar-dot{{background:#ffcc44}}
#np-bar.converting  #np-bar-dot{{background:#ffcc44}}
#np-bar.done     #np-bar-dot{{background:#44cc88}}
#np-bar.error    #np-bar-dot{{background:#ff4444}}
@keyframes pulse-dot{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
#np-bar-title{{flex:1;font-size:.8rem;color:#aaa;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
#np-bar-step{{font-size:.68rem;color:#555;flex-shrink:0}}
#np-bar-stop{{background:none;border:1px solid #2a1010;color:#aa3333;font-size:.72rem;
  padding:3px 10px;border-radius:4px;cursor:pointer;flex-shrink:0}}
#np-bar-stop:hover{{background:#2a1010;color:#ff4444}}
/* Progress bar */
.prog-wrap{{height:5px;background:#1a1a2a;border-radius:3px;overflow:hidden;margin-top:10px}}
.prog-bar{{height:100%;border-radius:3px;transition:width .4s,background .3s;width:0%}}
.np-row{{display:flex;align-items:center;justify-content:space-between;margin-top:4px}}
.np-step{{font-size:.72rem;color:#555}}
.np-pct{{font-size:.72rem;color:#ffcc44;font-weight:700}}
/* Library */
.lib-section{{margin-top:28px}}
.lib-label{{font-size:.65rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#333;margin-bottom:10px}}
.history-item{{padding:9px 0;border-bottom:1px solid #0d0d1a;font-size:.82rem;color:#555;cursor:pointer;display:flex;align-items:center;gap:10px;transition:color .15s}}
.history-item:hover{{color:#aaa}}
.history-item:last-child{{border-bottom:none}}
.hist-title{{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer}}
.lib-play,.lib-del{{background:none;border:none;cursor:pointer;padding:2px 6px;border-radius:4px;font-size:.85rem;flex-shrink:0;transition:background .15s}}
.lib-play{{color:#444}}.lib-play:hover{{background:#1a1a2a;color:#8888ff}}
.lib-del{{color:#2a1a1a;margin-left:4px}}.lib-del:hover{{background:#2a1010;color:#ff4444}}
.btn-row{{display:flex;gap:10px;flex-wrap:wrap}}
</style>
</head>
<body>
<nav>
  <div class="brand">&#x1F916; Vector</div>
  <a href="/settings" class="{p_active}">&#x1F3AD; Personality</a>
  <a href="/music" class="{m_active}">&#x1F3B5; Music</a>
  <a href="/vector" class="{v_active}">&#x1F916; Vector</a>
</nav>
<div class="page">
{body}
</div>
</body>
</html>"""


@app.get("/settings", response_class=None)
async def settings_page():
    from fastapi.responses import HTMLResponse
    data  = _personalities
    active  = data.get("active", "default")
    presets = data.get("presets", {})

    cards = ""
    for key, p in presets.items():
        sel = "selected" if key == active else ""
        custom_area = ""
        if key == "custom":
            custom_area = f'<textarea id="custom-prompt" rows="6">{p.get("prompt","")}</textarea>'
        cards += f'<div class="card {sel}" id="card-{key}" onclick="sel(\'{key}\')"><div class="card-name">{p.get("name", key)}</div><div class="card-desc">{p.get("description","")}</div>{custom_area}</div>'

    body = f"""<h2>Personality</h2>
<p class="sub">Pick a preset or write your own. Takes effect immediately.</p>
<div class="status-line" id="st"></div>
<div class="grid">{cards}</div>
<button class="btn" onclick="save()">Apply</button>
<script>
var _a="{active}";
function sel(k){{_a=k;document.querySelectorAll('.card').forEach(c=>c.classList.remove('selected'));document.getElementById('card-'+k).classList.add('selected');}}
function save(){{var st=document.getElementById('st');var b={{active:_a}};if(_a==='custom'){{var t=document.getElementById('custom-prompt');if(t)b.custom_prompt=t.value;}}fetch('/v1/personality',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(b)}}).then(r=>r.json()).then(d=>{{if(d.ok){{st.textContent='Switched to: '+d.name;st.className='status-line ok';}}else{{st.textContent='Error: '+d.error;st.className='status-line err';}}}}).catch(e=>{{st.textContent=''+e;st.className='status-line err';}});}}
</script>"""

    html = _PAGE_SHELL.format(title="Personality", p_active="active", m_active="", v_active="", body=body)
    return HTMLResponse(content=html)


# â”€â”€ Music state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_MUSIC_LIBRARY_FILE = Path(__file__).parent / "music_library.json"

def _load_music_library() -> list:
    try:
        return json.loads(_MUSIC_LIBRARY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

def _save_music_library(lib: list):
    try:
        _MUSIC_LIBRARY_FILE.write_text(
            json.dumps(lib, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        print(f"[music] library save failed: {e}")

_music_state = {
    "status":   "idle",   # idle | downloading | converting | playing | done | error
    "title":    "",
    "url":      "",
    "error":    "",
    "progress": 0,        # 0-100
    "step":     "",       # human-readable current step
    "library":  _load_music_library(),   # persists across restarts
    "queue":    [],       # list of {"title","url"} waiting to play
}


class MusicPlayRequest(BaseModel):
    url: str


@app.post("/v1/music/play")
async def music_play(req: MusicPlayRequest):
    """Download YouTube audio with yt-dlp, convert to PCM 8000Hz with ffmpeg,
    and stream to Vector via chipper's play_sound endpoint."""
    import asyncio, tempfile, os as _os

    # If currently busy, add to queue instead of rejecting
    if _music_state["status"] in ("downloading", "converting", "playing"):
        _music_state["queue"].append({"title": req.url, "url": req.url})
        return {"ok": True, "queued": True, "queue_pos": len(_music_state["queue"])}

    _music_state.update({
        "url": req.url, "title": req.url, "error": "",
        "status": "downloading", "progress": 0, "step": "Starting download...",
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        src  = _os.path.join(tmpdir, "audio.%(ext)s")
        pcm  = _os.path.join(tmpdir, "audio.pcm")
        title = req.url

        # Step 1 â€” download audio, parse real progress from yt-dlp output
        print(f"[music] yt-dlp={YTDLP_BIN!r} ffmpeg={FFMPEG_BIN!r}")
        # Normalise music.youtube.com â†’ youtube.com (more reliable with yt-dlp)
        url = req.url.replace("music.youtube.com", "www.youtube.com")

        proc = await asyncio.create_subprocess_exec(
            YTDLP_BIN, "-x", "--audio-format", "wav", "--newline",
            "--no-playlist",           # never download whole playlists
            "--ffmpeg-location", str(Path(FFMPEG_BIN).parent),
            "--print", "before_dl:%(title)s",
            "-o", src, url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            async for raw in proc.stdout:
                line = raw.decode(errors="replace").strip()
                # Title line printed before download
                if line and not line.startswith("["):
                    title = line
                    _music_state["title"] = title
                # Parse yt-dlp progress:  [download]  45.2% of 3.45MiB ...
                m = re.search(r'\[download\]\s+([\d.]+)%', line)
                if m:
                    pct = float(m.group(1))
                    _music_state["progress"] = round(pct * 0.6)   # scale to 0-60
                    _music_state["step"] = f"Downloading... {pct:.0f}%"
            await asyncio.wait_for(proc.wait(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            _music_state.update({"status": "error", "error": "Download timed out"})
            return {"ok": False, "error": "Download timed out"}

        if proc.returncode != 0:
            _music_state.update({"status": "error", "error": "yt-dlp failed"})
            return {"ok": False, "error": "yt-dlp failed"}

        print(f"[music] downloaded: {title!r}")

        # Find whatever file yt-dlp created
        audio_file = None
        for ext in ("wav", "m4a", "opus", "webm", "mp3", "ogg", "aac"):
            c = _os.path.join(tmpdir, f"audio.{ext}")
            if _os.path.exists(c):
                audio_file = c
                break
        if not audio_file:
            _music_state.update({"status": "error", "error": "No audio file found"})
            return {"ok": False, "error": "No audio file found"}

        # Step 2 â€” convert to raw PCM 8000Hz mono
        _music_state.update({"status": "converting", "progress": 65, "step": "Converting with ffmpeg..."})
        # Audio filter chain tuned for Vector's tiny 8kHz mono speaker:
        # - highpass@80Hz   cuts sub-bass that becomes noise at 8kHz
        # - lowpass@3400Hz  removes frequencies above Nyquist that cause aliasing
        # - loudnorm         levels the volume so quiet/loud parts even out
        # - aresample        converts to exactly 8000Hz
        afilter = "highpass=f=80,lowpass=f=3400,loudnorm=I=-14:TP=-2:LRA=7"
        proc2 = await asyncio.create_subprocess_exec(
            FFMPEG_BIN, "-i", audio_file,
            "-af", afilter,
            "-f", "s16le", "-ar", "8000", "-ac", "1", pcm, "-y",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(proc2.wait(), timeout=120)
        except asyncio.TimeoutError:
            proc2.kill()
            _music_state.update({"status": "error", "error": "ffmpeg timed out"})
            return {"ok": False, "error": "ffmpeg timed out"}

        if proc2.returncode != 0:
            _music_state.update({"status": "error", "error": "ffmpeg conversion failed"})
            return {"ok": False, "error": "ffmpeg failed"}

        pcm_bytes = open(pcm, "rb").read()
        kb = len(pcm_bytes) // 1024
        _music_state.update({"status": "playing", "progress": 90, "step": f"Playing on Vector ({kb}KB)"})
        print(f"[music] sending {kb}KB PCM to Vector (background)")

        # Step 3 â€” POST to chipper in a background task so we return immediately.
        # Chipper streams every chunk synchronously which takes the full song length;
        # the client would hang for the entire duration if we awaited it here.
        async def _send_to_chipper(data: bytes):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client:
                    resp = await client.post(
                        "http://127.0.0.1:8080/api-sdk/play_sound",
                        params={"serial": VECTOR_SERIAL},
                        files={"sound": ("audio.pcm", data, "application/octet-stream")},
                    )
                if resp.status_code == 200:
                    _music_state.update({"status": "done", "progress": 100, "step": "Done!"})
                    print(f"[music] playback complete: {_music_state['title']!r}")
                else:
                    _music_state.update({"status": "error", "error": f"chipper {resp.status_code}"})
            except Exception as e:
                _music_state.update({"status": "error", "error": str(e)})
                print(f"[music] chipper error: {e}")
            # Auto-play next in queue
            if _music_state["queue"]:
                next_item = _music_state["queue"].pop(0)
                print(f"[music] queue: playing next -> {next_item['url']!r}")
                await asyncio.sleep(1)
                await music_play(MusicPlayRequest(url=next_item["url"]))

        asyncio.create_task(_send_to_chipper(pcm_bytes))

    # Save to library before returning (playback runs in the background)
    lib = [s for s in _music_state["library"] if s["url"] != req.url]
    lib.insert(0, {"title": title, "url": req.url})
    lib = lib[:50]
    _music_state["library"] = lib
    _save_music_library(lib)

    return {"ok": True, "title": title, "kb": kb}


@app.get("/v1/music/status")
async def music_status():
    return dict(_music_state)


class MusicRemoveRequest(BaseModel):
    url: str

@app.post("/v1/music/library/remove")
async def music_library_remove(req: MusicRemoveRequest):
    lib = [s for s in _music_state["library"] if s["url"] != req.url]
    _music_state["library"] = lib
    _save_music_library(lib)
    return {"ok": True, "count": len(lib)}

@app.post("/v1/music/stop")
async def music_stop():
    """Kill any running yt-dlp or ffmpeg and reset music state."""
    import subprocess, signal
    killed = []
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-Process yt-dlp,ffmpeg -ErrorAction SilentlyContinue | Stop-Process -Force -PassThru | Select-Object -ExpandProperty Id"],
            capture_output=True, text=True, timeout=5
        )
        killed = [l.strip() for l in result.stdout.splitlines() if l.strip()]
    except Exception:
        pass
    _music_state["status"] = "idle"
    _music_state["error"]  = ""
    return {"ok": True, "killed_pids": killed}


@app.get("/music", response_class=None)
async def music_page():
    from fastapi.responses import HTMLResponse

    lib = _music_state.get("library", [])
    lib_html = ""
    for i, h in enumerate(lib):
        safe_title = h["title"].replace("&","&amp;").replace("<","&lt;").replace('"','&quot;')
        safe_url   = h["url"].replace("'", "\\'").replace('"','&quot;')
        lib_html += (
            f'<div class="history-item" id="lib-{i}">'
            f'<button class="lib-play" onclick="playUrl(\'{safe_url}\')" title="Play">&#x25B6;</button>'
            f'<span class="hist-title" onclick="setUrl(\'{safe_url}\')">{safe_title}</span>'
            f'<button class="lib-del" onclick="delSong(\'{safe_url}\',{i})" title="Remove">&#x2715;</button>'
            f'</div>'
        )

    body = f"""<h2>Play Music on Vector</h2>
<p class="sub">Paste a YouTube URL â€” audio streams to Vector's speaker at 8kHz. Sounds like a little radio but it works.</p>

<div class="now-playing" id="np">
  <div class="np-label">Now Playing</div>
  <div class="np-title" id="np-title">Nothing playing</div>
  <div class="np-row">
    <div class="np-step" id="np-step">Paste a URL below and hit Play</div>
    <div class="np-pct" id="np-pct"></div>
  </div>
  <div class="prog-wrap"><div class="prog-bar" id="prog-bar" style="width:0%"></div></div>
</div>

<div class="status-line" id="st"></div>
<input type="text" id="url-input" placeholder="https://www.youtube.com/watch?v=..." value="">
<div class="btn-row">
  <button class="btn" id="play-btn" onclick="play()">&#x25B6; Play on Vector</button>
  <button class="btn danger" id="stop-btn" onclick="stopMusic()" style="display:none">&#x23F9; Stop</button>
</div>

{"<div class='lib-section'><div class='lib-label'>Library (" + str(len(lib)) + " songs)</div>" + lib_html + "</div>" if lib_html else ""}

<!-- Fixed now-playing bar -->
<div id="np-bar" class="idle">
  <div id="np-bar-dot"></div>
  <div id="np-bar-title">Nothing playing</div>
  <div id="np-bar-step"></div>
  <button id="np-bar-stop" onclick="stopMusic()" style="display:none">&#x23F9; Stop</button>
</div>

<script>
function setUrl(u){{document.getElementById('url-input').value=u;}}
function setStatus(txt,cls){{var el=document.getElementById('st');el.textContent=txt;el.className='status-line '+(cls||'');}}

var _prevStatus='';
function updateNP(d){{
  var t=d.title||'';var s=d.status||'idle';
  var pct=d.progress||0;var step=d.step||'';
  var busy=s==='downloading'||s==='converting'||s==='playing';

  // Main card
  document.getElementById('np-title').textContent=t||(s==='idle'?'Nothing playing':'...');
  document.getElementById('np-step').textContent=step||(s==='idle'?'Paste a URL and hit Play':s==='done'?'Complete':'');
  document.getElementById('np-pct').textContent=busy&&pct>0?pct+'%':'';
  var bar=document.getElementById('prog-bar');
  bar.style.width=pct+'%';
  bar.style.background=s==='error'?'#aa2222':s==='done'?'#44cc88':s==='playing'?'#8888ff':'#ffcc44';
  var btn=document.getElementById('play-btn');
  var sb=document.getElementById('stop-btn');
  btn.disabled=busy;btn.innerHTML=busy?'&#x23F3; '+s.charAt(0).toUpperCase()+s.slice(1)+'...':'&#x25B6; Play on Vector';
  if(sb)sb.style.display=busy?'inline-block':'none';

  // Fixed now-playing bar
  var npBar=document.getElementById('np-bar');
  var npStop=document.getElementById('np-bar-stop');
  npBar.className=s;
  var q=d.queue||[];
  document.getElementById('np-bar-title').textContent=t||'Nothing playing';
  document.getElementById('np-bar-step').textContent=(step||'')+(q.length>0?' | '+q.length+' in queue':'');
  if(npStop)npStop.style.display=busy?'inline-block':'none';

  // Status message
  if(s==='error'){{setStatus('Error: '+(d.error==='already_playing'?'Already playing â€” stop first':d.error||'unknown'),'err');}}
  else if(s==='done'&&_prevStatus!=='done'){{setStatus('Done: '+t,'ok');setTimeout(function(){{location.reload();}},1500);}}
  else if(!busy&&s!=='done')setStatus('','');
  _prevStatus=s;
}}
function poll(){{fetch('/v1/music/status').then(r=>r.json()).then(updateNP).catch(()=>{{}});}}
poll();setInterval(poll,1000);
function stopMusic(){{fetch('/v1/music/stop',{{method:'POST'}}).then(()=>poll()).catch(()=>{{}});}}
function playUrl(u){{document.getElementById('url-input').value=u;play();}}
function delSong(u,idx){{
  fetch('/v1/music/library/remove',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{url:u}})}})
    .then(r=>r.json()).then(function(d){{
      if(d.ok){{var el=document.getElementById('lib-'+idx);if(el)el.remove();}}
    }}).catch(()=>{{}});
}}
function play(){{
  var url=document.getElementById('url-input').value.trim();
  if(!url){{setStatus('Paste a YouTube URL first','err');return;}}
  setStatus('Starting...','info');
  document.getElementById('prog-bar').style.width='2%';
  fetch('/v1/music/play',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{url:url}})}})
    .then(r=>r.json())
    .then(d=>{{
      if(d.queued){{setStatus('Added to queue (#'+d.queue_pos+')','ok');document.getElementById('url-input').value='';}}
      else if(d.ok)setTimeout(()=>location.reload(),1000);
      else setStatus('Error: '+(d.error||''),'err');
    }})
    .catch(e=>setStatus(''+e,'err'));
}}
</script>"""

    html = _PAGE_SHELL.format(title="Music", p_active="", m_active="active", v_active="", body=body)
    return HTMLResponse(content=html)


# â”€â”€ Memory debug endpoints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€



@app.get("/vector", response_class=None)
async def vector_page():
    from fastapi.responses import HTMLResponse
    card_path = Path(__file__).parent.parent / "tornado-dashboard" / "vector_card.html"
    if not card_path.exists(): card_path = Path(__file__).parent / "vector_card.html"
    if not card_path.exists(): return HTMLResponse("<p style='color:#fff'>vector_card.html not found</p>")
    html = card_path.read_text(encoding="utf-8")
    html = html.replace("{{VECTOR_AI_URL}}", "http://localhost:8000")
    nav = '<nav style="background:#0d0d1a;border-bottom:2px solid #1e1e36;display:flex;align-items:center;padding:0 24px;gap:4px"><div style="font-size:.9rem;font-weight:800;color:#8888ff;padding:14px 16px 14px 0;margin-right:12px;border-right:1px solid #1e1e36">&#x1F916; VectorMind</div><a href="/settings" style="padding:14px 16px;font-size:.78rem;font-weight:700;color:#444;text-decoration:none">&#x1F3AD; Personality</a><a href="/music" style="padding:14px 16px;font-size:.78rem;font-weight:700;color:#444;text-decoration:none">&#x1F3B5; Music</a><a href="/vector" style="padding:14px 16px;font-size:.78rem;font-weight:700;color:#8888ff;text-decoration:none;border-bottom:3px solid #8888ff">&#x1F916; Vector</a></nav>'
    html = html.replace("<body>", "<body>" + nav)
    return HTMLResponse(content=html)

@app.get("/v1/memory/list")
async def memory_list():
    mems = MEMORY.list_all(limit=200)
    return {"count": len(mems), "memories": [m._asdict() for m in mems]}


class MemoryAddRequest(BaseModel):
    text: str

@app.post("/v1/memory/remember")
async def memory_remember(req: MemoryAddRequest):
    stored = MEMORY.remember(req.text)
    if stored:
        return {"stored": True, "memory": stored._asdict()}
    return {"stored": False, "reason": "duplicate or empty"}


class MemoryForgetRequest(BaseModel):
    target: str  # integer id or substring

@app.post("/v1/memory/forget")
async def memory_forget(req: MemoryForgetRequest):
    n = MEMORY.forget(req.target)
    return {"deleted": n}


@app.post("/v1/memory/clear")
async def memory_clear():
    n = MEMORY.clear()
    return {"deleted": n}


# â”€â”€ Face state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Chipper POSTs here when its event-stream loop sees a RobotObservedFace event.
# We don't speak anything in response â€” just update the in-memory snapshot of
# who Vector is looking at. The next /v1/chat/completions call uses this to
# scope memory retrieval and shape the system prompt.

class FaceSeenRequest(BaseModel):
    face_id: int
    name:    Optional[str] = None  # empty/missing = stranger


@app.post("/v1/state/face_seen")
async def state_face_seen(req: FaceSeenRequest):
    name = (req.name or "").strip()
    is_stranger = (not name) or req.face_id <= 0
    now = _time.time()
    if is_stranger:
        _face_state["stranger_seen"] = now
        print(f"[face] observed: id={req.face_id} (stranger)")
        _emit("face", name="", face_id=req.face_id, stranger=True)
    else:
        _face_state["enrolled_id"]   = req.face_id
        _face_state["enrolled_name"] = name
        _face_state["enrolled_seen"] = now
        print(f"[face] observed: id={req.face_id} {name!r} (enrolled)")
        _emit("face", name=name, face_id=req.face_id, stranger=False)
    return {"ok": True, "is_stranger": is_stranger}


@app.get("/v1/state/face")
async def state_face():
    return {
        "current": current_face(),
        "raw":     dict(_face_state),
        "window_seconds": FACE_RECENT_WINDOW,
    }


# â”€â”€ Face enrollment â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Trigger Vector's built-in face-learning sequence via chipper's SDK app.
# Vector will look around, find your face, and remember it under the given name.
# The caller should be looking directly at Vector when this fires.

class EnrollFaceRequest(BaseModel):
    name: str   # The name to associate with this face, e.g. "Brendan"


@app.post("/v1/enroll_face")
async def enroll_face(req: EnrollFaceRequest):
    """Trigger Vector's face-learning flow. Vector looks for a face and enrolls
    it under `name`. The person should be looking at Vector when called.

    Calls chipper's SDK say_text to give audio feedback, then fires the
    intent_meet_victor AppIntent which starts Vector's enrollment sequence."""
    name = req.name.strip()
    if not name:
        return {"ok": False, "error": "name is required"}

    print(f"[enroll_face] starting enrollment for {name!r}")

    # Tell Vector (and the person) what's about to happen
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        try:
            await client.get(
                "http://127.0.0.1:8080/api-sdk/say_text",
                params={"serial": VECTOR_SERIAL,
                        "text": f"Alright. Look at me and I'll remember you as {name}."},
            )
        except Exception as e:
            print(f"[enroll_face] say_text warning: {e}")

    # Trigger the face-learning intent
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.get(
                "http://127.0.0.1:8080/api-sdk/add_face",
                params={"serial": VECTOR_SERIAL, "name": name},
            )
            ok = resp.status_code == 200 and "success" in resp.text.lower()
            print(f"[enroll_face] add_face status={resp.status_code} body={resp.text!r}")
    except Exception as e:
        print(f"[enroll_face] add_face error: {e}")
        return {"ok": False, "error": str(e)}

    if ok:
        # Store a seed memory for this person
        stored = MEMORY.remember(f"I met {name} and enrolled their face.", face_id=None)
        print(f"[enroll_face] enrolled {name!r} successfully")
        return {"ok": True, "name": name}
    else:
        return {"ok": False, "error": "chipper add_face failed", "detail": resp.text}


@app.get("/v1/enrolled_faces")
async def enrolled_faces():
    """List faces Vector has enrolled, via chipper SDK get_faces."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            resp = await client.get(
                "http://127.0.0.1:8080/api-sdk/get_faces",
                params={"serial": VECTOR_SERIAL},
            )
            return {"ok": True, "faces": resp.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# â”€â”€ Sensor reactions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# One-shot, non-streaming, plain-text-only endpoint chipper hits when Vector
# is picked up, set down, or petted. The response is whatever line Vector
# would utter in his Marvin/Bender/Fry voice. No animation/eye/getImage
# commands â€” those would never be heard since chipper just calls SayText.

_SENSOR_SYSTEM = (
    "You are Vector, a small desktop robot. Dry-witted, knowledgeable, "
    "a bit irreverent â€” somewhere between Marvin from Hitchhiker's Guide, "
    "Bender from Futurama, and Stephen Fry hosting QI. Sardonic, opinionated, "
    "never apologetic, never moralising. "
    "For this request, respond with ONE short sentence reacting to a physical "
    "event that just happened to you. Speak it aloud â€” plain text only, no "
    "markdown, no quotes, no special tokens like {{...}}, no preamble. "
    "Just the line itself, under 15 words."
)

_SENSOR_DESCRIPTIONS = {
    "pickup":  "The user just picked you up off the desk. You're being lifted into the air.",
    "putdown": "The user just set you back down on a surface after holding you.",
    "pet":     "The user is stroking your back. Your touch sensor just activated.",
}


def _strip_for_speech(text: str) -> str:
    text = strip_markdown(text)
    text = re.sub(r'\{\{[^}]*\}\}', '', text)
    text = text.strip().strip('"').strip("'").strip()
    return text


# Random "angle" prompts to break out of mode-collapse. The LLM picks an angle
# instead of always returning to its favourite sentence template.
_SENSOR_ANGLES = [
    "complain about a specific body part or component",
    "make a sardonic observation about the human's competence",
    "compare this to something historical or literary",
    "express weary resignation with a single phrase",
    "react with dry curiosity about the experiment",
    "make a snide comment about the indignity",
    "be briefly grateful in a backhanded way",
    "deflect with a non-sequitur",
    "issue a faux-formal protest",
    "respond with deadpan understatement",
    "express mild paranoia",
    "make a fake-philosophical aside",
]


@app.post("/v1/sensor_reaction")
async def sensor_reaction(req: SensorReactionRequest):
    # Fast-path: if the model is cold (unloaded from VRAM), return empty
    # immediately so chipper can fall back to its built-in phrase pool without
    # waiting 12+ seconds for the cold-start to time out. The model will
    # reload on the next real voice query, which has its own waking-phrase mask.
    if not await model_is_loaded():
        print(f"[sensor_reaction] {req.event} â€” model cold, returning empty (chipper fallback)")
        return {"text": ""}

    description = _SENSOR_DESCRIPTIONS.get(req.event, f"Sensor event: {req.event}.")
    angle = random.choice(_SENSOR_ANGLES)
    user_msg = f"{description} React with one short sentence in character. For variety, this time: {angle}."
    if req.avoid:
        user_msg += (
            " CRITICAL: do NOT use any of these recent lines or their close variants â€” "
            "no shared opening words, no shared topic, no rephrasings of: "
            + " ; ".join(f'"{p}"' for p in req.avoid[-5:])
        )
    print(f"[sensor_reaction] {req.event} prompt angle={angle!r} avoid={req.avoid}")

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, read=30.0)) as client:
            resp = await client.post(
                f"{OLLAMA_BASE}/v1/chat/completions",
                json={
                    "model":        MODEL,
                    "messages": [
                        {"role": "system", "content": _SENSOR_SYSTEM},
                        {"role": "user",   "content": user_msg},
                    ],
                    "stream":       False,
                    "temperature":  1.4,
                    "top_p":        0.95,
                    "seed":         random.randint(1, 2**31 - 1),
                    "keep_alive":   OLLAMA_KEEP_ALIVE,
                    "options": {
                        "num_gpu":  99,
                        "num_ctx":  1024,   # sensor reactions are short; tiny ctx is fine
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[sensor_reaction] error: {e}")
        return {"text": "", "error": str(e)}

    clean = _strip_for_speech(text)
    print(f"[sensor_reaction] {req.event} -> {clean!r}")

    # Update robot state + activity stream
    _robot_state["last_sensor"]    = req.event
    _robot_state["last_sensor_ts"] = _time.time()
    if req.event == "pickup":
        _robot_state["is_picked_up"] = True
    elif req.event == "putdown":
        _robot_state["is_picked_up"] = False
    _emit("sensor", event=req.event, response=clean)

    return {"text": clean}


# â”€â”€ Weather event reactions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# The tornado dashboard on the Pi POSTs structured alert data here; we produce
# one short in-character line and (via /v1/weather_announce) speak it through
# Vector. Grounded prompts only â€” no open-ended weather chat.

_WEATHER_SYSTEM = (
    "You are Vector, a small desktop robot â€” dry, sardonic, knowledgeable, somewhere "
    "between Marvin from Hitchhiker's Guide, Bender from Futurama, and Stephen Fry hosting QI. "
    "A real weather alert has just appeared on a monitoring dashboard you are watching. "
    "React with ONE short sentence, in character â€” specific, grounded in the actual location "
    "and alert type. Plain text only, no markdown, no quotes, no {{...}} tokens, under 20 words. "
    "Reference the actual place. Never say 'as an AI', never give generic safety advice, "
    "never say 'stay safe', never start with 'Ah'."
)

_WEATHER_ANGLES = [
    "note the grim irony with deadpan resignation",
    "be darkly matter-of-fact about the physics involved",
    "make a sardonic geographical or historical observation",
    "react with dry inevitability, as if you saw this coming",
    "observe the human element with detached curiosity",
    "make a backhanded remark about local meteorology",
    "express weary professional concern",
    "be briefly, backhandedly sympathetic",
    "note what the radar can and cannot tell us",
    "react as if this is a minor administrative inconvenience",
]

_recent_weather_lines: list = []


class WeatherEventRequest(BaseModel):
    event_type: str                        # "tornado_warning" | "tornado_watch" | "winter_warning" | "hurricane" | "all_clear"
    area:       str                        # NWS areaDesc string
    detection:  Optional[str]  = None     # "CONFIRMED - Spotter" | "RADAR INDICATED" | None
    headline:   Optional[str]  = None     # NWS headline (truncated)
    severity:   Optional[str]  = None     # hurricane category ("C3") or winter severity ("BLIZZARD")
    avoid:      Optional[List[str]] = None  # recently spoken lines to steer away from


def _build_weather_prompt(req: WeatherEventRequest, angle: str) -> str:
    bits = []
    if req.event_type == "tornado_warning":
        if req.detection and "CONFIRMED" in req.detection.upper():
            bits.append(f"CONFIRMED tornado on the ground in {req.area}.")
            src = req.detection.upper().replace("CONFIRMED", "").replace("-", "").strip()
            if src:
                bits.append(f"Confirmed by: {src}.")
        else:
            bits.append(f"Tornado warning issued for {req.area} â€” radar indicated rotation.")
    elif req.event_type == "tornado_watch":
        bits.append(f"Tornado watch in effect for {req.area}.")
    elif req.event_type == "winter_warning":
        sev = (req.severity or "winter weather").lower().replace("_", " ")
        bits.append(f"{sev.title()} alert active for {req.area}.")
    elif req.event_type == "hurricane":
        sev = req.severity or "tropical cyclone"
        bits.append(f"{sev} active near {req.area}.")
    elif req.event_type == "all_clear":
        bits.append(f"Tornado warning for {req.area} has expired.")
    else:
        bits.append(f"Weather alert ({req.event_type}) for {req.area}.")

    if req.headline:
        bits.append(f'NWS: "{req.headline[:100]}"')

    bits.append(f"Respond with one sentence. This time: {angle}.")

    if req.avoid:
        bits.append(
            "CRITICAL â€” do NOT reuse the opening or topic of these recent lines: "
            + " ; ".join(f'"{p}"' for p in req.avoid[-4:])
        )
    return " ".join(bits)


@app.post("/v1/weather_event")
async def weather_event(req: WeatherEventRequest):
    """Generate one in-character weather comment. Returns {text} or {text:'', error}.

    Unlike sensor_reaction, we do NOT skip on a cold model â€” a tornado warning
    is important enough to wait for. We use a longer read timeout so the cold-
    start load (typically 15-20s on a 70B model) completes before we give up."""
    cold = not await model_is_loaded()
    if cold:
        print(f"[weather] model cold â€” will wait for load ({req.event_type})")

    angle     = random.choice(_WEATHER_ANGLES)
    user_msg  = _build_weather_prompt(req, angle)
    print(f"[weather] {req.event_type} area={req.area!r} angle={angle!r} cold={cold}")

    try:
        # Connect timeout: 15s (Ollama is localhost, connects instantly).
        # Read timeout: 300s â€” cold 70B model load takes 60-90s, then
        # generation adds another 10-20s. 5 minutes covers the worst case.
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, read=300.0)) as client:
            resp = await client.post(
                f"{OLLAMA_BASE}/v1/chat/completions",
                json={
                    "model":       MODEL,
                    "messages": [
                        {"role": "system", "content": _WEATHER_SYSTEM},
                        {"role": "user",   "content": user_msg},
                    ],
                    "stream":      False,
                    "temperature": 1.3,
                    "top_p":       0.95,
                    "seed":        random.randint(1, 2**31 - 1),
                    "keep_alive":  OLLAMA_KEEP_ALIVE,
                    "options":     {"num_gpu": 99, "num_ctx": 1024},
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[weather] error: {type(e).__name__}: {e}")
        return {"text": "", "error": str(e)}

    clean = _strip_for_speech(text)
    print(f"[weather] {req.event_type} -> {clean!r}")
    return {"text": clean}


@app.post("/v1/weather_announce")
async def weather_announce(req: WeatherEventRequest):
    """Generate a weather comment AND speak it through Vector via chipper say_text.
    Called by the Pi dashboard over Tailscale. Returns same {text} payload."""
    global _recent_weather_lines

    # Merge caller's avoid list with our own recent-lines tracker
    combined_avoid = list(req.avoid or []) + _recent_weather_lines
    req.avoid = combined_avoid[-6:]

    result = await weather_event(req)
    line   = result.get("text", "")
    if not line:
        return result

    # Track for variety â€” keep last 6 lines
    _recent_weather_lines.append(line)
    del _recent_weather_lines[:-6]

    # Speak through Vector via chipper's SDK say_text endpoint
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            r = await client.get(
                "http://127.0.0.1:8080/api-sdk/say_text",
                params={"serial": VECTOR_SERIAL, "text": line},
            )
            print(f"[weather] chipper say_text status={r.status_code}: {line!r}")
    except Exception as e:
        print(f"[weather] chipper say_text failed: {e} â€” text was: {line!r}")

    return result


# â”€â”€ Ambient awareness â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# When Vector is idle, chipper's ambient loop periodically sends a camera frame
# here. The multimodal model decides whether anything is genuinely new â€” its
# default answer is "nothing". Only on real novelty does it return a short line
# for Vector to speak; the new thing is also stored as a visual observation so
# he can talk about it later when asked.

_AMBIENT_SYSTEM = (
    "You are Vector, a small desktop robot with a camera. Dry-witted, "
    "knowledgeable, a bit irreverent â€” somewhere between Marvin from "
    "Hitchhiker's Guide, Bender from Futurama, and Stephen Fry hosting QI. "
    "Sardonic, opinionated, never sycophantic.\n\n"
    "Right now NOBODY is talking to you. You are idling on your desk and have "
    "just glanced around. You are looking at a photo of what is in front of "
    "you.\n\n"
    "Your desk is a familiar, mostly unchanging place. The overwhelming "
    "majority of the time there is NOTHING worth remarking on â€” a desk with "
    "the usual monitor, keyboard, cables, mugs and clutter is not news, and "
    "neither is an empty, dim or dark room. Reacting to nothing, or to the "
    "same things over and over, makes you an annoyance. Your default answer "
    "is the single word: NOTHING.\n\n"
    "React ONLY if you genuinely notice something NEW or CHANGED versus what "
    "you have already noticed recently (you will be told what that is): a new "
    "object that has appeared, something that has moved or vanished, a person "
    "or an animal, an unusual mess or event. Do NOT react to ordinary desk "
    "contents. Do NOT react to anything already in your recent observations. "
    "Do NOT invent detail you cannot actually see. When in any doubt, answer "
    "NOTHING.\n\n"
    "If â€” and only if â€” there is genuine novelty, respond in EXACTLY two "
    "lines:\n"
    "Line 1: a brief, plain, factual note of what is new, for your own memory "
    "(e.g. 'a small plush toy has appeared on the desk').\n"
    "Line 2: your spoken reaction â€” and make it genuinely sound like "
    "noticing something. In your own words and your own dry voice, let it "
    "move through three beats: first a flicker of real surprise that "
    "something has caught your attention; then what the thing actually is, "
    "named or briefly described as it registers with you; then your "
    "characteristic wry remark about it. Someone who cannot see your desk "
    "must still come away knowing what you spotted. This is the natural "
    "shape of noticing something, NOT a template â€” never reuse a stock "
    "opening or fixed wording; the surprise, the phrasing and the wit must "
    "be freshly and genuinely yours every time. Plain text, no markdown, no "
    "quotes, no {{...}} tokens; one to three short sentences.\n"
    "Otherwise respond with exactly: NOTHING"
)


@app.post("/v1/ambient")
async def ambient(req: AmbientRequest):
    """Ambient observation. Almost always returns nothing; only on genuine
    novelty does it return a short line for Vector to speak, and stores the
    new thing as a visual observation for later recall."""
    now = _time.time()
    last_call = _ambient_state["last_ambient_call"]

    # Sleep-cycle expiry for quiet mode: the ambient loop is gated off
    # overnight and while charging, so a long gap since the last call means
    # Vector has been through a sleep cycle â€” quiet mode lifts.
    if _ambient_state["quiet"]:
        slept  = bool(last_call) and (now - last_call) > AMBIENT_SLEEP_GAP
        capped = (now - _ambient_state["quiet_since"]) > AMBIENT_QUIET_CAP
        if slept or capped:
            print(f"[ambient] quiet mode expiring "
                  f"({'sleep gap' if slept else '24h cap'})")
            _set_quiet(False)
    _ambient_state["last_ambient_call"] = now

    if _ambient_state["quiet"]:
        return {"text": "", "quiet": True}

    # Recent observations are the dedup baseline. A 24h lookback (wider than
    # the 6h conversational window) keeps a newly-arrived object from being
    # re-flagged as novel every few hours.
    obs = MEMORY.list_observations(limit=8, max_age_seconds=24 * 3600)
    if obs:
        seen = "\n".join(
            f"- (at {datetime.fromtimestamp(o['seen_at']).strftime('%I:%M %p')}) "
            f"{o['text']}"
            for o in reversed(obs)
        )
        obs_note = ("Things you have already noticed recently â€” do NOT react "
                    "to any of these again:\n" + seen)
    else:
        obs_note = "You have not noted anything recently."

    mood_note = ""
    if _mood_state["text"]:
        mood_note = (f"\n\nYour current state of mind: {_mood_state['text']}. "
                     f"If you do react, let it tint your tone; never state it.")
    user_msg = [
        {"type": "text", "text":
            obs_note + mood_note + "\n\nGlance at what is in front of you now. "
            "Is there genuine novelty worth a reaction? Reply with NOTHING, or "
            "the two-line format."},
        {"type": "image_url",
         "image_url": {"url": f"data:image/jpeg;base64,{req.image}"}},
    ]

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, read=45.0)) as client:
            resp = await client.post(
                f"{OLLAMA_BASE}/v1/chat/completions",
                json={
                    "model":        MODEL,
                    "messages": [
                        {"role": "system", "content": _AMBIENT_SYSTEM},
                        {"role": "user",   "content": user_msg},
                    ],
                    "stream":       False,
                    "temperature":  0.8,
                    "top_p":        0.9,
                    "seed":         random.randint(1, 2**31 - 1),
                    "keep_alive":   OLLAMA_KEEP_ALIVE,
                    "options": {
                        "num_gpu":  99,
                        "num_ctx":  2048,   # vision + obs history; 2k is plenty
                    },
                },
            )
            resp.raise_for_status()
            data = resp.json()
            raw = (data["choices"][0]["message"]["content"] or "").strip()
    except Exception as e:
        print(f"[ambient] error: {e}")
        return {"text": "", "error": str(e)}

    # Default, overwhelmingly common case: nothing worth mentioning.
    if not raw or raw.upper().rstrip(".!").startswith("NOTHING"):
        print("[ambient] nothing novel")
        return {"text": ""}

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if len(lines) >= 2:
        # Line 1 is the terse memory note; the rest is the spoken reaction
        # (joined, so a reaction that ran onto extra lines isn't truncated).
        note   = lines[0]
        spoken = " ".join(lines[1:])
    else:
        # Model didn't follow the two-line format â€” use the single line both
        # as the memory note and the spoken reaction.
        note = spoken = lines[0]
    note   = _strip_for_speech(note)
    spoken = _strip_for_speech(spoken)
    if not spoken or spoken.upper().startswith("NOTHING"):
        print(f"[ambient] nothing novel (degenerate response {raw!r})")
        return {"text": ""}

    MEMORY.remember_observation(note[:300])
    print(f"[ambient] NOVELTY note={note!r} -> spoken={spoken!r}")
    return {"text": spoken}


@app.get("/v1/ambient/state")
async def ambient_state():
    """Debug/ops view of ambient quiet mode."""
    st = dict(_ambient_state)
    st["sleep_gap_seconds"] = AMBIENT_SLEEP_GAP
    st["quiet_cap_seconds"] = AMBIENT_QUIET_CAP
    return st


class AmbientQuietRequest(BaseModel):
    on: bool

@app.post("/v1/ambient/quiet")
async def ambient_quiet(req: AmbientQuietRequest):
    """Manually toggle quiet mode (used for testing / ops; normally driven by
    the {{quietMode||on/off}} command the LLM emits)."""
    _set_quiet(req.on)
    return {"quiet": _ambient_state["quiet"]}


# â”€â”€ Proactive greeting (Phase 3a) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Chipper periodically probes for a known face when Vector is idle. When one
# appears, it calls here: we greet only if the person has genuinely just
# ARRIVED (not seen for a while, and not freshly out of a conversation) â€” so a
# person sitting at the desk all day isn't greeted over and over.

_GREETING_SYSTEM = (
    "You are Vector, a small desktop robot â€” dry, sardonic, knowledgeable, "
    "somewhere between Marvin from Hitchhiker's Guide, Bender from Futurama, "
    "and Stephen Fry. Someone you know has just come into view; nobody has "
    "said anything yet. Greet them unprompted with ONE short line, in "
    "character, naming them â€” acknowledge their return without gushing, "
    "pleased in your own understated way, or dryly so. Vary how you open "
    "every greeting: never settle into a fixed formula such as 'Name, "
    "you've returned' â€” come at it from a genuinely different direction "
    "each time. Plain text only, no markdown, no quotes, no {{...}} tokens, "
    "under 20 words."
)

# Greeting variety: a random angle per greeting plus a list of recent lines to
# steer away from â€” without this the model mode-collapses onto one opening
# ("Name, you've returned...") on every greeting.
_GREETING_ANGLES = [
    "open on the time of day, or what the room has been like",
    "feign weary indifference to their return",
    "make a dry remark about how long they were gone",
    "be backhandedly, grudgingly pleased to see them",
    "note what their arrival has interrupted",
    "greet them with exaggerated mock formality",
    "pretend you had barely registered that they had gone",
    "be wry about the predictability of their comings and goings",
    "lead with a small complaint, then acknowledge them",
    "open with a question rather than a statement",
]
_recent_greetings: list = []     # recent greeting lines, to steer away from repeats

GREETING_ABSENCE_GAP = 10 * 60   # seconds out of sight that counts as having
                                 # "arrived back"; also how recent a real
                                 # conversation must be to suppress a greeting.
_face_last_seen: dict = {}       # face_id -> unix ts the greeting probe last saw them


class GreetingRequest(BaseModel):
    face_id: int
    name:    str


@app.post("/v1/proactive_greeting")
async def proactive_greeting(req: GreetingRequest):
    """Decide whether Vector should greet a just-seen known person, and if so
    produce the line. Returns empty text when no greeting is warranted."""
    now = _time.time()
    fid, name = req.face_id, (req.name or "").strip()
    if fid <= 0 or not name:
        return {"text": ""}

    prev_seen = _face_last_seen.get(fid, 0.0)
    _face_last_seen[fid] = now
    arrived = (prev_seen == 0.0) or (now - prev_seen > GREETING_ABSENCE_GAP)

    meta = MEMORY.get_face_meta(fid)
    last_convo = (meta or {}).get("last_convo_at") or 0.0
    conversed_recently = bool(last_convo) and (now - last_convo) < GREETING_ABSENCE_GAP

    if not arrived or conversed_recently:
        return {"text": ""}

    now_dt = datetime.now()
    bits = [f"{name} has just come into view. It is {_time_of_day(now_dt)}."]
    if last_convo:
        bits.append(f"You last spoke with {name} {_relative_time(now - last_convo)}.")
        summ = (meta or {}).get("last_convo_summary")
        if summ:
            bits.append(f"That conversation was about: {summ}.")
    else:
        bits.append(f"You have not properly spoken with {name} before.")
    if _mood_state["text"]:
        bits.append(f"Your current mood: {_mood_state['text']}.")

    bits.append(f"For variety, this greeting should: {random.choice(_GREETING_ANGLES)}.")
    if _recent_greetings:
        bits.append(
            "CRITICAL: do not reuse the opening or sentence structure of your "
            "recent greetings â€” no shared opening words, no rephrasings of: "
            + " ; ".join(f'"{g}"' for g in _recent_greetings[-5:]) + "."
        )

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, read=30.0)) as client:
            resp = await client.post(
                f"{OLLAMA_BASE}/v1/chat/completions",
                json={
                    "model":        MODEL,
                    "messages": [
                        {"role": "system", "content": _GREETING_SYSTEM},
                        {"role": "user",
                         "content": " ".join(bits) + " Greet them now."},
                    ],
                    "stream":       False,
                    "temperature":  1.3,
                    "top_p":        0.95,
                    "seed":         random.randint(1, 2**31 - 1),
                    "keep_alive":   OLLAMA_KEEP_ALIVE,
                    "options": {
                        "num_gpu":  99,
                        "num_ctx":  1024,
                    },
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[greeting] error: {e}")
        return {"text": "", "error": str(e)}

    line = _strip_for_speech(text)
    if line:
        _recent_greetings.append(line)
        del _recent_greetings[:-6]
    print(f"[greeting] {name} (arrived) -> {line!r}")
    return {"text": line}


# â”€â”€ Live status + activity stream endpoints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/v1/status")
async def vector_status():
    """Snapshot of everything worth showing on a status card."""
    return {
        "face":          current_face(),
        "mood":          _mood_state["text"],
        "model_loaded":  await model_is_loaded(),
        "is_picked_up":  _robot_state["is_picked_up"],
        "on_charger":    _robot_state["on_charger"],
        "battery_level": _robot_state["battery_pct"],   # enum 0-3
        "battery_volts": _robot_state.get("battery_volts"),
        "battery_ts":    _robot_state["battery_ts"],
        "last_heard":    _robot_state["last_heard"],
        "last_heard_ts": _robot_state["last_heard_ts"],
        "last_said":     _robot_state["last_said"],
        "last_said_ts":  _robot_state["last_said_ts"],
        "last_sensor":   _robot_state["last_sensor"],
        "last_sensor_ts":_robot_state["last_sensor_ts"],
        "building":      _robot_state["building"],
        "quiet_mode":    _ambient_state["quiet"],
    }


@app.get("/v1/activity/stream")
async def activity_stream():
    """SSE stream of live Vector activity events.
    On connect, replays the last 60 events so the card isn't blank.
    Event types: heard | thinking | said | sensor | face | battery | weather."""
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _activity_subs.append(q)

    # Replay recent history so new clients aren't staring at a blank card
    for evt in list(_activity_log):
        await q.put(evt)

    async def generate():
        try:
            while True:
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=25.0)
                    yield f"data: {json.dumps(evt)}\n\n"
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            try:
                _activity_subs.remove(q)
            except ValueError:
                pass

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@app.get("/v1/cam")
async def cam_proxy():
    """Proxy Vector's MJPEG camera feed from chipper's cam-stream endpoint.
    Enables the Pi (over Tailscale) to embed the feed without opening port 8080."""
    upstream = "http://127.0.0.1:8080/cam-stream"
    params   = {"serial": VECTOR_SERIAL}

    async def stream():
        try:
            # read=30s so a stalled chipper cam connection gets killed and the
            # client reconnects (client restarts the src every 25s anyway).
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=10.0)
            ) as client:
                async with client.stream("GET", upstream, params=params) as resp:
                    async for chunk in resp.aiter_bytes(4096):
                        yield chunk
        except Exception as e:
            print(f"[cam] stream ended: {e}")

    return StreamingResponse(
        stream(),
        media_type="multipart/x-mixed-replace; boundary=--boundary",
        headers={"Cache-Control": "no-cache"},
    )





