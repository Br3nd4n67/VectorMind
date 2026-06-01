import os
#!/usr/bin/env python3
"""
Tornado Warning Dashboard â€” full-screen kiosk page on port 8082.
Polls NWS every 60s and pushes live updates via Server-Sent Events.
Has a floating nav button to switch back to network-monitor (8080) or pc-temps (8081).
"""

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  TABLE OF CONTENTS
#  Â§1   Imports
#  Â§2   Constants & Configuration
#  Â§3   Runtime State
#  Â§4   Database
#  Â§5   Shared Helpers
#  Â§6   Tornado Backend
#  Â§7   LSR (Local Storm Reports) Backend
#  Â§8   Hurricane / Tropical Backend
#  Â§9   Winter Weather Backend
#  Â§10  Test Fixtures
#  Â§11  Page Templates
#  Â§12  Routes
#  Â§13  Entry Point
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§1  IMPORTS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
import csv
import random
import io
import json
import re
import time
import sqlite3
import threading
import logging
from collections import Counter
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

CST = ZoneInfo("America/Chicago")

import requests
from flask import Flask, Response, render_template_string, request

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§2  CONSTANTS & CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
POLL_INTERVAL = 60
PORT        = 8082
NWS_URL     = "https://api.weather.gov/alerts/active"
NWS_HEADERS = {
    "User-Agent": "TornadoDashboard/1.0 (kiosk)",
    "Accept": "application/geo+json",
}
TEST_KEY = "0pEtEyDv5VJrRkdMY8hE"
DB_PATH  = Path(__file__).parent / "history.db"
SPC_LSR_URL      = "https://www.spc.noaa.gov/climo/reports/today_torn.csv"
LSR_POLL_INTERVAL = 300   # 5 minutes
NHC_STORMS_URL    = "https://www.nhc.noaa.gov/CurrentStorms.json"
NHC_POLL_INTERVAL = 300   # 5 minutes

# Vector AI â€” Windows machine Tailscale IP. Set to None to disable announcements.
VECTOR_AI_URL = os.getenv("VECTOR_AI_URL", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§3  RUNTIME STATE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
_lock      = threading.Lock()
_warnings  = []
_watches   = []
_last_poll = "Never"
_lsr_lock     = threading.Lock()
_lsrs         = []
_server_start = str(int(time.time()))   # bumped on every restart; clients use it to auto-reload

# â”€â”€ Vector announcement state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Track which alerts have already been announced so we only speak each one once.
# Keys are sent_raw (ISO timestamp from NWS) â€” stable and unique per alert.
_announced_keys:   set  = set()
_recent_vx_lines:  list = []   # last 6 lines spoken, for variety avoidance

# â”€â”€ Vector announce helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _vector_announce(event_type: str, area: str, detection: str = None,
                     headline: str = None, severity: str = None):
    """POST a structured alert to vector-ai /v1/weather_announce in a daemon thread.
    Returns immediately; Vector speaks asynchronously."""
    if not VECTOR_AI_URL:
        return

    def _call():
        global _recent_vx_lines
        payload = {
            "event_type": event_type,
            "area":       area[:120],
            "detection":  detection,
            "headline":   (headline or "")[:120] or None,
            "severity":   severity,
            "avoid":      list(_recent_vx_lines[-4:]),
        }
        try:
            resp = requests.post(
                f"{VECTOR_AI_URL}/v1/weather_announce",
                json=payload,
                timeout=340,   # cold 70B load (~90s) + LLM gen (~20s) + chipper say (~5s)
            )
            data = resp.json()
            line = (data.get("text") or "").strip()
            if line:
                _recent_vx_lines.append(line)
                del _recent_vx_lines[:-6]
                log.info("Vector announced [%s]: %s", event_type, line)
        except Exception as exc:
            log.warning("Vector announce failed (%s %s): %s", event_type, area, exc)

    threading.Thread(target=_call, daemon=True).start()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§4  DATABASE
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def db_connect():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    with db_connect() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS warnings_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sent_raw    TEXT UNIQUE,
                area        TEXT,
                detection   TEXT,
                headline    TEXT,
                issued      TEXT,
                expires     TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                expired     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS watches_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sent_raw    TEXT UNIQUE,
                area        TEXT,
                headline    TEXT,
                issued      TEXT,
                expires     TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                expired     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS winter_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sent_raw    TEXT UNIQUE,
                area        TEXT,
                event       TEXT,
                severity    TEXT,
                headline    TEXT,
                issued      TEXT,
                expires     TEXT,
                first_seen  TEXT,
                last_seen   TEXT,
                expired     INTEGER DEFAULT 0,
                tna         INTEGER DEFAULT 0
            );
        """)
    log.info("DB initialised at %s", DB_PATH)

def db_upsert_warning(w: dict, now_str: str):
    key = w.get("sent_raw", "")
    if not key:
        return
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO warnings_log (sent_raw, area, detection, headline, issued, expires, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(sent_raw) DO UPDATE SET
                detection = excluded.detection,
                last_seen = excluded.last_seen,
                expired   = 0
        """, (key, w["area"], w["detection"], w["headline"], w["issued"], w["expires"], now_str, now_str))

def db_upsert_watch(w: dict, now_str: str):
    key = w.get("sent_raw", "")
    if not key:
        return
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO watches_log (sent_raw, area, headline, issued, expires, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(sent_raw) DO UPDATE SET
                last_seen = excluded.last_seen,
                expired   = 0
        """, (key, w["area"], w["headline"], w["issued"], w["expires"], now_str, now_str))

def db_expire_warnings(active_keys: set):
    with db_connect() as conn:
        if active_keys:
            conn.execute(
                "UPDATE warnings_log SET expired=1 WHERE expired=0 AND sent_raw NOT IN ({})".format(
                    ",".join("?" * len(active_keys))
                ),
                list(active_keys)
            )
        else:
            conn.execute("UPDATE warnings_log SET expired=1 WHERE expired=0")

def db_expire_watches(active_keys: set):
    with db_connect() as conn:
        if active_keys:
            conn.execute(
                "UPDATE watches_log SET expired=1 WHERE expired=0 AND sent_raw NOT IN ({})".format(
                    ",".join("?" * len(active_keys))
                ),
                list(active_keys)
            )
        else:
            conn.execute("UPDATE watches_log SET expired=1 WHERE expired=0")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§5  SHARED HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def extract_states(area: str) -> set:
    """Pull 2-letter state codes out of an NWS area string."""
    return set(re.findall(r'\b([A-Z]{2})\b', area))


def detection_type(props: dict) -> str:
    params = props.get("parameters", {})
    td_list = params.get("tornadoDetection", [])
    text = (props.get("description", "") + " " + props.get("headline", "")).upper()

    base = None
    if isinstance(td_list, list) and td_list:
        val = td_list[0].upper()
        if "CONFIRM" in val or "OBSERV" in val:
            base = "CONFIRMED"
        elif "RADAR" in val:
            base = "RADAR INDICATED"

    if base is None:
        if re.search(
            r"TORNADO[.\s,]+(CONFIRMED|OBSERVED)"
            r"|CONFIRMED[.\s,]+TORNADO"
            r"|CONFIRMED TOUCHDOWN"
            r"|TORNADO ON THE GROUND"
            r"|OBSERVED TORNADO"
            r"|TORNADO IS ON THE GROUND",
            text,
        ):
            base = "CONFIRMED"
        else:
            return "RADAR INDICATED"

    if base != "CONFIRMED":
        return base

    sources = [
        (r"TRAINED SPOTTER",           "Spotter"),
        (r"EMERGENCY MANAGER",         "Emerg. Mgr"),
        (r"LAW ENFORCEMENT",           "Law Enforcement"),
        (r"BROADCAST MEDIA",           "Media"),
        (r"NWS EMPLOYEE|NWS SURVEY",   "NWS Survey"),
        (r"PUBLIC",                    "Public Report"),
    ]
    for pattern, label in sources:
        if re.search(pattern, text):
            return f"CONFIRMED - {label}"
    return "CONFIRMED"


def fmt_time(iso_str):
    if not iso_str:
        return "--"
    try:
        return datetime.fromisoformat(iso_str).astimezone(CST).strftime("%-I:%M %p %Z")
    except Exception:
        return iso_str


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§6  TORNADO BACKEND
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def fetch_warnings():
    try:
        resp = requests.get(
            NWS_URL,
            params={"event": "Tornado Warning"},
            headers=NWS_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        out = []
        for f in resp.json().get("features", []):
            p = f["properties"]
            out.append({
                "area":        p.get("areaDesc", "Unknown"),
                "issued":      fmt_time(p.get("sent")),
                "expires":     fmt_time(p.get("expires")),
                "expires_raw": p.get("expires", ""),
                "sent_raw":    p.get("sent", ""),
                "detection":   detection_type(p),
                "headline":    (p.get("headline") or "").replace("\n", " "),
                "description": (p.get("description") or "").strip(),
            })
        return out
    except requests.RequestException as exc:
        log.warning("NWS request failed: %s", exc)
        return []


def fetch_watches():
    try:
        resp = requests.get(
            NWS_URL,
            params={"event": "Tornado Watch"},
            headers=NWS_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        out = []
        for f in resp.json().get("features", []):
            p = f["properties"]
            out.append({
                "area":        p.get("areaDesc", "Unknown"),
                "issued":      fmt_time(p.get("sent")),
                "expires":     fmt_time(p.get("expires")),
                "expires_raw": p.get("expires", ""),
                "sent_raw":    p.get("sent", ""),
                "headline":    (p.get("headline") or "").replace("\n", " "),
                "upgraded":    False,
            })
        return out
    except requests.RequestException as exc:
        log.warning("NWS watch request failed: %s", exc)
        return []


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§7  LSR BACKEND
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def fetch_lsrs() -> list:
    try:
        resp = requests.get(SPC_LSR_URL, timeout=20,
                            headers={"User-Agent": "TornadoDashboard/1.0 (kiosk)", "Accept": "text/csv"})
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        out = []
        for row in reader:
            out.append({
                "time":     row.get("Time", ""),
                "f_scale":  row.get("F_Scale", ""),
                "location": row.get("Location", ""),
                "county":   row.get("County", ""),
                "state":    row.get("State", ""),
                "lat":      row.get("Lat", ""),
                "lon":      row.get("Lon", ""),
                "comments": row.get("Comments", ""),
            })
        return out
    except Exception as exc:
        log.warning("SPC LSR fetch failed: %s", exc)
        return []

def lsr_poller():
    global _lsrs
    while True:
        lsrs = fetch_lsrs()
        with _lsr_lock:
            _lsrs = lsrs
        log.info("SPC LSR poll â€” %d tornado reports", len(lsrs))
        time.sleep(LSR_POLL_INTERVAL)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§8  HURRICANE / TROPICAL BACKEND
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

_hurricane_lock = threading.Lock()
_storms         = []

_CAT_COLORS = {
    "C5": "#cc44ff", "C4": "#ff2222", "C3": "#ff6600",
    "C2": "#ffaa00", "C1": "#ffee44",
    "TS": "#44bbff", "TD": "#5588ff", "LO": "#888888",
}


def classify_storm(cls_code: str, winds_mph: int) -> str:
    c = (cls_code or "").upper()
    if c in ("TD", "SD"):
        return "TD"
    if c in ("TS", "SS", "TT"):
        return "TS"
    if c in ("HU", "TY", "ST", "TC"):
        if winds_mph >= 157: return "C5"
        if winds_mph >= 130: return "C4"
        if winds_mph >= 111: return "C3"
        if winds_mph >= 96:  return "C2"
        return "C1"
    return "LO"


def fetch_storms() -> list:
    try:
        resp = requests.get(
            NHC_STORMS_URL, timeout=20,
            headers={"User-Agent": "TornadoDashboard/1.0 (kiosk)"}
        )
        resp.raise_for_status()
        active = resp.json().get("activeStorms", [])
        out = []
        for s in active:
            try:
                kt = int(s.get("intensity", 0) or 0)
            except (ValueError, TypeError):
                kt = 0
            mph = round(kt * 1.15078)
            cat = classify_storm(s.get("classification", ""), mph)
            upd = s.get("lastUpdate", "")
            try:
                upd = datetime.fromisoformat(upd).astimezone(CST).strftime("%-I:%M %p %Z %m/%d")
            except Exception:
                pass
            out.append({
                "id":           s.get("id", ""),
                "name":         (s.get("name") or "Unnamed").title(),
                "classification": s.get("classification", ""),
                "category":     cat,
                "winds_mph":    mph,
                "winds_kt":     kt,
                "pressure":     s.get("pressure", "â€”"),
                "lat":          float(s.get("latitudeNumeric") or 0),
                "lon":          float(s.get("longitudeNumeric") or 0),
                "lat_str":      s.get("latitude", ""),
                "lon_str":      s.get("longitude", ""),
                "movement_dir": int(s.get("movementDir") or 0),
                "movement_spd": int(s.get("movementSpeed") or 0),
                "last_update":  upd,
                "basin":        ((s.get("id") or "??")[:2]).upper(),
                "color":        _CAT_COLORS.get(cat, "#888888"),
            })
        return out
    except Exception as exc:
        log.warning("NHC fetch failed: %s", exc)
        return []


def hurricane_poller():
    global _storms
    while True:
        storms = fetch_storms()
        with _hurricane_lock:
            _storms = storms
        log.info("NHC poll â€” %d active tropical cyclones", len(storms))
        time.sleep(NHC_POLL_INTERVAL)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§9  WINTER WEATHER BACKEND
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

WINTER_EVENTS = {
    "Blizzard Warning",
    "Winter Storm Warning",
    "Winter Storm Watch",
    "Ice Storm Warning",
    "Winter Weather Advisory",
    "Freezing Rain Advisory",
    "Freezing Drizzle Advisory",
    "Lake Effect Snow Warning",
    "Lake Effect Snow Watch",
    "Lake Effect Snow Advisory",
    "Wind Chill Warning",
    "Wind Chill Watch",
    "Wind Chill Advisory",
    "Heavy Snow Warning",
    "Snow Squall Warning",
}

WINTER_POLL_INTERVAL = 60   # 1 minute, matches tornado poller

WINTER_SEVERITY_COLORS = {
    "BLIZZARD":  "#cc44ff",  # purple   â€” extreme
    "ICE_STORM": "#ff4444",  # red      â€” severe
    "WSW":       "#4488ff",  # blue     â€” Winter Storm Warning
    "SNOW_WARN": "#44ccff",  # cyan     â€” Lake Effect / Heavy Snow Warning
    "WIND_WARN": "#ff8c00",  # orange   â€” Wind Chill Warning, Snow Squall
    "WATCH":     "#ffaa00",  # amber    â€” any watch
    "ADVISORY":  "#aaccff",  # pale blue â€” any advisory
}

WINTER_TEXT_COLORS = {
    "BLIZZARD":  "#fff",
    "ICE_STORM": "#fff",
    "WSW":       "#fff",
    "SNOW_WARN": "#000",
    "WIND_WARN": "#fff",
    "WATCH":     "#000",
    "ADVISORY":  "#000",
}

_winter_lock    = threading.Lock()
_winter_alerts  = []
_zone_geo_cache = {}          # zone URL -> geometry dict (persists for life of process)
_zone_geo_lock  = threading.Lock()


def classify_winter(event_type: str) -> str:
    e = event_type.lower()
    if "blizzard"                        in e: return "BLIZZARD"
    if "ice storm"                       in e: return "ICE_STORM"
    if "winter storm warning"            in e: return "WSW"
    if "lake effect snow warning"        in e: return "SNOW_WARN"
    if "heavy snow warning"              in e: return "SNOW_WARN"
    if "wind chill warning"              in e: return "WIND_WARN"
    if "snow squall warning"             in e: return "WIND_WARN"
    if "watch"                           in e: return "WATCH"
    return "ADVISORY"


def travel_not_advised(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return (
        "travel not advised"   in t
        or "do not travel"     in t
        or "travel discouraged" in t
        or "travel is not advised" in t
    )


def fetch_zone_geometry(zone_url: str) -> dict | None:
    """Fetch a NWS forecast zone polygon and cache it for the life of the process."""
    with _zone_geo_lock:
        if zone_url in _zone_geo_cache:
            return _zone_geo_cache[zone_url]
    try:
        resp = requests.get(zone_url, headers=NWS_HEADERS, timeout=10)
        resp.raise_for_status()
        geom = resp.json().get("geometry")
        with _zone_geo_lock:
            _zone_geo_cache[zone_url] = geom
        return geom
    except Exception as exc:
        log.debug("Zone geo fetch failed (%s): %s", zone_url, exc)
        return None


def resolve_alert_geometry(feature: dict) -> dict | None:
    """Return geometry from the alert feature itself, or look it up from affectedZones."""
    geom = feature.get("geometry")
    if geom:
        return geom
    zones = feature.get("properties", {}).get("affectedZones", [])
    if not zones:
        return None
    polys = []
    for url in zones[:20]:    # cap at 20 zones per alert to avoid runaway fetches
        z = fetch_zone_geometry(url)
        if z is None:
            continue
        if z["type"] == "Polygon":
            polys.append(z["coordinates"])
        elif z["type"] == "MultiPolygon":
            polys.extend(z["coordinates"])
    if not polys:
        return None
    if len(polys) == 1:
        return {"type": "Polygon", "coordinates": polys[0]}
    return {"type": "MultiPolygon", "coordinates": polys}


def fetch_winter_alerts() -> list:
    try:
        resp = requests.get(
            NWS_URL,
            params={"status": "actual"},
            headers=NWS_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        out = []
        for feature in resp.json().get("features", []):
            p     = feature.get("properties", {})
            event = p.get("event", "")
            if event not in WINTER_EVENTS:
                continue
            sev  = classify_winter(event)
            desc = (p.get("description", "") or "") + " " + (p.get("instruction", "") or "")
            tna  = travel_not_advised(desc)
            out.append({
                "area":        p.get("areaDesc", ""),
                "event":       event,
                "severity":    sev,
                "headline":    (p.get("headline") or "").replace("\n", " "),
                "issued":      fmt_time(p.get("onset") or p.get("effective", "")),
                "expires":     fmt_time(p.get("expires", "")),
                "expires_raw": p.get("expires", ""),
                "sent_raw":    p.get("sent", ""),
                "tna":         tna,
                "color":       WINTER_SEVERITY_COLORS.get(sev, "#aaccff"),
                "text_color":  WINTER_TEXT_COLORS.get(sev, "#000"),
                "geometry":    resolve_alert_geometry(feature),  # resolves zone polygons when geometry is null
            })
        return out
    except requests.RequestException as exc:
        log.warning("NWS winter fetch failed: %s", exc)
        return []


def db_upsert_winter(w: dict, now_str: str):
    key = w.get("sent_raw", "")
    if not key:
        return
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO winter_log
                (sent_raw, area, event, severity, headline, issued, expires,
                 first_seen, last_seen, tna)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(sent_raw) DO UPDATE SET
                area      = excluded.area,
                event     = excluded.event,
                severity  = excluded.severity,
                headline  = excluded.headline,
                issued    = excluded.issued,
                expires   = excluded.expires,
                last_seen = excluded.last_seen,
                expired   = 0,
                tna       = excluded.tna
        """, (key, w["area"], w["event"], w["severity"], w["headline"],
              w["issued"], w["expires"], now_str, now_str, 1 if w["tna"] else 0))


def db_expire_winter(active_keys: set):
    with db_connect() as conn:
        if active_keys:
            conn.execute(
                "UPDATE winter_log SET expired=1 WHERE expired=0 AND sent_raw NOT IN ({})".format(
                    ",".join("?" * len(active_keys))
                ),
                list(active_keys)
            )
        else:
            conn.execute("UPDATE winter_log SET expired=1 WHERE expired=0")


def winter_poller():
    global _winter_alerts
    while True:
        alerts  = fetch_winter_alerts()
        now_str = datetime.now(tz=CST).isoformat()
        active_keys = set()
        for w in alerts:
            if w["sent_raw"]:
                active_keys.add(w["sent_raw"])
                db_upsert_winter(w, now_str)
        db_expire_winter(active_keys)
        with _winter_lock:
            _winter_alerts = alerts
        log.info("NWS winter poll â€” %d active alerts", len(alerts))
        time.sleep(WINTER_POLL_INTERVAL)


def poller():
    global _warnings, _watches, _last_poll
    while True:
        warnings = fetch_warnings()
        watches  = fetch_watches()
        now      = datetime.now().strftime("%m/%d/%Y  %I:%M:%S %p")
        now_iso  = datetime.now(tz=CST).isoformat()

        # Mark watches that share a state with an active warning
        warning_states = set()
        for w in warnings:
            warning_states |= extract_states(w["area"])
        for watch in watches:
            watch["upgraded"] = bool(extract_states(watch["area"]) & warning_states)

        # Log to SQLite
        active_warning_keys = set()
        active_watch_keys   = set()
        for w in warnings:
            db_upsert_warning(w, now_iso)
            if w.get("sent_raw"):
                active_warning_keys.add(w["sent_raw"])
        for w in watches:
            db_upsert_watch(w, now_iso)
            if w.get("sent_raw"):
                active_watch_keys.add(w["sent_raw"])
        db_expire_warnings(active_warning_keys)
        db_expire_watches(active_watch_keys)

        # Announce new warnings to Vector (one call per new alert, never repeated)
        for w in warnings:
            key = w.get("sent_raw")
            if key and key not in _announced_keys:
                _announced_keys.add(key)
                _vector_announce(
                    event_type="tornado_warning",
                    area=w.get("area", ""),
                    detection=w.get("detection"),
                    headline=w.get("headline"),
                )

        with _lock:
            # Preserve any test-injected alerts so the poller doesn't wipe them
            _warnings  = warnings + [w for w in _warnings if w.get("_test")]
            _watches   = watches  + [w for w in _watches  if w.get("_test")]
            _last_poll = now
        log.info("Polled NWS -- %d warnings, %d watches", len(warnings), len(watches))
        time.sleep(POLL_INTERVAL)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§11  PAGE TEMPLATES
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

KIOSK_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒªï¸</text></svg>">
<title>Tornado Warnings</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a0a0f;
    color: #fff;
    font-family: 'Segoe UI', Arial, sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  header {
    background: #1a0000;
    border-bottom: 3px solid #cc0000;
    padding: 8px 16px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-shrink: 0;
  }
  header h1 {
    font-size: 1.3rem;
    font-weight: 800;
    letter-spacing: 0.08em;
    color: #ff4444;
    text-transform: uppercase;
  }
  .pulse { animation: pulse 1.4s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  /* â”€â”€ Confirmed touchdown: full-screen strobe â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  body.confirmed-active {
    animation: body-strobe 1.2s ease-in-out infinite;
  }
  @keyframes body-strobe {
    0%,100% { background: #0a0a0f; }
    50%     { background: #3a0000; }
  }

  /* â”€â”€ Confirmed card glow + border pulse â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  .warning.confirmed {
    animation: card-flash 1.2s ease-in-out infinite;
    box-shadow: 0 0 18px 4px #ff222288;
  }
  @keyframes card-flash {
    0%,100% { background: #2a0000; border-color: #ff2222; box-shadow: 0 0 18px 4px #ff222288; }
    50%     { background: #550000; border-color: #ff6666; box-shadow: 0 0 32px 10px #ff2222cc; }
  }

  /* â”€â”€ Header flash on confirmed â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  header.confirmed-active {
    animation: header-flash 1.2s ease-in-out infinite;
  }
  @keyframes header-flash {
    0%,100% { background: #1a0000; border-color: #cc0000; }
    50%     { background: #4a0000; border-color: #ff4444; }
  }
  #clock { font-size: 0.8rem; color: #aaa; text-align: right; line-height: 1.4; }
  #last-poll { font-size: 0.65rem; color: #555; }

  /* â”€â”€ Split content area â”€â”€ */
  #content { flex: 1; overflow: hidden; display: flex; flex-direction: column; }
  #warnings-pane { flex: 1; overflow-y: hidden; padding: 8px 14px; }
  #scroll-inner { display: inline-block; width: 100%; }
  @keyframes scroll-up {
    0%   { transform: translateY(0); }
    10%  { transform: translateY(0); }
    80%  { transform: translateY(var(--scroll-dist)); }
    90%  { transform: translateY(var(--scroll-dist)); }
    100% { transform: translateY(0); }
  }
  #scroll-inner.scrolling { animation: scroll-up var(--scroll-dur) ease-in-out infinite; }

  #no-warnings {
    display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    height: 100%; gap: 14px;
  }
  #no-warnings .icon  { font-size: 3.5rem; }
  #no-warnings .label { font-size: 1.6rem; font-weight: 700; color: #22cc66; letter-spacing: 0.06em; }

  .warning { border-radius: 6px; padding: 8px 12px; margin-bottom: 6px; border-left: 5px solid; }
  .warning.confirmed { background: #2a0000; border-color: #ff2222; }
  .warning.radar     { background: #1e1200; border-color: #ff8c00; }
  .badge {
    display: inline-block; font-size: 0.6rem; font-weight: 700;
    letter-spacing: 0.08em; padding: 2px 6px; border-radius: 3px;
    text-transform: uppercase; margin-bottom: 3px;
  }
  .badge.confirmed { background: #ff2222; color: #fff; }
  .badge.radar     { background: #ff8c00; color: #000; }
  .area     { font-size: 0.95rem; font-weight: 700; line-height: 1.2; margin-bottom: 2px; }
  .headline { font-size: 0.68rem; color: #ccc; margin-bottom: 4px; }
  .times    { display: flex; gap: 12px; font-size: 0.65rem; color: #aaa; }
  .times span strong { color: #fff; }
  .age      { font-size: 0.6rem; color: #555; margin-left: auto; font-style: italic; }


  /* â”€â”€ Test alert button â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  #test-btn {
    position: fixed; bottom: 14px; left: 14px; z-index: 200;
    background: #1a0000; border: 1px solid #550000; color: #884444;
    font-size: 0.75rem; padding: 5px 11px; border-radius: 5px;
    cursor: pointer; user-select: none; opacity: 0.5;
    transition: opacity 0.2s;
  }
  #test-btn:hover { opacity: 1; background: #2a0000; color: #ff4444; border-color: #aa0000; }
  #test-btn.fired { background: #003300; border-color: #005500; color: #44aa44; opacity: 1; }

  /* â”€â”€ Home button â”€â”€ */
  .home-btn {
    position: fixed; bottom: 14px; right: 14px; z-index: 200;
    background: #111; border: 1px solid #2a2a2a; color: #555;
    font-size: 0.72rem; padding: 5px 13px; border-radius: 5px;
    text-decoration: none; opacity: 0.6; transition: opacity 0.2s;
  }
  .home-btn:hover { opacity: 1; color: #aaa; border-color: #555; }

  /* â”€â”€ Other-view iframe overlay â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ */
  #view-overlay {
    display: none;
    position: fixed; inset: 0; z-index: 300;
    flex-direction: column;
  }
  #view-overlay.open { display: flex; }
  #view-bar {
    background: #111; border-bottom: 2px solid #333;
    padding: 8px 16px; display: flex; align-items: center;
    justify-content: space-between; flex-shrink: 0;
  }
  #view-bar span { font-size: 0.9rem; color: #aaa; }
  #back-btn {
    background: #cc0000; border: none; color: #fff;
    font-size: 0.85rem; font-weight: 700; padding: 6px 16px;
    border-radius: 5px; cursor: pointer; letter-spacing: 0.05em;
  }
  #back-btn:hover { background: #ee0000; }
  #alert-banner {
    display: none; background: #ff2222; color: #fff;
    text-align: center; font-size: 1rem; font-weight: 700;
    padding: 10px; letter-spacing: 0.08em; flex-shrink: 0;
    animation: pulse 1s ease-in-out infinite;
  }
  #view-frame { flex: 1; border: none; width: 100%; }
</style>
</head>
<body>
<header>
  <h1 id="header-text">Tornado Warnings</h1>
  <div id="clock">
    <div id="time-display"></div>
    <div id="last-poll">Last NWS poll: <span id="poll-time">&mdash;</span></div>
  </div>
</header>

<div id="content">
  <div id="warnings-pane">
    <div id="scroll-inner">
      <div id="warnings-list"></div>
    </div>
    <div id="no-warnings" style="display:none">
      <div class="icon">&#x2714;</div>
      <div class="label">No Active Tornado Warnings</div>
    </div>
  </div>
</div>

<!-- Test alert button -->
<button id="test-btn" onclick="sendTest()">&#x26A0; Test Alert</button>

<a class="home-btn" href="http://localhost:8080/">Home</a>

<!-- Full-screen iframe overlay (shown when viewing other dashboards) -->
<div id="view-overlay">
  <div id="view-bar">
    <span id="view-title"></span>
    <button id="back-btn" onclick="closeView()">&#x2190; Back to Tornado Warnings</button>
  </div>
  <div id="alert-banner">&#x26A0; TORNADO WARNING ACTIVE &mdash; RETURNING NOW</div>
  <iframe id="view-frame" src="about:blank"></iframe>
</div>

<script>
  // â”€â”€ Clock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function tick() {
    const now = new Date();
    document.getElementById('time-display').textContent =
      now.toLocaleDateString('en-US',{month:'2-digit',day:'2-digit',year:'numeric'}) + '  ' +
      now.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',second:'2-digit'});
  }
  tick(); setInterval(tick, 1000);

  // â”€â”€ Test alert button â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function sendTest() {
    var btn = document.getElementById('test-btn');
    btn.textContent = 'â³ Firing...';
    btn.disabled = true;
    fetch('/test/fire?key={{ TEST_KEY }}&type=all&duration=60')
      .then(function(r){ return r.text(); })
      .then(function(){
        btn.textContent = 'âœ” Test Fired!';
        btn.classList.add('fired');
        setTimeout(function(){
          btn.textContent = 'âš  Test Alert';
          btn.classList.remove('fired');
          btn.disabled = false;
        }, 62000);
      })
      .catch(function(){
        btn.textContent = 'âš  Test Alert';
        btn.disabled = false;
      });
  }


  // â”€â”€ Iframe overlay â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function openView(name, url) {
    document.getElementById('nav-drawer').classList.remove('open');
    document.getElementById('view-title').textContent = name;
    document.getElementById('view-frame').src = url;
    document.getElementById('alert-banner').style.display = 'none';
    document.getElementById('view-overlay').classList.add('open');
  }
  function closeView() {
    document.getElementById('view-overlay').classList.remove('open');
    document.getElementById('view-frame').src = 'about:blank';
  }

  // â”€â”€ Age helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function ageStr(sent_raw) {
    if (!sent_raw) return '';
    var ms = Date.now() - new Date(sent_raw).getTime();
    if (ms < 0) return '';
    var mins = Math.floor(ms / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return mins + 'm ago';
    var h = Math.floor(mins/60), m = mins%60;
    return h + 'h' + (m ? ' ' + m + 'm' : '') + ' ago';
  }

  // â”€â”€ SSE + render (with auto-reconnect) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var lastKData = { warnings: [], watches: [] };
  var sseRetryTimer = null;
  var _sseStart = null;

  function connectSSE() {
    if (sseRetryTimer) { clearTimeout(sseRetryTimer); sseRetryTimer = null; }
    var src = new EventSource('/stream');
    src.onmessage = function(e) {
      if (e.data === 'ping') return;
      try {
        var data = JSON.parse(e.data);
        // Reload if server was restarted (new server_start token)
        if (data.server_start) {
          if (_sseStart === null) { _sseStart = data.server_start; }
          else if (_sseStart !== data.server_start) { location.reload(); return; }
        }
        lastKData = data;
        document.getElementById('poll-time').textContent = data.last_poll;
        render(data.warnings || [], data.watches || []);
      } catch(err) {}
    };
    src.onerror = function() { src.close(); sseRetryTimer = setTimeout(connectSSE, 5000); };
  }
  connectSSE();
  setInterval(function() { render(lastKData.warnings || [], lastKData.watches || []); }, 60000);

  function render(warnings, watches) {
    var list    = document.getElementById('warnings-list');
    var inner   = document.getElementById('scroll-inner');
    var none    = document.getElementById('no-warnings');
    var hdr     = document.getElementById('header-text');
    var overlay = document.getElementById('view-overlay');
    var banner  = document.getElementById('alert-banner');
    var wpane   = document.getElementById('warnings-pane');
    var hasWarnings = warnings && warnings.length > 0;

    // Auto-dismiss overlay when a warning comes in
    if (hasWarnings && overlay.classList.contains('open')) {
      banner.style.display = 'block';
      setTimeout(closeView, 3000);
    }

    // â”€â”€ Header â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (!hasWarnings) {
      list.innerHTML = '';
      none.style.display = 'flex';
      inner.classList.remove('scrolling');
      hdr.classList.remove('pulse');
      hdr.style.color = '#22cc66';
      hdr.textContent = 'Tornado Warnings';
      document.body.classList.remove('confirmed-active');
      document.querySelector('header').classList.remove('confirmed-active');
    } else {
      none.style.display = 'none';
      hdr.style.color = '#ff4444';
      hdr.textContent = warnings.length === 1 ? '1 Active Tornado Warning' : warnings.length + ' Active Tornado Warnings';
      var hasConfirmed = warnings.some(function(w){ return w.detection.startsWith('CONFIRMED'); });
      if (hasConfirmed) {
        hdr.classList.add('pulse');
        document.body.classList.add('confirmed-active');
        document.querySelector('header').classList.add('confirmed-active');
      } else {
        hdr.classList.remove('pulse');
        document.body.classList.remove('confirmed-active');
        document.querySelector('header').classList.remove('confirmed-active');
      }
    }

    // â”€â”€ Warning cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (hasWarnings) {
      none.style.display = 'none';
      list.innerHTML = warnings.map(function(w) {
        var cls = w.detection.startsWith('CONFIRMED') ? 'confirmed' : 'radar';
        var lbl = w.detection.startsWith('CONFIRMED') ? w.detection : 'RADAR INDICATED';
        var ag  = w.sent_raw ? '<span class="age">' + esc(ageStr(w.sent_raw)) + '</span>' : '';
        return '<div class="warning ' + cls + '">'
          + '<div class="badge ' + cls + '">' + lbl + '</div>'
          + '<div class="area">'     + esc(w.area)     + '</div>'
          + '<div class="headline">' + esc(w.headline) + '</div>'
          + '<div class="times">'
          + '<span><strong>Issued:</strong> '  + esc(w.issued)  + '</span>'
          + '<span><strong>Expires:</strong> ' + esc(w.expires) + '</span>'
          + ag + '</div></div>';
      }).join('');
    }

    // Auto-scroll
    inner.classList.remove('scrolling');
    inner.style.removeProperty('--scroll-dist');
    inner.style.removeProperty('--scroll-dur');
    requestAnimationFrame(function() {
      var overflow = inner.scrollHeight - wpane.clientHeight;
      if (overflow > 20) {
        var dur = Math.max(8, Math.round(overflow / 20)) + 's';
        inner.style.setProperty('--scroll-dist', '-' + overflow + 'px');
        inner.style.setProperty('--scroll-dur', dur);
        inner.classList.add('scrolling');
      }
    });

  }

  function esc(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
</script>
</body>
</html>"""


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§12  ROUTES
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@app.route("/kiosk")
def index():
    return render_template_string(KIOSK_PAGE, TEST_KEY=TEST_KEY)

@app.route("/")
def root():
    return DISPLAY_PAGE


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§10  TEST FIXTURES
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
_test_active = {}   # type_key -> {"expires_at": float, "timer": threading.Timer}

# Real county centroids (lat, lon) per state â€” test polygons land here randomly each fire
_COUNTY_CENTROIDS = {
    "KS": [(37.69,-97.34),(38.88,-94.82),(37.34,-95.67),(39.02,-96.84),(39.05,-98.21),(37.82,-99.33),(37.05,-100.92),(38.48,-98.77)],
    "OK": [(35.47,-97.52),(36.15,-95.99),(35.22,-97.44),(35.53,-98.00),(34.60,-98.41),(35.88,-97.43),(36.08,-96.55),(34.87,-95.96)],
    "TX": [(32.78,-96.80),(32.45,-97.28),(33.22,-96.62),(33.59,-101.85),(30.27,-97.74),(29.76,-95.37),(28.70,-100.48),(35.17,-101.87)],
    "NE": [(40.81,-96.68),(41.26,-96.00),(41.57,-99.37),(41.99,-101.76),(40.52,-98.39),(42.03,-97.43),(40.37,-100.14),(41.40,-98.35)],
    "IA": [(41.59,-93.62),(42.03,-91.64),(41.52,-90.58),(42.50,-92.33),(42.03,-95.86),(41.00,-94.38),(43.38,-93.62),(42.49,-94.17)],
    "MO": [(39.10,-94.58),(38.63,-90.24),(37.21,-93.29),(37.95,-91.77),(38.95,-92.33),(36.62,-93.24),(38.07,-94.35),(37.07,-89.56)],
    "MN": [(44.97,-93.27),(44.02,-92.47),(45.60,-94.31),(46.87,-96.78),(47.52,-92.08),(48.51,-93.42),(45.13,-95.04),(44.05,-91.66)],
    "ND": [(46.87,-96.79),(46.81,-100.78),(47.92,-97.06),(48.25,-101.30),(46.94,-98.71),(47.55,-100.44),(46.23,-97.08),(46.39,-102.83)],
    "SD": [(43.55,-96.73),(44.37,-100.33),(43.89,-99.32),(44.87,-103.70),(45.46,-98.49),(43.19,-101.65),(44.07,-97.26),(45.35,-100.69)],
    "IL": [(41.84,-87.68),(41.75,-88.15),(42.28,-88.01),(41.30,-88.83),(39.80,-89.65),(40.10,-88.24),(40.70,-90.48),(37.73,-89.22)],
    "IN": [(39.77,-86.16),(41.49,-87.35),(41.07,-85.14),(40.06,-85.99),(40.41,-86.88),(39.16,-86.52),(41.68,-86.22),(38.29,-87.67)],
    "OH": [(40.00,-82.99),(41.50,-81.69),(39.96,-84.19),(41.12,-81.44),(39.38,-83.80),(39.76,-84.19),(39.10,-84.51),(41.39,-82.66)],
    "MI": [(42.33,-83.05),(42.58,-83.09),(43.02,-83.69),(42.97,-85.67),(42.73,-84.55),(43.56,-84.78),(42.29,-85.59),(42.10,-86.49)],
    "WI": [(43.04,-87.91),(43.07,-89.40),(44.52,-87.98),(43.05,-88.23),(42.73,-87.78),(44.25,-89.63),(43.80,-91.25),(45.13,-89.63)],
    "TN": [(36.17,-86.78),(35.15,-90.05),(36.54,-82.55),(35.96,-83.92),(35.58,-88.82),(36.34,-84.11),(35.46,-92.42),(35.06,-85.31)],
    "AR": [(34.74,-92.33),(35.37,-94.40),(33.44,-94.04),(35.33,-90.22),(36.35,-94.22),(34.51,-91.59),(33.65,-93.10),(35.93,-91.27)],
    "MS": [(32.30,-90.18),(34.31,-89.01),(30.42,-89.12),(33.76,-88.62),(32.64,-88.69),(34.83,-90.03),(31.34,-91.40),(33.25,-88.42)],
    "AL": [(33.52,-86.81),(32.36,-86.30),(30.69,-88.04),(34.73,-87.70),(34.16,-86.84),(32.74,-85.96),(31.22,-87.53),(34.80,-85.97)],
    "GA": [(33.75,-84.39),(33.03,-83.93),(32.08,-81.09),(30.83,-83.32),(34.47,-84.44),(33.57,-83.20),(32.46,-84.99),(31.58,-84.18)],
    "SC": [(34.00,-81.03),(34.85,-82.37),(33.90,-80.53),(33.44,-79.00),(32.78,-80.02),(34.20,-79.45),(34.58,-82.88),(33.06,-81.13)],
    "NC": [(35.78,-78.64),(35.23,-80.84),(36.07,-79.79),(35.52,-82.56),(34.23,-77.95),(36.40,-77.00),(35.33,-78.47),(36.08,-81.68)],
    "VA": [(37.54,-77.44),(36.85,-76.29),(37.25,-79.94),(38.03,-78.47),(37.39,-80.05),(36.87,-82.78),(38.87,-77.36),(37.79,-75.46)],
    "WV": [(38.35,-81.63),(39.33,-80.09),(37.40,-81.63),(38.80,-79.96),(39.63,-79.97),(38.00,-80.65),(40.08,-80.65),(37.66,-80.07)],
    "PA": [(40.44,-79.99),(39.95,-75.16),(41.17,-77.19),(40.27,-76.89),(42.12,-80.08),(41.41,-75.66),(41.24,-77.00),(40.78,-77.86)],
    "NY": [(42.65,-73.75),(40.71,-73.99),(43.04,-76.15),(42.09,-76.81),(44.69,-75.45),(41.09,-74.13),(42.88,-78.88),(43.99,-74.66)],
    "CO": [(39.74,-104.98),(38.27,-104.61),(40.48,-105.07),(37.68,-105.02),(39.59,-107.34),(40.09,-108.77),(39.06,-108.55),(37.28,-107.88)],
    "WY": [(41.14,-104.82),(44.80,-106.96),(42.86,-108.73),(43.04,-110.65),(41.79,-107.23),(44.28,-110.40),(41.56,-105.96),(43.86,-104.57)],
    "MT": [(46.60,-112.03),(47.50,-111.30),(48.56,-109.42),(46.88,-114.01),(47.50,-106.42),(46.17,-108.50),(48.77,-104.55),(45.78,-109.69)],
    "ID": [(43.62,-116.21),(43.48,-112.03),(42.57,-114.47),(46.73,-117.00),(47.69,-116.78),(42.93,-113.37),(44.48,-114.91),(48.23,-116.56)],
    "XX": [(37.69,-97.34),(35.47,-97.52),(38.88,-94.82),(41.59,-93.62),(40.81,-96.68),(39.10,-94.58)],
}
# Fallback for states not in the table â€” use the list below keyed by region
_STATE_CENTER_FALLBACK = {
    "AK":(64.2,-153.4),"AZ":(34.3,-111.1),"CA":(37.2,-119.4),"CT":(41.6,-72.7),
    "DE":(39.0,-75.5),"FL":(28.7,-82.5),"HI":(20.9,-157.0),"KY":(37.5,-85.3),
    "LA":(31.2,-91.8),"MD":(39.1,-76.8),"MA":(42.3,-71.8),"ME":(45.4,-69.0),
    "MN":(46.4,-93.2),"NV":(39.5,-116.9),"NH":(43.7,-71.6),"NJ":(40.1,-74.5),
    "NM":(34.4,-106.1),"OR":(44.1,-120.5),"RI":(41.7,-71.5),"UT":(39.4,-111.1),
    "VT":(44.0,-72.7),"WA":(47.4,-120.5),
}

def _test_polygon(area: str) -> dict:
    """Build a ~60-mile square polygon over a randomly chosen real county in the given state."""
    m = re.search(r'\b([A-Z]{2})\b', area or "")
    state = m.group(1) if m else "XX"
    if state in _COUNTY_CENTROIDS:
        lat, lon = random.choice(_COUNTY_CENTROIDS[state])
    else:
        lat, lon = _STATE_CENTER_FALLBACK.get(state, (37.0, -97.0))
    d = 0.50  # ~35 miles each side
    return {"type": "Polygon", "coordinates": [[[lon-d, lat-d],[lon+d, lat-d],
            [lon+d, lat+d],[lon-d, lat+d],[lon-d, lat-d]]]}

_FAKE_WARN_DEFS = {
    "spotter":   ("CONFIRMED - Spotter",         "TEST COUNTY, KS", "TEST â€” Tornado on the ground confirmed by trained spotter."),
    "emerg_mgr": ("CONFIRMED - Emerg. Mgr",      "TEST COUNTY, OK", "TEST â€” Tornado on the ground confirmed by emergency manager."),
    "law":       ("CONFIRMED - Law Enforcement", "TEST COUNTY, TX", "TEST â€” Tornado on the ground confirmed by law enforcement."),
    "media":     ("CONFIRMED - Media",           "TEST COUNTY, NE", "TEST â€” Tornado on the ground confirmed by broadcast media."),
    "nws":       ("CONFIRMED - NWS Survey",      "TEST COUNTY, IA", "TEST â€” Tornado confirmed by NWS storm survey team."),
    "public":    ("CONFIRMED - Public Report",   "TEST COUNTY, MO", "TEST â€” Tornado confirmed by trained public report."),
    "radar":     ("RADAR INDICATED",             "TEST COUNTY, XX", "TEST â€” Tornado indicated by radar. Do not wait to see or hear the tornado."),
}
_FAKE_WATCH_DEFS = {
    "watch":          ("TEST REGION, NE; TEST REGION, IA", False, "TEST â€” Tornado Watch in effect. Conditions favorable for tornado development."),
    "watch_upgraded": ("TEST REGION, OK; TEST REGION, KS", True,  "TEST â€” Tornado Watch â€” a Tornado Warning is now active in this watch area."),
}

# â”€â”€ Hurricane test scenarios â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_FAKE_STORM_DEFS = {
    "hur_ts": {"name": "TEST",  "classification": "TS", "category": "TS",
               "winds_mph": 60,  "winds_kt": 52,  "pressure": "1000",
               "lat": 27.0, "lon": -79.0, "lat_str": "27.0N", "lon_str": "79.0W",
               "movement_dir": 315, "movement_spd": 14, "basin": "AL",
               "color": "#44bbff", "last_update": "TEST"},
    "hur_c1": {"name": "TEST",  "classification": "HU", "category": "C1",
               "winds_mph": 85,  "winds_kt": 74,  "pressure": "985",
               "lat": 26.0, "lon": -79.5, "lat_str": "26.0N", "lon_str": "79.5W",
               "movement_dir": 310, "movement_spd": 12, "basin": "AL",
               "color": "#ffee44", "last_update": "TEST"},
    "hur_c3": {"name": "TEST",  "classification": "HU", "category": "C3",
               "winds_mph": 120, "winds_kt": 104, "pressure": "955",
               "lat": 25.5, "lon": -80.0, "lat_str": "25.5N", "lon_str": "80.0W",
               "movement_dir": 320, "movement_spd": 10, "basin": "AL",
               "color": "#ff6600", "last_update": "TEST"},
    "hur_c5": {"name": "TEST",  "classification": "HU", "category": "C5",
               "winds_mph": 165, "winds_kt": 143, "pressure": "895",
               "lat": 25.0, "lon": -80.5, "lat_str": "25.0N", "lon_str": "80.5W",
               "movement_dir": 325, "movement_spd": 8,  "basin": "AL",
               "color": "#cc44ff", "last_update": "TEST"},
}

# â”€â”€ Winter test scenarios â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_FAKE_WINTER_DEFS = {
    "wtr_blizzard": {"area": "TEST COUNTY, MN; TEST COUNTY, ND",
                     "event": "Blizzard Warning",       "severity": "BLIZZARD",
                     "headline": "TEST â€” Blizzard Warning until 6 PM CST. Travel is not advised.",
                     "tna": True,  "color": "#cc44ff", "text_color": "#fff", "geometry": None},
    "wtr_ice":      {"area": "TEST COUNTY, IL; TEST COUNTY, MO",
                     "event": "Ice Storm Warning",       "severity": "ICE_STORM",
                     "headline": "TEST â€” Ice Storm Warning until midnight. Travel is not advised.",
                     "tna": True,  "color": "#ff4444", "text_color": "#fff", "geometry": None},
    "wtr_wsw":      {"area": "TEST COUNTY, WI; TEST COUNTY, MI",
                     "event": "Winter Storm Warning",    "severity": "WSW",
                     "headline": "TEST â€” Winter Storm Warning. 8â€“12 inches of snow expected.",
                     "tna": False, "color": "#4488ff", "text_color": "#fff", "geometry": None},
    "wtr_watch":    {"area": "TEST COUNTY, IA; TEST COUNTY, KS",
                     "event": "Winter Storm Watch",      "severity": "WATCH",
                     "headline": "TEST â€” Winter Storm Watch in effect through tonight.",
                     "tna": False, "color": "#ffaa00", "text_color": "#000", "geometry": None},
    "wtr_advisory": {"area": "TEST COUNTY, OH; TEST COUNTY, IN",
                     "event": "Winter Weather Advisory", "severity": "ADVISORY",
                     "headline": "TEST â€” Winter Weather Advisory. 2â€“4 inches of snow expected.",
                     "tna": False, "color": "#aaccff", "text_color": "#000", "geometry": None},
}

_ALL_TEST_TYPES = (list(_FAKE_WARN_DEFS.keys()) + list(_FAKE_WATCH_DEFS.keys()) +
                   list(_FAKE_STORM_DEFS.keys()) + list(_FAKE_WINTER_DEFS.keys()))


def _inject_type(type_key: str, duration: int, custom_text):
    global _warnings, _watches, _storms, _winter_alerts
    now  = datetime.now().astimezone().strftime("%-I:%M %p %Z")
    sent = datetime.now(tz=timezone.utc).isoformat()

    # Cancel any existing timer for this slot
    with _lock:
        existing = _test_active.get(type_key)
        if existing:
            existing["timer"].cancel()

    if type_key in _FAKE_STORM_DEFS:
        fake = dict(_FAKE_STORM_DEFS[type_key])
        fake.update({"id": "TEST_" + type_key, "last_update": now,
                     "_test": True, "_test_type": type_key})
        with _hurricane_lock:
            _storms = [s for s in _storms if s.get("_test_type") != type_key]
            _storms = [fake] + _storms

    elif type_key in _FAKE_WINTER_DEFS:
        fake = dict(_FAKE_WINTER_DEFS[type_key])
        fake.update({"issued": now, "expires": "TEST", "expires_raw": "",
                     "sent_raw": sent, "_test": True, "_test_type": type_key,
                     "headline": custom_text or fake["headline"],
                     "geometry": _test_polygon(fake["area"])})   # fresh random county each fire
        with _winter_lock:
            _winter_alerts = [a for a in _winter_alerts if a.get("_test_type") != type_key]
            _winter_alerts = [fake] + _winter_alerts

    elif type_key in _FAKE_WATCH_DEFS:
        area, upgraded, default_text = _FAKE_WATCH_DEFS[type_key]
        fake = {"area": area, "issued": now, "expires": "TEST",
                "sent_raw": sent, "upgraded": upgraded,
                "_test": True, "_test_type": type_key,
                "headline": custom_text or default_text}
        with _lock:
            _watches = [w for w in _watches if w.get("_test_type") != type_key]
            _watches = [fake] + _watches

    else:  # tornado warning
        detection, area, default_text = _FAKE_WARN_DEFS[type_key]
        headline = custom_text or default_text
        fake = {"area": area, "issued": now, "expires": "TEST",
                "sent_raw": sent, "detection": detection,
                "_test": True, "_test_type": type_key,
                "headline": headline,
                "geometry": _test_polygon(area)}   # fresh random county each fire
        with _lock:
            _warnings = [w for w in _warnings if w.get("_test_type") != type_key]
            _warnings = [fake] + _warnings
        # Announce test warning to Vector immediately (bypasses the 60s poller)
        _vector_announce(
            event_type="tornado_warning",
            area=area,
            detection=detection,
            headline=headline,
        )

    def _remove():
        global _warnings, _watches, _storms, _winter_alerts
        if type_key in _FAKE_STORM_DEFS:
            with _hurricane_lock:
                _storms = [s for s in _storms if s.get("_test_type") != type_key]
        elif type_key in _FAKE_WINTER_DEFS:
            with _winter_lock:
                _winter_alerts = [a for a in _winter_alerts if a.get("_test_type") != type_key]
        else:
            with _lock:
                _warnings = [w for w in _warnings if w.get("_test_type") != type_key]
                _watches  = [w for w in _watches  if w.get("_test_type") != type_key]
        with _lock:
            _test_active.pop(type_key, None)
        log.info("TEST: removed %s", type_key)

    timer = threading.Timer(duration, _remove)
    timer.daemon = True
    timer.start()
    with _lock:
        _test_active[type_key] = {"expires_at": time.time() + duration, "timer": timer}
    log.info("TEST: injected %s for %ds", type_key, duration)


@app.route("/test")
def test_panel():
    if request.args.get("key") != TEST_KEY:
        return "Invalid key.", 403
    return render_template_string(TEST_PAGE, TEST_KEY=TEST_KEY)


@app.route("/test/fire")
def test_fire():
    if request.args.get("key") != TEST_KEY:
        return "Invalid key.", 403
    type_key    = request.args.get("type", "all")
    custom_text = request.args.get("text", "").strip() or None
    try:
        duration = max(5, min(3600, int(request.args.get("duration", 60))))
    except (ValueError, TypeError):
        duration = 60
    targets = _ALL_TEST_TYPES if type_key == "all" else [type_key]
    fired   = [t for t in targets if t in _ALL_TEST_TYPES]
    for t in fired:
        _inject_type(t, duration, custom_text)
    return json.dumps({"ok": True, "fired": fired, "duration": duration}), 200, {"Content-Type": "application/json"}


@app.route("/test/clear")
def test_clear():
    if request.args.get("key") != TEST_KEY:
        return "Invalid key.", 403
    global _warnings, _watches, _storms, _winter_alerts
    with _lock:
        for v in list(_test_active.values()):
            v["timer"].cancel()
        _test_active.clear()
        _warnings = [w for w in _warnings if not w.get("_test")]
        _watches  = [w for w in _watches  if not w.get("_test")]
    with _hurricane_lock:
        _storms = [s for s in _storms if not s.get("_test")]
    with _winter_lock:
        _winter_alerts = [a for a in _winter_alerts if not a.get("_test")]
    log.info("TEST: cleared all")
    return json.dumps({"ok": True}), 200, {"Content-Type": "application/json"}


@app.route("/test/status")
def test_status():
    if request.args.get("key") != TEST_KEY:
        return "Invalid key.", 403
    now = time.time()
    with _lock:
        active = [{"type": k, "expires_in": max(0, round(v["expires_at"] - now))}
                  for k, v in _test_active.items()]
    return json.dumps({"active": active}), 200, {"Content-Type": "application/json"}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#  TEST PANEL PAGE
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
TEST_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒªï¸</text></svg>">
<title>Test Panel â€” tornadowatch.org</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #060608; color: #fff; font-family: 'Segoe UI', Arial, sans-serif; min-height: 100vh; }
  a { color: #ff4444; text-decoration: none; }

  #topbar {
    background: #0a0a0f; border-bottom: 2px solid #880000;
    padding: 10px 24px; display: flex; align-items: center;
  }
  .title { font-size: 1.1rem; font-weight: 900; letter-spacing: 0.1em; text-transform: uppercase; color: #ff4444; }
  .sub   { font-size: 0.7rem; color: #555; margin-top: 2px; }

  #navbar {
    background: #0a0a0f; border-bottom: 1px solid #1a1a1a;
    display: flex; align-items: center; padding: 0 20px; height: 36px;
  }
  .nav-link {
    color: #555; text-decoration: none; font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase; padding: 0 16px;
    height: 100%; display: flex; align-items: center;
    border-bottom: 2px solid transparent; transition: color 0.15s;
  }
  .nav-link:hover  { color: #aaa; border-bottom-color: #444; }
  .nav-link.active { color: #ff4444; border-bottom-color: #cc0000; }
  .nav-divider { width: 1px; height: 16px; background: #1a1a1a; margin: 0 4px; }

  .page { max-width: 980px; margin: 0 auto; padding: 28px 24px; }

  /* Settings */
  .settings-row { display: flex; gap: 16px; margin-bottom: 28px; align-items: flex-end; flex-wrap: wrap; }
  .field { display: flex; flex-direction: column; gap: 6px; }
  .field label { font-size: 0.63rem; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: #555; }
  .field input {
    background: #0e0e14; border: 1px solid #2a2a3a; border-radius: 6px;
    color: #fff; font-size: 0.9rem; padding: 8px 12px; outline: none; transition: border-color 0.15s;
  }
  .field input:focus { border-color: #774; }
  .field input[type=number] { width: 90px; }
  .field input[type=text]   { width: 420px; }

  /* Section headers */
  .section-title {
    font-size: 0.65rem; font-weight: 800; letter-spacing: 0.12em; text-transform: uppercase;
    color: #444; margin-bottom: 12px; border-bottom: 1px solid #111; padding-bottom: 5px;
  }

  /* Type buttons */
  .btn-grid { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 24px; }
  .type-btn {
    background: #0e0e14; border: 1px solid #222; border-radius: 8px; color: #666;
    font-size: 0.82rem; font-weight: 700; padding: 10px 18px 8px; cursor: pointer;
    transition: all 0.15s; user-select: none; min-width: 128px;
    display: flex; flex-direction: column; align-items: center; gap: 2px;
  }
  .type-btn .btn-sub   { font-size: 0.58rem; letter-spacing: 0.08em; text-transform: uppercase; color: #444; }
  .type-btn .btn-timer { font-size: 0.6rem; min-height: 0.6rem; margin-top: 2px; }
  .type-btn:hover { background: #151520; border-color: #333; color: #ccc; }
  .type-btn:hover .btn-sub { color: #666; }

  /* Active button states */
  .type-btn.act-confirmed {
    background: #1e0000; border-color: #880000; color: #ff5555;
    box-shadow: 0 0 16px 2px #ff000030;
    animation: pulse-red 1.4s ease-in-out infinite;
  }
  .type-btn.act-confirmed .btn-sub   { color: #aa3333; }
  .type-btn.act-confirmed .btn-timer { color: #dd4444; }
  .type-btn.act-radar {
    background: #1a0e00; border-color: #885500; color: #ff9922;
    box-shadow: 0 0 14px 2px #ff880025;
  }
  .type-btn.act-radar .btn-sub   { color: #885500; }
  .type-btn.act-radar .btn-timer { color: #cc7700; }
  .type-btn.act-watch {
    background: #110e00; border-color: #554400; color: #ffbb22;
    box-shadow: 0 0 12px 2px #ffaa0020;
  }
  .type-btn.act-watch .btn-sub   { color: #554400; }
  .type-btn.act-watch .btn-timer { color: #aa8800; }
  .type-btn.act-hurricane {
    background: #001428; border-color: #0055aa; color: #44aaff;
    box-shadow: 0 0 14px 2px #0055aa35;
  }
  .type-btn.act-hurricane .btn-sub   { color: #003377; }
  .type-btn.act-hurricane .btn-timer { color: #3399cc; }
  .type-btn.act-winter {
    background: #001220; border-color: #006699; color: #44ccff;
    box-shadow: 0 0 14px 2px #44ccff25;
  }
  .type-btn.act-winter .btn-sub   { color: #003355; }
  .type-btn.act-winter .btn-timer { color: #22aacc; }
  @keyframes pulse-red {
    0%,100% { box-shadow: 0 0 16px 2px #ff000030; }
    50%      { box-shadow: 0 0 28px 6px #ff000060; }
  }

  .s-chip.hurricane { background: #001428; border-color: #0055aa; color: #44aaff; }
  .s-chip.winter    { background: #001220; border-color: #006699; color: #44ccff; }

  /* Action buttons */
  .action-row { display: flex; gap: 12px; margin-bottom: 32px; align-items: center; }
  .btn-send-all {
    background: #cc0000; border: none; border-radius: 8px; color: #fff;
    font-size: 0.85rem; font-weight: 800; letter-spacing: 0.1em; text-transform: uppercase;
    padding: 12px 32px; cursor: pointer; transition: background 0.15s;
  }
  .btn-send-all:hover { background: #ee0000; }
  .btn-clear {
    background: #111; border: 1px solid #333; border-radius: 8px; color: #666;
    font-size: 0.85rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
    padding: 12px 24px; cursor: pointer; transition: all 0.15s;
  }
  .btn-clear:hover { background: #1a1a1a; color: #bbb; border-color: #555; }

  /* Status */
  #status-area { background: #0a0a0f; border: 1px solid #1a1a1a; border-radius: 8px; padding: 14px 18px; min-height: 56px; }
  #status-list { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
  .s-chip {
    background: #1a0000; border: 1px solid #550000; border-radius: 20px;
    padding: 4px 14px; font-size: 0.72rem; color: #ff6666;
    display: flex; align-items: center; gap: 6px;
  }
  .s-chip.watch { background: #130e00; border-color: #554400; color: #ffbb44; }
  .s-chip .chip-t { font-weight: 800; }
  #status-empty { color: #333; font-size: 0.8rem; font-style: italic; }
</style>
</head>
<body>

<div id="topbar">
  <div>
    <div class="title">&#x26A0; Test Panel</div>
    <div class="sub">tornadowatch.org &mdash; Alert injection console</div>
  </div>
</div>

<div id="navbar">
  <a href="/" class="nav-link">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link">&#x1F5FA; Map</a>
  <div class="nav-divider"></div>
  <a href="/test?key={{ TEST_KEY }}" class="nav-link active">&#x26A0; Test Panel</a>
</div>

<div class="page">

  <div class="settings-row">
    <div class="field">
      <label>Duration (seconds)</label>
      <input type="number" id="inp-duration" value="60" min="5" max="3600">
    </div>
    <div class="field" style="flex:1">
      <label>Custom Headline &mdash; leave blank to use default text</label>
      <input type="text" id="inp-text" placeholder="e.g. TORNADO ON THE GROUND near Greensburg moving NE at 35 mph...">
    </div>
  </div>

  <div class="section-title">Warnings</div>
  <div class="btn-grid">
    <button class="type-btn" data-type="spotter"   data-cat="confirmed" onclick="fire('spotter')">
      <span class="btn-sub">Confirmed</span>Spotter<span class="btn-timer" id="t-spotter"></span>
    </button>
    <button class="type-btn" data-type="emerg_mgr" data-cat="confirmed" onclick="fire('emerg_mgr')">
      <span class="btn-sub">Confirmed</span>Emerg. Mgr<span class="btn-timer" id="t-emerg_mgr"></span>
    </button>
    <button class="type-btn" data-type="law"       data-cat="confirmed" onclick="fire('law')">
      <span class="btn-sub">Confirmed</span>Law Enforcement<span class="btn-timer" id="t-law"></span>
    </button>
    <button class="type-btn" data-type="media"     data-cat="confirmed" onclick="fire('media')">
      <span class="btn-sub">Confirmed</span>Media<span class="btn-timer" id="t-media"></span>
    </button>
    <button class="type-btn" data-type="nws"       data-cat="confirmed" onclick="fire('nws')">
      <span class="btn-sub">Confirmed</span>NWS Survey<span class="btn-timer" id="t-nws"></span>
    </button>
    <button class="type-btn" data-type="public"    data-cat="confirmed" onclick="fire('public')">
      <span class="btn-sub">Confirmed</span>Public Report<span class="btn-timer" id="t-public"></span>
    </button>
    <button class="type-btn" data-type="radar"     data-cat="radar"     onclick="fire('radar')">
      <span class="btn-sub">Detection</span>Radar Indicated<span class="btn-timer" id="t-radar"></span>
    </button>
  </div>

  <div class="section-title">Watches</div>
  <div class="btn-grid">
    <button class="type-btn" data-type="watch"          data-cat="watch" onclick="fire('watch')">
      <span class="btn-sub">Watch</span>Standard<span class="btn-timer" id="t-watch"></span>
    </button>
    <button class="type-btn" data-type="watch_upgraded" data-cat="watch" onclick="fire('watch_upgraded')">
      <span class="btn-sub">Watch</span>&#x26A0; Upgraded<span class="btn-timer" id="t-watch_upgraded"></span>
    </button>
  </div>

  <div class="section-title">&#x1F300; Hurricanes</div>
  <div class="btn-grid">
    <button class="type-btn" data-type="hur_ts"  data-cat="hurricane" onclick="fire('hur_ts')">
      <span class="btn-sub">Tropical</span>Storm (TS)<span class="btn-timer" id="t-hur_ts"></span>
    </button>
    <button class="type-btn" data-type="hur_c1"  data-cat="hurricane" onclick="fire('hur_c1')">
      <span class="btn-sub">Category 1</span>Hurricane<span class="btn-timer" id="t-hur_c1"></span>
    </button>
    <button class="type-btn" data-type="hur_c3"  data-cat="hurricane" onclick="fire('hur_c3')">
      <span class="btn-sub">Category 3</span>Hurricane<span class="btn-timer" id="t-hur_c3"></span>
    </button>
    <button class="type-btn" data-type="hur_c5"  data-cat="hurricane" onclick="fire('hur_c5')">
      <span class="btn-sub">Category 5</span>Hurricane<span class="btn-timer" id="t-hur_c5"></span>
    </button>
  </div>

  <div class="section-title">&#x1F328; Winter Storms</div>
  <div class="btn-grid">
    <button class="type-btn" data-type="wtr_blizzard"  data-cat="winter" onclick="fire('wtr_blizzard')">
      <span class="btn-sub">&#x1F6AB; TNA</span>Blizzard Warning<span class="btn-timer" id="t-wtr_blizzard"></span>
    </button>
    <button class="type-btn" data-type="wtr_ice"       data-cat="winter" onclick="fire('wtr_ice')">
      <span class="btn-sub">&#x1F6AB; TNA</span>Ice Storm Warning<span class="btn-timer" id="t-wtr_ice"></span>
    </button>
    <button class="type-btn" data-type="wtr_wsw"       data-cat="winter" onclick="fire('wtr_wsw')">
      <span class="btn-sub">Warning</span>Winter Storm Warn.<span class="btn-timer" id="t-wtr_wsw"></span>
    </button>
    <button class="type-btn" data-type="wtr_watch"     data-cat="winter" onclick="fire('wtr_watch')">
      <span class="btn-sub">Watch</span>Winter Storm Watch<span class="btn-timer" id="t-wtr_watch"></span>
    </button>
    <button class="type-btn" data-type="wtr_advisory"  data-cat="winter" onclick="fire('wtr_advisory')">
      <span class="btn-sub">Advisory</span>Winter Advisory<span class="btn-timer" id="t-wtr_advisory"></span>
    </button>
  </div>

  <div class="action-row">
    <button class="btn-send-all" onclick="fire('all')">&#x26A0; Send All</button>
    <button class="btn-clear"    onclick="clearAll()">&#x2715; Clear All</button>
  </div>

  <div class="section-title">Currently Active</div>
  <div id="status-area">
    <span id="status-empty">No test alerts active.</span>
    <div id="status-list" style="display:none"></div>
  </div>

</div>

<script>
  var KEY = '{{ TEST_KEY }}';

  var CAT = {
    spotter:'confirmed', emerg_mgr:'confirmed', law:'confirmed',
    media:'confirmed',   nws:'confirmed',        public:'confirmed',
    radar:'radar',
    watch:'watch',       watch_upgraded:'watch',
    hur_ts:'hurricane',  hur_c1:'hurricane',  hur_c3:'hurricane',  hur_c5:'hurricane',
    wtr_blizzard:'winter', wtr_ice:'winter', wtr_wsw:'winter',
    wtr_watch:'winter',    wtr_advisory:'winter'
  };
  var LABEL = {
    spotter:'Spotter', emerg_mgr:'Emerg. Mgr', law:'Law Enf.',
    media:'Media',     nws:'NWS Survey',        public:'Public',
    radar:'Radar Indicated',
    watch:'Standard Watch', watch_upgraded:'Watch (Upgraded)',
    hur_ts:'Tropical Storm', hur_c1:'Hurricane C1', hur_c3:'Hurricane C3', hur_c5:'Hurricane C5',
    wtr_blizzard:'Blizzard Warn.', wtr_ice:'Ice Storm Warn.', wtr_wsw:'Winter Storm Warn.',
    wtr_watch:'Winter Storm Watch', wtr_advisory:'Winter Advisory'
  };

  function fmtSecs(s) {
    return s >= 60 ? Math.floor(s/60) + 'm ' + (s%60) + 's' : s + 's';
  }

  function fire(type) {
    var dur = parseInt(document.getElementById('inp-duration').value) || 60;
    var txt = document.getElementById('inp-text').value.trim();
    var url = '/test/fire?key=' + KEY + '&type=' + type + '&duration=' + dur;
    if (txt) url += '&text=' + encodeURIComponent(txt);
    fetch(url).then(function(r){ return r.json(); }).catch(function(){});
  }

  function clearAll() {
    fetch('/test/clear?key=' + KEY).then(function(r){ return r.json(); }).catch(function(){});
  }

  function updateUI(active) {
    // Buttons
    document.querySelectorAll('.type-btn').forEach(function(btn) {
      var type  = btn.dataset.type;
      var cat   = btn.dataset.cat;
      var entry = active.find(function(a){ return a.type === type; });
      var timer = document.getElementById('t-' + type);
      btn.className = 'type-btn';
      if (entry) {
        btn.classList.add('act-' + cat);
        if (timer) timer.textContent = fmtSecs(entry.expires_in);
      } else {
        if (timer) timer.textContent = '';
      }
    });
    // Status chips
    var list  = document.getElementById('status-list');
    var empty = document.getElementById('status-empty');
    if (!active.length) {
      list.style.display  = 'none';
      empty.style.display = '';
      return;
    }
    empty.style.display = 'none';
    list.style.display  = 'flex';
    list.innerHTML = active.map(function(a) {
      var cat = CAT[a.type] || '';
      var cls = cat === 'watch' ? 's-chip watch' :
                cat === 'hurricane' ? 's-chip hurricane' :
                cat === 'winter'    ? 's-chip winter' : 's-chip';
      return '<div class="' + cls + '">' + (LABEL[a.type]||a.type) +
             ' <span class="chip-t">' + fmtSecs(a.expires_in) + '</span></div>';
    }).join('');
  }

  function poll() {
    fetch('/test/status?key=' + KEY)
      .then(function(r){ return r.json(); })
      .then(function(d){ updateUI(d.active || []); })
      .catch(function(){});
  }

  poll();
  setInterval(poll, 1000);
</script>
<style>.home-btn{position:fixed;bottom:14px;right:14px;z-index:200;background:#111;border:1px solid #2a2a2a;color:#555;font-size:.72rem;padding:5px 13px;border-radius:5px;text-decoration:none;opacity:.6;transition:opacity .2s}.home-btn:hover{opacity:1;color:#aaa;border-color:#555}</style>
<a class="home-btn" href="http://localhost:8080/">Home</a>
</body>
</html>"""


MAP_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒªï¸</text></svg>">
<title>Storm Map â€” tornadowatch.org</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { height: 100%; background: #060608; color: #fff; font-family: 'Segoe UI', Arial, sans-serif; display: flex; flex-direction: column; }
  #navbar {
    background: #0a0a0f; border-bottom: 1px solid #1a1a1a;
    display: flex; align-items: center; padding: 0 20px; height: 36px; flex-shrink: 0;
  }
  .nav-link {
    color: #555; text-decoration: none; font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase; padding: 0 16px;
    height: 100%; display: flex; align-items: center;
    border-bottom: 2px solid transparent; transition: color 0.15s;
  }
  .nav-link:hover  { color: #aaa; border-bottom-color: #444; }
  .nav-link.active { color: #ff4444; border-bottom-color: #cc0000; }
  .nav-divider { width: 1px; height: 16px; background: #1a1a1a; margin: 0 4px; }
  #map { flex: 1; min-height: 0; }
  #map-status {
    position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
    background: #111118ee; border: 1px solid #333; border-radius: 20px;
    padding: 6px 18px; font-size: 0.72rem; color: #ccc; pointer-events: none; z-index: 500;
    white-space: nowrap; display: flex; align-items: center; gap: 10px;
  }
  #map-clock { color: #888; font-variant-numeric: tabular-nums; }
  /* Layer toggle panel */
  #layer-control {
    position: fixed; top: 56px; right: 12px; z-index: 600;
    background: #111118ee; border: 1px solid #2a2a3a; border-radius: 8px;
    padding: 10px 14px; display: flex; flex-direction: column; gap: 6px; min-width: 148px;
    backdrop-filter: blur(4px);
  }
  #layer-control .lc-title {
    font-size: 0.6rem; color: #444; letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 2px;
  }
  .layer-btn {
    display: flex; align-items: center; gap: 7px;
    padding: 5px 10px; border-radius: 20px; border: 1px solid transparent;
    font-size: 0.75rem; font-weight: 700; letter-spacing: 0.04em;
    cursor: pointer; background: transparent; color: #ccc; width: 100%;
    transition: opacity 0.2s, border-color 0.2s;
  }
  .layer-btn .dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
  .layer-btn.on-tornado { border-color: #ff4444; color: #ff8888; background: rgba(255,68,68,0.1); }
  .layer-btn.on-winter  { border-color: #44ccff; color: #88ddff; background: rgba(68,204,255,0.1); }
  .layer-btn.layer-off  { opacity: 0.35; border-color: #2a2a3a; color: #555; background: transparent; }
  /* Leaflet popup dark override */
  .leaflet-popup-content-wrapper { background: #0e0e14; color: #fff; border: 1px solid #2a2a3a; border-radius: 6px; }
  .leaflet-popup-tip { background: #0e0e14; }
  .leaflet-popup-content { font-size: 0.8rem; line-height: 1.5; }
  .pop-event { font-size: 0.65rem; font-weight: 800; letter-spacing: 0.08em; }
  .pop-area  { font-weight: 700; margin: 2px 0; }
  .pop-exp   { font-size: 0.68rem; color: #888; }
  .pop-tna   { display:inline-block; background:#ff2222; color:#fff; border-radius:3px; padding:1px 6px; font-size:0.65rem; font-weight:800; margin-top:3px; }
  .pop-sev   { display:inline-block; border-radius:3px; padding:2px 7px; font-size:0.65rem; font-weight:800; letter-spacing:0.06em; text-transform:uppercase; margin-bottom:4px; }
</style>
</head>
<body>
<div id="navbar">
  <a href="/" class="nav-link">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link active">&#x1F5FA; Map</a>
</div>

<div id="map"></div>
<div id="map-status">
  <span id="map-alert-status">Loading...</span>
  <span style="color:#444">&middot;</span>
  <span id="map-clock">--:-- --</span>
</div>

<div id="layer-control">
  <div class="lc-title">Layers</div>
  <button class="layer-btn on-tornado" id="btn-tornado" onclick="toggleLayer('tornado')">
    <span class="dot" style="background:#ff4444"></span>&#x1F32A; Tornadoes
  </button>
  <button class="layer-btn on-winter" id="btn-winter" onclick="toggleLayer('winter')">
    <span class="dot" style="background:#44ccff"></span>&#x1F328; Winter
  </button>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  var map = L.map('map', { zoomControl: true }).setView([38.5, -96], 4);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd', maxZoom: 19
  }).addTo(map);

  var tornadoLayer = L.layerGroup().addTo(map);
  var winterLayer  = L.layerGroup().addTo(map);
  var layerOn = { tornado: true, winter: true };
  var tornadoCount = 0, winterCount = 0;

  var WINTER_COLORS = {
    'BLIZZARD':'#cc44ff','ICE_STORM':'#ff4444','WSW':'#4488ff',
    'SNOW_WARN':'#44ccff','WIND_WARN':'#ff8c00','WATCH':'#ffaa00','ADVISORY':'#aaccff'
  };
  var WINTER_TEXT = {
    'BLIZZARD':'#fff','ICE_STORM':'#fff','WSW':'#fff',
    'SNOW_WARN':'#000','WIND_WARN':'#fff','WATCH':'#000','ADVISORY':'#000'
  };

  function toggleLayer(which) {
    layerOn[which] = !layerOn[which];
    var btn = document.getElementById('btn-' + which);
    if (layerOn[which]) {
      btn.className = 'layer-btn on-' + which;
      if (which === 'tornado') tornadoLayer.addTo(map);
      else                     winterLayer.addTo(map);
    } else {
      btn.className = 'layer-btn layer-off';
      map.removeLayer(which === 'tornado' ? tornadoLayer : winterLayer);
    }
  }

  function updateStatus() {
    var tc = tornadoCount, wc = winterCount;
    var msg = '';
    if (tc === 0 && wc === 0) {
      msg = '&#x2714; No active warnings';
    } else {
      if (tc > 0) msg += '&#x1F32A; ' + tc + ' tornado' + (tc !== 1 ? 's' : '');
      if (tc > 0 && wc > 0) msg += '  &middot;  ';
      if (wc > 0) msg += '&#x1F328; ' + wc + ' winter alert' + (wc !== 1 ? 's' : '');
    }
    document.getElementById('map-alert-status').innerHTML = msg;
  }

  function tick() {
    document.getElementById('map-clock').textContent =
      new Date().toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit', second:'2-digit'});
  }

  function escHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  /* â”€â”€ TORNADO LAYER â”€â”€ */
  function loadWarnings() {
    fetch('/map-data')
      .then(function(r){ return r.json(); })
      .then(function(geojson) {
        tornadoLayer.clearLayers();
        tornadoCount = 0;
        var noPolygon = 0;
        (geojson.features || []).forEach(function(f) {
          var p   = f.properties || {};
          var det = (p.parameters && p.parameters.tornadoDetection)
                    ? p.parameters.tornadoDetection[0] : '';
          var confirmed = det && det.toUpperCase().indexOf('CONFIRM') !== -1;
          var color = confirmed ? '#ff2222' : '#ff8c00';
          if (f.geometry) {
            L.geoJSON(f, { style: { color:color, weight:2, fillColor:color, fillOpacity:0.18, opacity:0.85 } })
             .bindPopup(
               '<div class="pop-event" style="color:' + color + '">' + (det || 'RADAR INDICATED') + '</div>' +
               '<div class="pop-area">' + escHtml(p.areaDesc || '') + '</div>' +
               '<div class="pop-exp">Expires: ' + (p.expires ? new Date(p.expires).toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',timeZoneName:'short'}) : 'â€”') + '</div>'
             ).addTo(tornadoLayer);
            tornadoCount++;
          } else { noPolygon++; }
        });
        updateStatus();
      })
      .catch(function(){ document.getElementById('map-status').textContent = 'Failed to load tornado data'; });
  }

  /* â”€â”€ WINTER LAYER â”€â”€ */
  function loadWinter() {
    fetch('/winter-geo')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        winterLayer.clearLayers();
        winterCount = 0;
        (data.alerts || []).forEach(function(a) {
          if (!a.geometry) return;
          var color  = WINTER_COLORS[a.severity] || '#aaccff';
          var tcolor = WINTER_TEXT[a.severity]   || '#000';
          var tnaBadge = a.tna ? '<br><span class="pop-tna">&#x1F6AB; TRAVEL NOT ADVISED</span>' : '';
          L.geoJSON(a.geometry, { style: { color:color, weight:2, fillColor:color, fillOpacity:0.18, opacity:0.85 } })
           .bindPopup(
             '<span class="pop-sev" style="background:' + color + ';color:' + tcolor + '">' + escHtml(a.severity) + '</span>' +
             '<div class="pop-event" style="color:' + color + '">' + escHtml(a.event) + '</div>' +
             '<div class="pop-area">' + escHtml(a.area) + '</div>' +
             '<div class="pop-exp">Expires: ' + escHtml(a.expires) + '</div>' +
             tnaBadge
           ).addTo(winterLayer);
          winterCount++;
        });
        updateStatus();
      })
      .catch(function(){ console.warn('winter-geo failed'); });
  }

  loadWarnings();
  loadWinter();
  setInterval(loadWarnings, 8000);
  setInterval(loadWinter,   8000);

  tick();
  setInterval(tick, 1000);

  // Auto-reload when server restarts
  (function(){ var s=null; setInterval(function(){
    fetch('/health').then(function(r){return r.json();}).then(function(d){
      if(!s){s=d.server_start;return;} if(d.server_start!==s)location.reload();
    }).catch(function(){});
  }, 30000); })();
</script>
<style>.home-btn{position:fixed;bottom:14px;right:14px;z-index:200;background:#111;border:1px solid #2a2a2a;color:#555;font-size:.72rem;padding:5px 13px;border-radius:5px;text-decoration:none;opacity:.6;transition:opacity .2s}.home-btn:hover{opacity:1;color:#aaa;border-color:#555}</style>
<a class="home-btn" href="http://localhost:8080/">Home</a>
</body>
</html>"""


WINTER_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒ¨ï¸</text></svg>">
<title>Winter Storm Tracker â€” tornadowatch.org</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #05080f; color: #e0eeff;
    font-family: 'Segoe UI', Arial, sans-serif;
    height: 100vh; display: flex; flex-direction: column; overflow: hidden;
  }

  /* â”€â”€ TOPBAR â”€â”€ */
  #topbar { display: flex; align-items: stretch; border-bottom: 3px solid #0066aa; flex-shrink: 0; }
  #branding {
    background: #004c88; padding: 10px 24px;
    display: flex; flex-direction: column; justify-content: center; flex-shrink: 0;
  }
  #branding .title { font-size: 1.1rem; font-weight: 900; letter-spacing: 0.12em; text-transform: uppercase; }
  #branding .sub   { font-size: 0.65rem; letter-spacing: 0.1em; color: #99ccee; margin-top: 1px; }
  .stat-box {
    background: #060e1a; padding: 10px 20px;
    display: flex; align-items: center; gap: 10px;
    border-right: 1px solid #0a2240; flex-shrink: 0;
  }
  .stat-box .big-num { font-size: 3.2rem; font-weight: 900; color: #44ccff; line-height: 1; }
  .stat-box .big-lbl { font-size: 0.72rem; color: #4a7a9b; text-transform: uppercase; letter-spacing: 0.08em; line-height: 1.4; }
  .stat-box .sm-num  { font-size: 2rem; font-weight: 900; line-height: 1; }
  .stat-box .sm-lbl  { font-size: 0.65rem; color: #4a7a9b; text-transform: uppercase; letter-spacing: 0.07em; margin-top: 2px; }
  .stat-box.blizzard  .sm-num { color: #cc44ff; }
  .stat-box.ice       .sm-num { color: #ff4444; }
  .stat-box.wsw       .sm-num { color: #4488ff; }
  .stat-box.watches   .sm-num { color: #ffaa00; }
  .stat-box.states    .sm-num { color: #22cc88; }
  #topbar-right {
    margin-left: auto; display: flex; flex-direction: column;
    align-items: flex-end; justify-content: center;
    padding: 10px 20px; gap: 4px; flex-shrink: 0;
  }
  #clock     { font-size: 1.5rem; font-weight: 800; color: #44ccff; letter-spacing: 0.04em; font-variant-numeric: tabular-nums; }
  #poll-time { font-size: 0.7rem; color: #2a4a6a; }

  /* â”€â”€ NAV â”€â”€ */
  #navbar {
    background: #060e1a; border-bottom: 1px solid #0a2240;
    display: flex; align-items: center; padding: 0 20px; height: 36px; flex-shrink: 0;
  }
  .nav-link {
    color: #2a4a6a; text-decoration: none; font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase; padding: 0 14px;
    height: 100%; display: flex; align-items: center;
    border-bottom: 2px solid transparent; transition: color 0.15s;
  }
  .nav-link:hover  { color: #88bbdd; border-bottom-color: #1a3a5a; }
  .nav-link.active { color: #44ccff; border-bottom-color: #0066aa; }
  .nav-divider { width: 1px; height: 16px; background: #0a2240; margin: 0 4px; }

  /* â”€â”€ TICKER â”€â”€ */
  #ticker-wrap {
    background: #030810; border-bottom: 1px solid #0a2240;
    height: 28px; display: flex; align-items: center; overflow: hidden; flex-shrink: 0;
  }
  #ticker-label {
    background: #0066aa; color: #fff; font-size: 0.7rem; font-weight: 800;
    letter-spacing: 0.08em; padding: 0 14px; height: 100%;
    display: flex; align-items: center; white-space: nowrap; flex-shrink: 0;
  }
  #ticker-track { flex: 1; overflow: hidden; position: relative; height: 100%; }
  #ticker-inner {
    position: absolute; white-space: nowrap; display: flex; align-items: center;
    height: 100%; font-size: 0.76rem; color: #5599bb; letter-spacing: 0.03em;
    animation: ticker-run 60s linear infinite;
  }
  @keyframes ticker-run { 0% { transform: translateX(100vw); } 100% { transform: translateX(-100%); } }

  /* â”€â”€ CARDS GRID â”€â”€ */
  #page { flex: 1; overflow-y: auto; padding: 16px 20px; }
  #alert-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 14px;
  }
  .alert-card {
    background: #070f1c; border: 1px solid #0a2240; border-left: 5px solid #44ccff;
    border-radius: 8px; padding: 14px 16px; position: relative;
    transition: box-shadow 0.2s, background 0.15s;
    cursor: pointer;
  }
  .alert-card:hover { box-shadow: 0 0 16px rgba(68, 204, 255, 0.18); background: #091525; }

  /* â”€â”€ Alert detail modal â”€â”€ */
  #alert-modal {
    display: none; position: fixed; inset: 0; z-index: 600;
    background: #000000bb; align-items: center; justify-content: center;
  }
  #alert-modal.open { display: flex; }
  #alert-modal-box {
    background: #06101e; border: 2px solid #0066aa; border-radius: 10px;
    max-width: 620px; width: 92%; padding: 22px 26px 20px;
    max-height: 80vh; overflow-y: auto; position: relative;
  }
  #alert-modal-close {
    position: absolute; top: 10px; right: 14px;
    background: none; border: none; color: #44ccff;
    font-size: 1.4rem; cursor: pointer; line-height: 1;
  }
  #alert-modal-badge { font-size: 0.65rem; font-weight: 900; letter-spacing: 0.1em;
    text-transform: uppercase; margin-bottom: 6px; }
  #alert-modal-event { font-size: 0.72rem; color: #4a7a9b; letter-spacing: 0.05em;
    text-transform: uppercase; margin-bottom: 8px; }
  #alert-modal-area  { font-size: 1rem; font-weight: 700; color: #cce6ff; margin-bottom: 10px; line-height: 1.4; }
  #alert-modal-tna   { display:none; background:#cc0000; color:#fff; border-radius:4px;
    padding:5px 12px; margin-bottom:10px; font-size:0.75rem; font-weight:900;
    letter-spacing:0.06em; text-transform:uppercase; text-align:center; border:1px solid #ff4444; }
  #alert-modal-headline { font-size: 0.82rem; color: #8ab0cc; line-height: 1.65; margin-bottom: 14px; }
  #alert-modal-times { display: flex; gap: 24px; font-size: 0.72rem; }
  #alert-modal-times .mt-lbl { color: #1a3a5a; font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.06em; }
  #alert-modal-times .mt-val { color: #6699bb; }
  .card-top { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 8px; }
  .sev-badge {
    display: inline-block; padding: 3px 9px; border-radius: 4px;
    font-size: 0.66rem; font-weight: 800; letter-spacing: 0.07em;
    text-transform: uppercase; white-space: nowrap; flex-shrink: 0;
  }
  .tna-badge {
    width: 100%; background: #cc0000; color: #fff;
    border-radius: 4px; padding: 4px 10px; margin-bottom: 9px;
    font-size: 0.72rem; font-weight: 900; letter-spacing: 0.06em;
    text-transform: uppercase; text-align: center; border: 1px solid #ff4444;
  }
  .card-area     { font-size: 0.92rem; font-weight: 600; color: #cce6ff; margin-bottom: 5px; line-height: 1.3; }
  .card-headline { font-size: 0.79rem; color: #4a7a9b; margin-bottom: 9px; line-height: 1.4; }
  .card-times    { display: flex; gap: 18px; font-size: 0.7rem; }
  .card-times .t-lbl { color: #1a3a5a; font-size: 0.62rem; letter-spacing: 0.06em; text-transform: uppercase; }
  .card-times .t-val { color: #6699bb; }

  /* â”€â”€ EMPTY â”€â”€ */
  #empty {
    display: none; text-align: center; padding: 80px 20px;
    color: #1a3a5a; grid-column: 1/-1;
  }
  #empty .e-icon { font-size: 4rem; margin-bottom: 16px; }
  #empty .e-msg  { font-size: 1.1rem; color: #2a5a7a; }
</style>
</head>
<body>

<div id="topbar">
  <div id="branding">
    <div class="title">&#x1F328; WINTER STORM TRACKER</div>
    <div class="sub">tornadowatch.org &mdash; NWS Active Winter Alerts</div>
  </div>
  <div class="stat-box">
    <div id="alert-count" class="big-num">â€”</div>
    <div class="big-lbl">Active<br>Alerts</div>
  </div>
  <div class="stat-box blizzard">
    <div><div id="cnt-blizzard" class="sm-num">â€”</div><div class="sm-lbl">Blizzards</div></div>
  </div>
  <div class="stat-box ice">
    <div><div id="cnt-ice" class="sm-num">â€”</div><div class="sm-lbl">Ice Storm<br>Warnings</div></div>
  </div>
  <div class="stat-box wsw">
    <div><div id="cnt-wsw" class="sm-num">â€”</div><div class="sm-lbl">Winter Storm<br>Warnings</div></div>
  </div>
  <div class="stat-box watches">
    <div><div id="cnt-watch" class="sm-num">â€”</div><div class="sm-lbl">Watches</div></div>
  </div>
  <div class="stat-box states">
    <div><div id="cnt-states" class="sm-num">â€”</div><div class="sm-lbl">States<br>Affected</div></div>
  </div>
  <div id="topbar-right">
    <div id="clock">--:-- --</div>
    <div id="poll-time">Connecting...</div>
  </div>
</div>

<div id="navbar">
  <a href="/" class="nav-link">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link active">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link">&#x1F5FA; Map</a>
</div>

<div id="ticker-wrap">
  <div id="ticker-label">&#x2744; WINTER ALERTS</div>
  <div id="ticker-track"><div id="ticker-inner">No active winter weather alerts.</div></div>
</div>

<div id="page">
  <div id="alert-grid">
    <div id="empty">
      <div class="e-icon">&#x2744;&#xFE0F;</div>
      <div class="e-msg">No active winter weather alerts</div>
    </div>
  </div>
</div>

<!-- Alert detail modal -->
<div id="alert-modal" onclick="closeAlertModal(event)">
  <div id="alert-modal-box">
    <button id="alert-modal-close" onclick="closeAlertModal(null,true)">&#x2715;</button>
    <div id="alert-modal-badge"></div>
    <div id="alert-modal-event"></div>
    <div id="alert-modal-area"></div>
    <div id="alert-modal-tna">&#x1F6AB; TRAVEL NOT ADVISED</div>
    <div id="alert-modal-headline"></div>
    <div id="alert-modal-times">
      <div><div class="mt-lbl">Issued</div><div class="mt-val" id="alert-modal-issued"></div></div>
      <div><div class="mt-lbl">Expires</div><div class="mt-val" id="alert-modal-expires"></div></div>
    </div>
  </div>
</div>

<script>
  var SEV_ORDER  = ['BLIZZARD','ICE_STORM','WSW','SNOW_WARN','WIND_WARN','WATCH','ADVISORY'];
  var SEV_LABELS = {
    'BLIZZARD':'Blizzard Warning','ICE_STORM':'Ice Storm Warning',
    'WSW':'Winter Storm Warning','SNOW_WARN':'Snow Warning',
    'WIND_WARN':'Wind/Squall Warning','WATCH':'Watch','ADVISORY':'Advisory'
  };
  var SEV_COLORS = {
    'BLIZZARD':'#cc44ff','ICE_STORM':'#ff4444','WSW':'#4488ff',
    'SNOW_WARN':'#44ccff','WIND_WARN':'#ff8c00','WATCH':'#ffaa00','ADVISORY':'#aaccff'
  };
  var SEV_TEXT = {
    'BLIZZARD':'#fff','ICE_STORM':'#fff','WSW':'#fff',
    'SNOW_WARN':'#000','WIND_WARN':'#fff','WATCH':'#000','ADVISORY':'#000'
  };

  var lastAlerts = [];

  function escHtml(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // â”€â”€ Alert modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function openAlertModal(a) {
    var color  = SEV_COLORS[a.severity] || '#aaccff';
    var label  = SEV_LABELS[a.severity] || a.severity;
    var badge  = document.getElementById('alert-modal-badge');
    badge.textContent = label;
    badge.style.color = color;
    document.getElementById('alert-modal-event').textContent    = a.event    || '';
    document.getElementById('alert-modal-area').textContent     = a.area     || '';
    document.getElementById('alert-modal-headline').textContent = a.headline || 'No additional details available.';
    document.getElementById('alert-modal-issued').textContent   = a.issued   || 'â€”';
    document.getElementById('alert-modal-expires').textContent  = a.expires  || 'â€”';
    document.getElementById('alert-modal-tna').style.display    = a.tna ? 'block' : 'none';
    document.getElementById('alert-modal-box').style.borderColor = color;
    document.getElementById('alert-modal').classList.add('open');
    document.body.style.overflow = 'hidden';
  }
  function closeAlertModal(e, force) {
    if (force || (e && e.target === document.getElementById('alert-modal'))) {
      document.getElementById('alert-modal').classList.remove('open');
      document.body.style.overflow = '';
    }
  }
  document.addEventListener('keydown', function(e){ if (e.key === 'Escape') closeAlertModal(null, true); });

  function updateStats(alerts) {
    var blizzard=0, ice=0, wsw=0, watches=0;
    var states = {};
    for (var i=0; i<alerts.length; i++) {
      var a = alerts[i];
      if (a.severity==='BLIZZARD')  blizzard++;
      if (a.severity==='ICE_STORM') ice++;
      if (a.severity==='WSW')       wsw++;
      if (a.severity==='WATCH')     watches++;
      var parts = a.area.split(';');
      for (var j=0; j<parts.length; j++) {
        var m = parts[j].trim().match(/,\s*([A-Z]{2})$/);
        if (m) states[m[1]] = true;
      }
    }
    document.getElementById('alert-count').textContent  = alerts.length;
    document.getElementById('cnt-blizzard').textContent = blizzard;
    document.getElementById('cnt-ice').textContent      = ice;
    document.getElementById('cnt-wsw').textContent      = wsw;
    document.getElementById('cnt-watch').textContent    = watches;
    document.getElementById('cnt-states').textContent   = Object.keys(states).length;
  }

  function updateTicker(alerts) {
    var el = document.getElementById('ticker-inner');
    if (!alerts || !alerts.length) {
      el.textContent = 'No active winter weather alerts.';
      return;
    }
    var parts = [];
    for (var i=0; i<alerts.length; i++) {
      var a = alerts[i];
      parts.push((SEV_LABELS[a.severity] || a.severity) + ': ' + a.area + (a.tna ? ' â€” TRAVEL NOT ADVISED' : ''));
    }
    el.textContent = parts.join('   Â·   ');
  }

  function renderAlerts(alerts) {
    var grid  = document.getElementById('alert-grid');
    var empty = document.getElementById('empty');
    // Remove old cards, keep the empty placeholder node
    Array.from(grid.children).forEach(function(c){ if (c !== empty) c.remove(); });
    if (!alerts || !alerts.length) {
      empty.style.display = 'block';
      return;
    }
    empty.style.display = 'none';
    var sorted = alerts.slice().sort(function(a,b){
      return SEV_ORDER.indexOf(a.severity) - SEV_ORDER.indexOf(b.severity);
    });
    sorted.forEach(function(a) {
      var color  = SEV_COLORS[a.severity] || '#aaccff';
      var tcolor = SEV_TEXT[a.severity]   || '#000';
      var label  = SEV_LABELS[a.severity] || a.severity;
      var card   = document.createElement('div');
      card.className = 'alert-card';
      card.style.borderLeftColor = color;
      card.innerHTML =
        '<div class="card-top">' +
        '  <span class="sev-badge" style="background:' + color + ';color:' + tcolor + '">' + escHtml(label) + '</span>' +
        '</div>' +
        (a.tna ? '<div class="tna-badge">&#x1F6AB; TRAVEL NOT ADVISED</div>' : '') +
        '<div class="card-area">'     + escHtml(a.area)     + '</div>' +
        '<div class="card-headline">' + escHtml(a.headline) + '</div>' +
        '<div class="card-times">' +
        '  <div><div class="t-lbl">Issued</div><div class="t-val">'  + escHtml(a.issued)  + '</div></div>' +
        '  <div><div class="t-lbl">Expires</div><div class="t-val">' + escHtml(a.expires) + '</div></div>' +
        '</div>';
      card.onclick = (function(alert){ return function(){ openAlertModal(alert); }; })(a);
      grid.insertBefore(card, empty);
    });
  }

  function load() {
    fetch('/winter-data')
      .then(function(r){ return r.json(); })
      .then(function(d) {
        lastAlerts = d.alerts || [];
        updateStats(lastAlerts);
        updateTicker(lastAlerts);
        renderAlerts(lastAlerts);
        document.getElementById('poll-time').textContent = 'NWS data: ' + (d.updated || 'â€”');
      })
      .catch(function(){
        document.getElementById('poll-time').textContent = 'Failed to reach NWS';
      });
  }

  function tick() {
    var now = new Date();
    document.getElementById('clock').textContent =
      now.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',second:'2-digit'});
  }
  tick();
  setInterval(tick, 1000);

  load();
  setInterval(load, 8000);

  // Auto-reload when server restarts
  (function(){ var s=null; setInterval(function(){
    fetch('/health').then(function(r){return r.json();}).then(function(d){
      if(!s){s=d.server_start;return;} if(d.server_start!==s)location.reload();
    }).catch(function(){});
  }, 30000); })();
</script>
<style>.home-btn{position:fixed;bottom:14px;right:14px;z-index:200;background:#111;border:1px solid #2a2a2a;color:#555;font-size:.72rem;padding:5px 13px;border-radius:5px;text-decoration:none;opacity:.6;transition:opacity .2s}.home-btn:hover{opacity:1;color:#aaa;border-color:#555}</style>
<a class="home-btn" href="http://localhost:8080/">Home</a>
</body>
</html>"""


DISPLAY_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="theme-color" content="#cc0000">
<link rel="icon" href="/icon.svg">
<link rel="manifest" href="/manifest.json">
<title>Tornado Warning Display â€” tornadowatch.org</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #060608;
    color: #fff;
    font-family: 'Segoe UI', Arial, sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* â”€â”€ Top bar â”€â”€ */
  #topbar {
    display: flex;
    align-items: stretch;
    border-bottom: 3px solid #cc0000;
    flex-shrink: 0;
  }
  #branding {
    background: #cc0000;
    padding: 10px 24px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    flex-shrink: 0;
  }
  #branding .title { font-size: 1.1rem; font-weight: 900; letter-spacing: 0.12em; text-transform: uppercase; }
  #branding .sub   { font-size: 0.65rem; letter-spacing: 0.1em; color: #ffcccc; margin-top: 1px; }
  #count-box {
    background: #1a0000;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 10px;
    border-right: 1px solid #440000;
    flex-shrink: 0;
  }
  #warning-count { font-size: 3.2rem; font-weight: 900; color: #ff4444; line-height: 1; }
  #count-label   { font-size: 0.72rem; color: #aaa; text-transform: uppercase; letter-spacing: 0.08em; line-height: 1.4; }
  #watch-count-box {
    background: #120d00;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 10px;
    border-right: 1px solid #332200;
    flex-shrink: 0;
  }
  #watch-count      { font-size: 3.2rem; font-weight: 900; color: #ffaa00; line-height: 1; }
  #watch-count-label { font-size: 0.72rem; color: #aa8833; text-transform: uppercase; letter-spacing: 0.08em; line-height: 1.4; }
  #stats-box { display: flex; gap: 0; flex-shrink: 0; }
  .stat {
    padding: 10px 18px;
    border-right: 1px solid #222;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  .stat .num { font-size: 1.6rem; font-weight: 800; }
  .stat .lbl { font-size: 0.6rem; color: #777; text-transform: uppercase; letter-spacing: 0.08em; }
  .stat.confirmed .num { color: #ff3333; }
  .stat.radar     .num { color: #ff8c00; }
  .stat.watches   .num { color: #ffaa00; }
  .stat.states    .num { color: #44aaff; }
  #clock-box {
    margin-left: auto;
    padding: 10px 20px;
    text-align: right;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  #clock-box #time-display { font-size: 1.2rem; color: #ccc; font-weight: 300; }
  #clock-box #poll-time    { font-size: 0.62rem; color: #444; margin-top: 2px; }

  /* â”€â”€ Ticker strip â”€â”€ */
  #ticker-wrap {
    background: #0d0d0d;
    border-bottom: 1px solid #1a1a1a;
    overflow: hidden;
    height: 28px;
    flex-shrink: 0;
    display: flex;
    align-items: center;
  }
  #ticker-label {
    background: #cc0000;
    color: #fff;
    font-size: 0.65rem;
    font-weight: 800;
    letter-spacing: 0.1em;
    padding: 0 10px;
    height: 100%;
    display: flex;
    align-items: center;
    flex-shrink: 0;
  }
  #ticker-track { flex: 1; overflow: hidden; white-space: nowrap; position: relative; }
  #ticker-inner {
    font-size: 0.85rem;
    font-weight: 600;
    color: #ddd;
    white-space: nowrap;
    display: inline-block;
    will-change: transform;
    backface-visibility: hidden;
    -webkit-backface-visibility: hidden;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
  }
  @keyframes ticker { from { transform: translate3d(var(--ticker-start), 0, 0); } to { transform: translate3d(var(--ticker-end), 0, 0); } }

  /* â”€â”€ Main split layout â”€â”€ */
  #main-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    min-height: 0;
  }

  /* â”€â”€ Warnings section (top) â”€â”€ */
  #warnings-section {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 12px 18px;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
    gap: 10px;
    align-content: start;
  }
  #no-warnings {
    grid-column: 1/-1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    min-height: 120px;
    gap: 14px;
    color: #22cc66;
  }
  #no-warnings .icon  { font-size: 3.5rem; }
  #no-warnings .label { font-size: 1.8rem; font-weight: 700; letter-spacing: 0.06em; }

  /* Warning cards */
  .card { border-radius: 8px; padding: 12px 16px; border-left: 6px solid; position: relative; }
  .card.confirmed { background: #220000; border-color: #ff2222; animation: card-flash 1.2s ease-in-out infinite; box-shadow: 0 0 20px 4px #ff222255; }
  .card.radar     { background: #1a1000; border-color: #ff8c00; }
  @keyframes card-flash {
    0%,100% { background: #220000; box-shadow: 0 0 20px 4px #ff222255; }
    50%     { background: #440000; box-shadow: 0 0 36px 10px #ff2222aa; }
  }
  .card-badge {
    font-size: 0.6rem; font-weight: 800; letter-spacing: 0.12em;
    padding: 2px 8px; border-radius: 3px; text-transform: uppercase; margin-bottom: 5px;
    display: inline-block;
  }
  .card-badge.confirmed { background: #ff2222; color: #fff; }
  .card-badge.radar     { background: #ff8c00; color: #000; }
  .card-area     { font-size: 1rem; font-weight: 700; margin-bottom: 3px; line-height: 1.3; }
  .card-headline { font-size: 0.7rem; color: #999; margin-bottom: 5px; }
  .card-times    { font-size: 0.66rem; color: #666; display: flex; gap: 14px; flex-wrap: wrap; }
  .card-times strong { color: #bbb; }
  .card-age      { font-size: 0.62rem; color: #555; margin-left: auto; font-style: italic; }
  .card-expiry   { font-size: 0.62rem; color: #888; font-style: italic; }
  .card { cursor: pointer; }

  /* â”€â”€ Modal overlay â”€â”€ */
  #warn-modal {
    display: none; position: fixed; inset: 0; z-index: 600;
    background: #000000bb; align-items: center; justify-content: center;
  }
  #warn-modal.open { display: flex; }
  #warn-modal-box {
    background: #0e0a0a; border: 2px solid #cc0000; border-radius: 10px;
    max-width: 660px; width: 92%; padding: 24px 28px 20px;
    max-height: 80vh; overflow-y: auto; position: relative;
  }
  #warn-modal-close {
    position: absolute; top: 10px; right: 14px;
    background: none; border: none; color: #ff4444;
    font-size: 1.4rem; cursor: pointer; line-height: 1;
  }
  #warn-modal-badge { font-size: 0.62rem; font-weight: 800; letter-spacing: 0.1em; color: #ff4444; margin-bottom: 6px; }
  #warn-modal-area  { font-size: 1.1rem; font-weight: 800; color: #fff; margin-bottom: 10px; line-height: 1.3; }
  #warn-modal-body  { font-size: 0.78rem; color: #bbb; line-height: 1.7; white-space: pre-wrap; }

  /* â”€â”€ LSR section â”€â”€ */
  #lsr-divider {
    flex-shrink: 0; background: #00080e; border-top: 2px solid #004466;
    border-bottom: 1px solid #002233; padding: 5px 18px;
    display: none; align-items: center; justify-content: space-between;
    font-size: 0.72rem; font-weight: 800; letter-spacing: 0.12em;
    color: #0099cc; text-transform: uppercase; cursor: pointer;
  }
  #lsr-divider:hover { background: #00101a; }
  #lsr-section {
    flex-shrink: 0; max-height: 28%; overflow-y: auto;
    padding: 8px 18px; display: none;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 6px; align-content: start;
  }
  .lsr-card {
    background: #00080e; border: 1px solid #003344; border-left: 4px solid #0099cc;
    border-radius: 6px; padding: 8px 12px; font-size: 0.7rem;
  }
  .lsr-card.ef0  { border-left-color: #44aaff; }
  .lsr-card.ef1  { border-left-color: #22ccff; }
  .lsr-card.ef2  { border-left-color: #ffcc00; }
  .lsr-card.ef3  { border-left-color: #ff8800; }
  .lsr-card.ef4, .lsr-card.ef5 { border-left-color: #ff2222; }
  .lsr-scale   { font-size: 0.58rem; font-weight: 800; letter-spacing: 0.1em; color: #0099cc; margin-bottom: 3px; }
  .lsr-loc     { font-weight: 700; color: #cce8ff; margin-bottom: 2px; }
  .lsr-meta    { color: #446; font-size: 0.62rem; margin-bottom: 3px; }
  .lsr-comment { color: #aaa; font-size: 0.68rem; line-height: 1.4; }

  /* â”€â”€ Watches divider (draggable) â”€â”€ */
  #watches-divider {
    flex-shrink: 0;
    background: #0e0b00;
    border-top: 2px solid #886600;
    border-bottom: 1px solid #443300;
    padding: 5px 18px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.12em;
    color: #cc9900;
    text-transform: uppercase;
    cursor: ns-resize;
    user-select: none;
    position: relative;
  }
  /* subtle grip dots in the centre of the divider bar */
  #watches-divider::after {
    content: 'â ¿';
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    font-size: 0.9rem;
    color: #554400;
    pointer-events: none;
    letter-spacing: -1px;
  }
  #watches-divider:hover { background: #181000; border-top-color: #aa8800; }
  #watches-divider .watch-div-right { font-size: 0.65rem; color: #665500; font-weight: 400; }

  /* â”€â”€ Watches section (bottom) â”€â”€ */
  #watches-section {
    flex-shrink: 0;
    height: 36%;
    min-height: 110px;
    overflow-y: auto;
    padding: 10px 18px;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap: 8px;
    align-content: start;
    background: #07060000;
  }
  .watch-card {
    border-radius: 6px;
    padding: 10px 14px;
    border-left: 5px solid #886600;
    background: #0e0b00;
  }
  .watch-card.upgraded {
    border-color: #ffaa00;
    background: #181000;
    animation: watch-pulse 2s ease-in-out infinite;
    box-shadow: 0 0 16px 3px #cc880055;
  }
  @keyframes watch-pulse {
    0%,100% { box-shadow: 0 0 16px 3px #cc880055; }
    50%     { box-shadow: 0 0 28px 8px #ffaa0088; }
  }
  .watch-badge {
    font-size: 0.58rem; font-weight: 800; letter-spacing: 0.1em;
    padding: 2px 7px; border-radius: 3px; text-transform: uppercase; margin-bottom: 4px;
    display: inline-block; background: #886600; color: #000;
  }
  .watch-badge.upgraded { background: #ffaa00; color: #000; }
  .upgraded-pill {
    display: inline-block; margin-left: 6px;
    font-size: 0.55rem; font-weight: 800; letter-spacing: 0.08em;
    padding: 1px 6px; border-radius: 3px;
    background: #ff3300; color: #fff; text-transform: uppercase; vertical-align: middle;
  }
  .watch-area     { font-size: 0.95rem; font-weight: 700; margin-bottom: 3px; line-height: 1.3; }
  .watch-headline { font-size: 0.67rem; color: #776644; margin-bottom: 4px; }
  .watch-times    { font-size: 0.64rem; color: #554433; display: flex; gap: 14px; flex-wrap: wrap; }
  .watch-times strong { color: #997722; }
  .watch-age      { font-size: 0.6rem; color: #443322; margin-left: auto; font-style: italic; }

  /* â”€â”€ Body strobe on confirmed â”€â”€ */
  body.confirmed-active { animation: body-strobe 1.2s ease-in-out infinite; }
  @keyframes body-strobe { 0%,100%{background:#060608} 50%{background:#1a0000} }

  /* â”€â”€ Nav bar â”€â”€ */
  #navbar {
    background: #0a0a0f;
    border-bottom: 1px solid #1a1a1a;
    display: flex;
    align-items: center;
    padding: 0 20px;
    flex-shrink: 0;
    height: 36px;
  }
  .nav-link {
    color: #555;
    text-decoration: none;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0 16px;
    height: 100%;
    display: flex;
    align-items: center;
    border-bottom: 2px solid transparent;
    transition: color 0.15s;
  }
  .nav-link:hover  { color: #aaa; border-bottom-color: #444; }
  .nav-link.active { color: #ff4444; border-bottom-color: #cc0000; }
  .nav-divider { width: 1px; height: 16px; background: #1a1a1a; margin: 0 4px; }
</style>
</head>
<body>

<div id="topbar">
  <div id="branding">
    <div class="title">Tornado Monitor</div>
    <div class="sub">tornadowatch.org &mdash; Live NWS Feed</div>
    <div style="font-size:0.7rem;letter-spacing:0.14em;color:#fff;margin-top:4px;text-transform:uppercase;font-weight:800;opacity:0.85">by Brendan G</div>
  </div>
  <div id="count-box">
    <div id="warning-count">0</div>
    <div id="count-label">Active<br>Warnings</div>
  </div>
  <div id="watch-count-box">
    <div id="watch-count">0</div>
    <div id="watch-count-label">Active<br>Watches</div>
  </div>
  <div id="stats-box">
    <div class="stat confirmed"><div class="num" id="stat-confirmed">0</div><div class="lbl">Confirmed</div></div>
    <div class="stat radar">    <div class="num" id="stat-radar">0</div>    <div class="lbl">Radar</div></div>
    <div class="stat watches">  <div class="num" id="stat-upgraded">0</div> <div class="lbl">Upgraded</div></div>
    <div class="stat states">   <div class="num" id="stat-states">0</div>   <div class="lbl">States</div></div>
  </div>
  <div id="clock-box">
    <div id="time-display"></div>
    <div id="poll-time">Connecting...</div>
  </div>
</div>

<div id="navbar">
  <a href="/" class="nav-link active">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link">&#x1F5FA; Map</a>
</div>

<div id="ticker-wrap">
  <div id="ticker-label">LIVE</div>
  <div id="ticker-track"><div id="ticker-inner">Connecting to NWS feed...</div></div>
</div>

<div id="main-content">
  <div id="warnings-section">
    <div id="no-warnings">
      <div class="icon">&#x2714;</div>
      <div class="label">No Active Tornado Warnings</div>
    </div>
  </div>

  <div id="watches-divider" style="display:none">
    <span>&#x1F441; Tornado Watches in Effect</span>
    <span class="watch-div-right" id="watches-divider-right"></span>
  </div>

  <div id="watches-section" style="display:none"></div>

  <div id="lsr-divider" onclick="toggleLSR()">
    <span>&#x1F4CB; SPC Storm Reports &mdash; Today</span>
    <span id="lsr-toggle-icon" style="font-size:0.8rem">&#x25BC;</span>
  </div>
  <div id="lsr-section"></div>
</div>

<!-- Warning detail modal -->
<div id="warn-modal" onclick="closeModal(event)">
  <div id="warn-modal-box">
    <button id="warn-modal-close" onclick="closeModal(null,true)">&#x2715;</button>
    <div id="warn-modal-badge"></div>
    <div id="warn-modal-area"></div>
    <div id="warn-modal-body"></div>
  </div>
</div>

<script>
  // â”€â”€ Clock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function tick() {
    var now = new Date();
    document.getElementById('time-display').textContent =
      now.toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric'}) + '  ' +
      now.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',second:'2-digit'});
  }
  tick(); setInterval(tick, 1000);

  // â”€â”€ Push notifications â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var prevWarnKeys = new Set();
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission();
  }
  function maybeNotify(warnings) {
    if (Notification.permission !== 'granted') return;
    (warnings || []).forEach(function(w) {
      if (w._test) return;   // skip test injections
      var k = w.sent_raw || w.area;
      if (!prevWarnKeys.has(k)) {
        new Notification('ðŸŒª Tornado Warning', {
          body: (w.detection || '') + ' â€” ' + w.area,
          tag:  k,   // prevents duplicate stacking for same warning
        });
      }
    });
    prevWarnKeys = new Set((warnings || []).map(function(w){ return w.sent_raw || w.area; }));
  }

  // â”€â”€ Age helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function ageStr(sent_raw) {
    if (!sent_raw) return '';
    var ms = Date.now() - new Date(sent_raw).getTime();
    if (ms < 0) return '';
    var mins = Math.floor(ms / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return mins + 'm ago';
    var h = Math.floor(mins / 60), m = mins % 60;
    return h + 'h' + (m ? ' ' + m + 'm' : '') + ' ago';
  }

  // â”€â”€ Expiry helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function expiryStr(expires_raw) {
    if (!expires_raw) return '';
    var ms = new Date(expires_raw).getTime() - Date.now();
    if (ms <= 0) return 'Expired';
    var mins = Math.ceil(ms / 60000);
    if (mins < 60) return 'Exp. in ' + mins + 'm';
    var h = Math.floor(mins / 60), m = mins % 60;
    return 'Exp. in ' + h + 'h' + (m ? ' ' + m + 'm' : '');
  }

  // â”€â”€ Modal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function openModal(detection, area, description) {
    document.getElementById('warn-modal-badge').textContent = detection;
    document.getElementById('warn-modal-area').textContent  = area;
    document.getElementById('warn-modal-body').textContent  =
      description && description.trim() ? description : 'No additional description available.';
    document.getElementById('warn-modal').classList.add('open');
    document.body.style.overflow = 'hidden';
  }
  function closeModal(e, force) {
    if (force || (e && e.target === document.getElementById('warn-modal'))) {
      document.getElementById('warn-modal').classList.remove('open');
      document.body.style.overflow = '';
    }
  }
  document.addEventListener('keydown', function(e){ if (e.key === 'Escape') closeModal(null, true); });

  // â”€â”€ LSR section â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var lsrOpen = false;
  function toggleLSR() {
    lsrOpen = !lsrOpen;
    var sec = document.getElementById('lsr-section');
    var ico = document.getElementById('lsr-toggle-icon');
    sec.style.display = lsrOpen ? 'grid' : 'none';
    ico.textContent   = lsrOpen ? 'â–²' : 'â–¼';
  }
  function efClass(scale) {
    if (!scale) return '';
    var s = scale.toUpperCase();
    if (s.indexOf('EF0') !== -1 || s === '0') return 'ef0';
    if (s.indexOf('EF1') !== -1 || s === '1') return 'ef1';
    if (s.indexOf('EF2') !== -1 || s === '2') return 'ef2';
    if (s.indexOf('EF3') !== -1 || s === '3') return 'ef3';
    if (s.indexOf('EF4') !== -1 || s === '4') return 'ef4';
    if (s.indexOf('EF5') !== -1 || s === '5') return 'ef5';
    return '';
  }
  function renderLSRs(lsrs) {
    var div   = document.getElementById('lsr-divider');
    var sec   = document.getElementById('lsr-section');
    if (!lsrs || !lsrs.length) {
      div.style.display = 'none';
      sec.style.display = 'none';
      return;
    }
    div.style.display = 'flex';
    // Only show section if user has opened it
    if (lsrOpen) sec.style.display = 'grid';
    // Sort newest time first (HHmm strings)
    var sorted = lsrs.slice().sort(function(a,b){ return b.time.localeCompare(a.time); });
    sec.innerHTML = sorted.map(function(r) {
      var cls = efClass(r.f_scale);
      var scale = r.f_scale && r.f_scale !== 'nan' ? r.f_scale.toUpperCase() : 'UNK';
      return '<div class="lsr-card ' + cls + '">' +
        '<div class="lsr-scale">' + esc(scale) + '</div>' +
        '<div class="lsr-loc">'   + esc(r.location) + '</div>' +
        '<div class="lsr-meta">'  + esc(r.county) + ', ' + esc(r.state) +
          (r.time ? '  &bull;  ' + esc(r.time.substring(0,2) + ':' + r.time.substring(2)) + ' UTC' : '') + '</div>' +
        (r.comments ? '<div class="lsr-comment">' + esc(r.comments) + '</div>' : '') +
        '</div>';
    }).join('');
  }

  // â”€â”€ SSE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var lastData = { warnings: [], watches: [], lsrs: [] };
  var sseRetryTimer = null;
  var _sseStart = null;
  function connectSSE() {
    if (sseRetryTimer) { clearTimeout(sseRetryTimer); sseRetryTimer = null; }
    var src = new EventSource('/stream');
    src.onmessage = function(e) {
      if (e.data === 'ping') return;
      try {
        var data = JSON.parse(e.data);
        // Reload if server was restarted (new server_start token)
        if (data.server_start) {
          if (_sseStart === null) { _sseStart = data.server_start; }
          else if (_sseStart !== data.server_start) { location.reload(); return; }
        }
        maybeNotify(data.warnings);
        lastData = data;
        document.getElementById('poll-time').textContent = 'NWS poll: ' + data.last_poll;
        render(data.warnings || [], data.watches || []);
        renderLSRs(data.lsrs || []);
      } catch(err) {}
    };
    src.onerror = function() { src.close(); sseRetryTimer = setTimeout(connectSSE, 5000); };
  }
  connectSSE();

  // Re-render every 30s so age/expiry indicators stay fresh
  setInterval(function() {
    render(lastData.warnings || [], lastData.watches || []);
    renderLSRs(lastData.lsrs || []);
  }, 30000);

  // â”€â”€ Ticker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function updateTicker(warnings, watches) {
    var parts = [];
    (warnings||[]).forEach(function(w){ parts.push(w.detection + ':  ' + w.area); });
    (watches||[]).forEach(function(w){
      parts.push((w.upgraded ? 'â¬† WATCH (WARNING ACTIVE):  ' : 'WATCH:  ') + w.area);
    });
    var tickerText = parts.length ? parts.join('   â€¢   ') : 'No active tornado warnings or watches at this time.';
    var inner = document.getElementById('ticker-inner');
    var track = document.getElementById('ticker-track');
    inner.style.animation = 'none';
    inner.textContent = tickerText;
    requestAnimationFrame(function() {
      var trackW = Math.round(track.offsetWidth);
      var textW  = Math.round(inner.scrollWidth);
      var dur    = ((trackW + textW) / 80).toFixed(1) + 's';
      inner.style.setProperty('--ticker-start', trackW + 'px');
      inner.style.setProperty('--ticker-end',   '-' + textW + 'px');
      inner.style.animation = 'ticker ' + dur + ' linear infinite';
    });
  }

  // â”€â”€ Main render â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function render(warnings, watches) {
    var wsec     = document.getElementById('warnings-section');
    var wtsec    = document.getElementById('watches-section');
    var divider  = document.getElementById('watches-divider');
    var none     = document.getElementById('no-warnings');

    var confirmed = warnings.filter(function(w){ return w.detection.startsWith('CONFIRMED'); });
    var radar     = warnings.filter(function(w){ return !w.detection.startsWith('CONFIRMED'); });
    var upgraded  = (watches||[]).filter(function(w){ return w.upgraded; });
    var states    = new Set();
    warnings.forEach(function(w){
      w.area.split(';').forEach(function(p){
        var m = p.trim().match(/, ([A-Z]{2})$/);
        if (m) states.add(m[1]);
      });
    });
    watches.forEach(function(w){
      w.area.split(';').forEach(function(p){
        var m = p.trim().match(/, ([A-Z]{2})$/);
        if (m) states.add(m[1]);
      });
    });

    // Stats bar
    document.getElementById('warning-count').textContent  = warnings.length;
    document.getElementById('watch-count').textContent    = watches.length;
    document.getElementById('stat-confirmed').textContent = confirmed.length;
    document.getElementById('stat-radar').textContent     = radar.length;
    document.getElementById('stat-upgraded').textContent  = upgraded.length;
    document.getElementById('stat-states').textContent    = states.size;

    // Body strobe
    if (confirmed.length > 0) {
      document.body.classList.add('confirmed-active');
    } else {
      document.body.classList.remove('confirmed-active');
    }

    updateTicker(warnings, watches);

    // â”€â”€ Tab title badge â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    var twister = String.fromCodePoint(0x1F32A);
    document.title = warnings.length
      ? '(' + warnings.length + ') ' + twister + ' Tornado Monitor'
      : twister + ' Tornado Monitor';

    // â”€â”€ Warning cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    wsec.querySelectorAll('.card').forEach(function(c){ c.remove(); });
    if (warnings.length === 0) {
      none.style.display = 'flex';
    } else {
      none.style.display = 'none';
      confirmed.concat(radar).forEach(function(w) {
        var cls = w.detection.startsWith('CONFIRMED') ? 'confirmed' : 'radar';
        var div = document.createElement('div');
        div.className = 'card ' + cls;
        div.innerHTML =
          '<div class="card-badge ' + cls + '">' + esc(w.detection) + '</div>' +
          '<div class="card-area">' + esc(w.area) + '</div>' +
          '<div class="card-headline">' + esc(w.headline) + '</div>' +
          '<div class="card-times">' +
          '<span><strong>Issued:</strong> ' + esc(w.issued) + '</span>' +
          '<span><strong>Expires:</strong> ' + esc(w.expires) + '</span>' +
          (w.expires_raw ? '<span class="card-expiry">' + esc(expiryStr(w.expires_raw)) + '</span>' : '') +
          (w.sent_raw ? '<span class="card-age">' + esc(ageStr(w.sent_raw)) + '</span>' : '') +
          '</div>';
        div.onclick = function() { openModal(w.detection, w.area, w.description || ''); };
        wsec.appendChild(div);
      });
    }

    // â”€â”€ Watch cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (watches.length === 0) {
      divider.style.display  = 'none';
      wtsec.style.display    = 'none';
    } else {
      divider.style.display = 'flex';
      wtsec.style.display   = 'grid';
      var upgCount = upgraded.length;
      document.getElementById('watches-divider-right').textContent =
        upgCount ? upgCount + ' area' + (upgCount > 1 ? 's' : '') + ' upgraded to Warning' : watches.length + ' watch' + (watches.length > 1 ? 'es' : '') + ' active';
      wtsec.querySelectorAll('.watch-card').forEach(function(c){ c.remove(); });
      // Upgraded watches first
      var sorted = watches.slice().sort(function(a,b){ return (b.upgraded ? 1 : 0) - (a.upgraded ? 1 : 0); });
      sorted.forEach(function(w) {
        var div = document.createElement('div');
        div.className = 'watch-card' + (w.upgraded ? ' upgraded' : '');
        div.innerHTML =
          '<div class="watch-badge' + (w.upgraded ? ' upgraded' : '') + '">TORNADO WATCH' +
          (w.upgraded ? '<span class="upgraded-pill">&#x26A0; WARNING ACTIVE</span>' : '') + '</div>' +
          '<div class="watch-area">' + esc(w.area) + '</div>' +
          '<div class="watch-headline">' + esc(w.headline) + '</div>' +
          '<div class="watch-times">' +
          '<span><strong>Issued:</strong> ' + esc(w.issued) + '</span>' +
          '<span><strong>Expires:</strong> ' + esc(w.expires) + '</span>' +
          (w.expires_raw ? '<span class="card-expiry">' + esc(expiryStr(w.expires_raw)) + '</span>' : '') +
          (w.sent_raw ? '<span class="watch-age">' + esc(ageStr(w.sent_raw)) + '</span>' : '') +
          '</div>';
        wtsec.appendChild(div);
      });
    }
  }

  function esc(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  // â”€â”€ Draggable watches divider â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  (function() {
    var divider  = document.getElementById('watches-divider');
    var wtsec    = document.getElementById('watches-section');
    var main     = document.getElementById('main-content');
    var dragging = false, startY = 0, startH = 0;

    function onStart(y) {
      dragging = true;
      startY   = y;
      startH   = wtsec.offsetHeight;
      document.body.style.cursor     = 'ns-resize';
      document.body.style.userSelect = 'none';
    }
    function onMove(y) {
      if (!dragging) return;
      var dy   = startY - y;                              // drag up = taller watches pane
      var maxH = main.offsetHeight - 80;                  // leave room for at least one warning
      var newH = Math.max(80, Math.min(startH + dy, maxH));
      wtsec.style.height = newH + 'px';
    }
    function onEnd() {
      if (!dragging) return;
      dragging = false;
      document.body.style.cursor     = '';
      document.body.style.userSelect = '';
    }

    // Mouse
    divider.addEventListener('mousedown', function(e) { onStart(e.clientY); e.preventDefault(); });
    document.addEventListener('mousemove', function(e) { onMove(e.clientY); });
    document.addEventListener('mouseup',   onEnd);

    // Touch (for tablets / Pi touchscreen)
    divider.addEventListener('touchstart', function(e) { onStart(e.touches[0].clientY); e.preventDefault(); }, { passive: false });
    document.addEventListener('touchmove', function(e) { if (dragging) { onMove(e.touches[0].clientY); e.preventDefault(); } }, { passive: false });
    document.addEventListener('touchend',  onEnd);
  })();
</script>
<style>.home-btn{{position:fixed;bottom:14px;right:14px;z-index:200;background:#111;border:1px solid #2a2a2a;color:#555;font-size:.72rem;padding:5px 13px;border-radius:5px;text-decoration:none;opacity:.6;transition:opacity .2s}}.home-btn:hover{{opacity:1;color:#aaa;border-color:#555}}</style>
<a class="home-btn" href="http://localhost:8080/">Home</a>
</body>
</html>"""


@app.route("/history/export")
def history_export():
    rows = []
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT first_seen, area, detection, issued, expires,
                   CASE WHEN expired THEN 'EXPIRED' ELSE 'ACTIVE' END as status,
                   headline
            FROM warnings_log ORDER BY id DESC
        """).fetchall()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Date", "Area", "Detection", "Issued", "Expires", "Status", "Headline"])
    for r in rows:
        writer.writerow([
            r["first_seen"][:10] if r["first_seen"] else "",
            r["area"], r["detection"], r["issued"],
            r["expires"], r["status"], r["headline"],
        ])
    output = buf.getvalue()
    filename = f"tornado_warnings_{datetime.now().strftime('%Y-%m-%d')}.csv"
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/history")
def history():
    with db_connect() as conn:
        # All-time stats
        totals = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN detection LIKE 'CONFIRMED%' THEN 1 ELSE 0 END) as confirmed,
                SUM(CASE WHEN detection = 'RADAR INDICATED' THEN 1 ELSE 0 END) as radar
            FROM warnings_log
        """).fetchone()

        watch_total = conn.execute("SELECT COUNT(*) as c FROM watches_log").fetchone()["c"]

        # Most active state all-time
        top_state = conn.execute("""
            SELECT area FROM warnings_log
        """).fetchall()

        # This month
        this_month = conn.execute("""
            SELECT COUNT(*) as c,
                   SUM(CASE WHEN detection LIKE 'CONFIRMED%' THEN 1 ELSE 0 END) as conf
            FROM warnings_log
            WHERE first_seen >= date('now','start of month')
        """).fetchone()

        # Recent 100 warnings
        recent = conn.execute("""
            SELECT area, detection, issued, expires, first_seen, expired
            FROM warnings_log
            ORDER BY id DESC LIMIT 100
        """).fetchall()

        # Recent 50 watches
        recent_watches = conn.execute("""
            SELECT area, issued, expires, first_seen, expired
            FROM watches_log
            ORDER BY id DESC LIMIT 50
        """).fetchall()

        # Daily counts for past 14 days
        daily = conn.execute("""
            SELECT DATE(first_seen) as day,
                   COUNT(*) as total,
                   SUM(CASE WHEN detection LIKE 'CONFIRMED%' THEN 1 ELSE 0 END) as confirmed
            FROM warnings_log
            WHERE first_seen >= date('now','-14 days')
            GROUP BY day ORDER BY day DESC
        """).fetchall()

        # Winter storm stats
        winter_totals = conn.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN tna=1 THEN 1 ELSE 0 END) as tna_count,
                   SUM(CASE WHEN severity='BLIZZARD' THEN 1 ELSE 0 END) as blizzards,
                   SUM(CASE WHEN severity='ICE_STORM' THEN 1 ELSE 0 END) as ice_storms,
                   SUM(CASE WHEN severity='WSW' THEN 1 ELSE 0 END) as wsw_count
            FROM winter_log
        """).fetchone()

        winter_month = conn.execute("""
            SELECT COUNT(*) as c FROM winter_log
            WHERE first_seen >= date('now','start of month')
        """).fetchone()

        recent_winter = conn.execute("""
            SELECT area, event, severity, issued, expires, first_seen, expired, tna
            FROM winter_log ORDER BY id DESC LIMIT 100
        """).fetchall()

    # Count states and counties from all warnings
    state_counts  = Counter()
    county_counts = Counter()
    for row in top_state:
        area = row["area"]
        for s in extract_states(area):
            state_counts[s] += 1
        # Extract "County Name, ST" segments
        for part in area.split(";"):
            part = part.strip()
            if re.match(r'^[^,]+,\s*[A-Z]{2}$', part):
                county_counts[part] += 1
    top_5_states    = state_counts.most_common(5)
    top_10_counties = county_counts.most_common(10)

    total_w    = totals["total"] or 0
    confirmed  = totals["confirmed"] or 0
    radar_cnt  = totals["radar"] or 0
    conf_pct   = round(confirmed / total_w * 100) if total_w else 0
    month_total = this_month["c"] or 0
    month_conf  = this_month["conf"] or 0

    # Build daily chart data (fill missing days with 0)
    day_map = {row["day"]: (row["total"], row["confirmed"]) for row in daily}
    chart_days = []
    for i in range(13, -1, -1):
        d = (date.today() - timedelta(days=i)).isoformat()
        t, c = day_map.get(d, (0, 0))
        chart_days.append({"day": d[5:], "total": t, "confirmed": c})

    max_day = max((d["total"] for d in chart_days), default=1) or 1

    chart_bars = ""
    for d in chart_days:
        h_total = round(d["total"] / max_day * 100)
        h_conf  = round(d["confirmed"] / max_day * 100) if d["total"] else 0
        chart_bars += f"""<div class="bar-col">
            <div class="bar-wrap">
                <div class="bar-total" style="height:{h_total}%"></div>
                <div class="bar-conf"  style="height:{h_conf}%"></div>
            </div>
            <div class="bar-label">{d['day']}</div>
            <div class="bar-num">{d['total'] if d['total'] else ''}</div>
        </div>"""

    def status_badge(expired):
        return '<span style="color:#ff4444;font-size:0.65rem">EXPIRED</span>' if expired else \
               '<span style="color:#22cc66;font-size:0.65rem">ACTIVE</span>'

    rows_html = ""
    for r in recent:
        cls = "conf-row" if r["detection"].startswith("CONFIRMED") else "radar-row"
        rows_html += f"""<tr class="{cls}">
            <td>{r['first_seen'][:10] if r['first_seen'] else 'â€”'}</td>
            <td>{r['area']}</td>
            <td>{r['detection']}</td>
            <td>{r['issued']}</td>
            <td>{r['expires']}</td>
            <td>{status_badge(r['expired'])}</td>
        </tr>"""

    watch_rows_html = ""
    for r in recent_watches:
        watch_rows_html += f"""<tr class="watch-row-h">
            <td>{r['first_seen'][:10] if r['first_seen'] else 'â€”'}</td>
            <td>{r['area']}</td>
            <td>{r['issued']}</td>
            <td>{r['expires']}</td>
            <td>{status_badge(r['expired'])}</td>
        </tr>"""

    top_states_html = "".join(
        f'<div class="state-pill">{s} <span class="state-cnt">{c}</span></div>'
        for s, c in top_5_states
    ) or '<span style="color:#555">No data yet</span>'

    top_counties_html = "".join(
        f'<div class="state-pill county-pill">'
        f'{name} <span class="state-cnt">{c}</span></div>'
        for name, c in top_10_counties
    ) or '<span style="color:#555">No county data yet â€” needs "County, ST" format in area field</span>'

    # Winter storm computed values
    w_total     = winter_totals["total"]      or 0
    w_tna       = winter_totals["tna_count"]  or 0
    w_blizzards = winter_totals["blizzards"]  or 0
    w_ice       = winter_totals["ice_storms"] or 0
    w_wsw       = winter_totals["wsw_count"]  or 0
    w_month     = winter_month["c"]           or 0

    SEV_COLOR_MAP = {
        "BLIZZARD":"#cc44ff","ICE_STORM":"#ff4444","WSW":"#4488ff",
        "SNOW_WARN":"#44ccff","WIND_WARN":"#ff8c00","WATCH":"#ffaa00","ADVISORY":"#aaccff",
    }

    winter_rows_html = ""
    for r in recent_winter:
        sev_color = SEV_COLOR_MAP.get(r["severity"] or "", "#aaccff")
        tna_badge = ' <span style="background:#cc0000;color:#fff;font-size:0.6rem;border-radius:3px;padding:1px 5px;font-weight:800">TNA</span>' if r["tna"] else ""
        winter_rows_html += f"""<tr class="winter-row">
            <td>{r['first_seen'][:10] if r['first_seen'] else 'â€”'}</td>
            <td>{r['area']}</td>
            <td><span style="color:{sev_color};font-size:0.72rem;font-weight:700">{r['severity'] or 'â€”'}</span>{tna_badge}</td>
            <td style="color:#88bbdd;font-size:0.75rem">{r['event'] or 'â€”'}</td>
            <td>{r['issued'] or 'â€”'}</td>
            <td>{r['expires'] or 'â€”'}</td>
            <td>{status_badge(r['expired'])}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒªï¸</text></svg>">
<title>Tornado History â€” tornadowatch.org</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #060608; color: #fff; font-family: 'Segoe UI', Arial, sans-serif; min-height: 100vh; }}
  a {{ color: #ff4444; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}

  /* Header */
  #topbar {{
    background: #cc0000; padding: 12px 28px;
    display: flex; align-items: center; justify-content: space-between;
    border-bottom: 3px solid #990000;
  }}
  #topbar .title {{ font-size: 1.2rem; font-weight: 900; letter-spacing: 0.1em; text-transform: uppercase; }}
  #topbar .sub   {{ font-size: 0.7rem; color: #ffcccc; margin-top: 2px; }}
  #topbar a {{ color: #ffcccc; font-size: 0.8rem; }}

  /* Navbar */
  #navbar {{
    background: #0a0a0f;
    border-bottom: 1px solid #1a1a1a;
    display: flex; align-items: center;
    padding: 0 20px; height: 36px;
  }}
  .nav-link {{
    color: #555; text-decoration: none;
    font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase;
    padding: 0 16px; height: 100%;
    display: flex; align-items: center;
    border-bottom: 2px solid transparent; transition: color 0.15s;
  }}
  .nav-link:hover  {{ color: #aaa; border-bottom-color: #444; }}
  .nav-link.active {{ color: #ff4444; border-bottom-color: #cc0000; }}
  .nav-divider {{ width: 1px; height: 16px; background: #1a1a1a; margin: 0 4px; }}

  /* Main layout */
  .page {{ max-width: 1400px; margin: 0 auto; padding: 24px 28px; }}

  /* Stat cards */
  .stats-row {{ display: flex; gap: 14px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat-card {{
    flex: 1; min-width: 140px; background: #0e0e14;
    border: 1px solid #1a1a2a; border-radius: 8px;
    padding: 16px 20px;
  }}
  .stat-card .num {{ font-size: 2.4rem; font-weight: 900; line-height: 1; margin-bottom: 4px; }}
  .stat-card .lbl {{ font-size: 0.65rem; color: #666; text-transform: uppercase; letter-spacing: 0.1em; }}
  .stat-card.red    .num {{ color: #ff3333; }}
  .stat-card.orange .num {{ color: #ff8c00; }}
  .stat-card.yellow .num {{ color: #ffaa00; }}
  .stat-card.blue   .num {{ color: #44aaff; }}
  .stat-card.green  .num {{ color: #22cc66; }}

  /* Chart */
  .section {{ margin-bottom: 28px; }}
  .section-title {{
    font-size: 0.7rem; font-weight: 800; letter-spacing: 0.12em;
    text-transform: uppercase; color: #555; margin-bottom: 12px;
    border-bottom: 1px solid #1a1a1a; padding-bottom: 6px;
  }}

  /* Top states */
  .states-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 24px; }}
  .state-pill {{
    background: #1a1a2a; border: 1px solid #333; border-radius: 20px;
    padding: 4px 14px; font-size: 0.8rem; font-weight: 700; color: #ccc;
  }}
  .state-cnt {{ color: #ff4444; margin-left: 6px; }}
  .county-pill {{ font-size: 0.72rem; }}

  /* Tables */
  .table-wrap {{ overflow-x: auto; border-radius: 8px; border: 1px solid #1a1a1a; margin-bottom: 28px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.78rem; }}
  thead tr {{ background: #0e0e14; }}
  thead th {{ padding: 10px 14px; text-align: left; font-size: 0.62rem; font-weight: 700;
              letter-spacing: 0.1em; text-transform: uppercase; color: #555; border-bottom: 1px solid #1a1a1a; }}
  tbody tr {{ border-bottom: 1px solid #111; }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody td {{ padding: 8px 14px; vertical-align: top; color: #ccc; }}
  tr.conf-row td:first-child {{ border-left: 3px solid #ff2222; }}
  tr.radar-row td:first-child {{ border-left: 3px solid #ff8c00; }}
  tr.watch-row-h td:first-child {{ border-left: 3px solid #ffaa00; }}
  tr.winter-row td:first-child {{ border-left: 3px solid #44ccff; }}
  tbody tr:hover {{ background: #0d0d12; }}
  .det-confirmed {{ color: #ff4444; font-weight: 700; font-size: 0.72rem; }}
  .det-radar     {{ color: #ff8c00; font-size: 0.72rem; }}

  /* Tab bar */
  .tab-bar {{ display: flex; gap: 0; margin-bottom: 24px; border-bottom: 2px solid #1a1a1a; }}
  .tab-btn {{
    background: transparent; border: none; color: #555; cursor: pointer;
    font-size: 0.78rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
    padding: 10px 22px; border-bottom: 3px solid transparent; margin-bottom: -2px;
    transition: color 0.15s;
  }}
  .tab-btn:hover {{ color: #aaa; }}
  .tab-btn.active {{ color: #ff4444; border-bottom-color: #cc0000; }}
  .tab-btn.active.winter {{ color: #44ccff; border-bottom-color: #0066aa; }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}

  /* Winter stat cards */
  .stat-card.ice-purple .num {{ color: #cc44ff; }}
  .stat-card.ice-blue   .num {{ color: #44ccff; }}
  .stat-card.ice-red    .num {{ color: #ff4444; }}

  /* Chart */
  .chart {{ display: flex; gap: 6px; align-items: flex-end; height: 120px; background: #0a0a0f; border-radius: 8px; padding: 12px 16px 0; }}
  .bar-col {{ flex: 1; display: flex; flex-direction: column; align-items: center; height: 100%; }}
  .bar-wrap {{ flex: 1; width: 100%; display: flex; align-items: flex-end; position: relative; min-height: 0; }}
  .bar-total {{ width: 100%; background: #ff8c00; border-radius: 3px 3px 0 0; position: absolute; bottom: 0; }}
  .bar-conf  {{ width: 100%; background: #ff2222; border-radius: 3px 3px 0 0; position: absolute; bottom: 0; }}
  .bar-label {{ font-size: 0.55rem; color: #444; margin-top: 4px; white-space: nowrap; }}
  .bar-num   {{ font-size: 0.6rem; color: #666; }}
</style>
</head>
<body>
<div id="topbar">
  <div>
    <div class="title">&#x1F32A; Tornado History</div>
    <div class="sub">tornadowatch.org â€” All-time warning log</div>
  </div>
</div>

<div id="navbar">
  <a href="/" class="nav-link">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link active">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link">&#x1F5FA; Map</a>
</div>

<div class="page">

  <div class="tab-bar">
    <button class="tab-btn active"        onclick="switchTab('tornado', this)">&#x1F32A; Tornadoes &amp; Watches</button>
    <button class="tab-btn winter"        onclick="switchTab('winter',  this)">&#x1F328; Winter Storms</button>
  </div>

  <!-- â•â• TORNADO TAB â•â• -->
  <div id="tab-tornado" class="tab-content active">

  <div class="stats-row">
    <div class="stat-card red">   <div class="num">{total_w}</div>       <div class="lbl">Total Warnings</div></div>
    <div class="stat-card red">   <div class="num">{confirmed}</div>     <div class="lbl">Confirmed Touchdowns</div></div>
    <div class="stat-card orange"><div class="num">{radar_cnt}</div>     <div class="lbl">Radar Indicated</div></div>
    <div class="stat-card green"> <div class="num">{conf_pct}%</div>     <div class="lbl">Confirmation Rate</div></div>
    <div class="stat-card yellow"><div class="num">{watch_total}</div>   <div class="lbl">Total Watches</div></div>
    <div class="stat-card blue">  <div class="num">{month_total}</div>   <div class="lbl">This Month</div></div>
    <div class="stat-card red">   <div class="num">{month_conf}</div>    <div class="lbl">Confirmed This Month</div></div>
  </div>

  <div class="section">
    <div class="section-title">Most Active States (All Time)</div>
    <div class="states-row">{top_states_html}</div>
  </div>

  <div class="section">
    <div class="section-title">Most Active Counties (All Time) <span style="color:#333;margin-left:10px;font-weight:400;font-size:0.6rem">Top 10</span></div>
    <div class="states-row">{top_counties_html}</div>
  </div>

  <div class="section">
    <div class="section-title">Daily Warnings â€” Last 14 Days <span style="color:#333;margin-left:12px">&#x25A0; Radar&nbsp;&nbsp;&#x25A0;<span style="color:#ff2222"> Confirmed</span></span></div>
    <div class="chart">{chart_bars}</div>
  </div>

  <div class="section">
    <div class="section-title" style="display:flex;align-items:center;justify-content:space-between">
      <span>Recent Warnings (Last 100)</span>
      <a href="/history/export" style="font-size:0.65rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#aaa;text-decoration:none;border:1px solid #444;border-radius:4px;padding:3px 10px;transition:color 0.15s" onmouseover="this.style.color='#fff'" onmouseout="this.style.color='#aaa'">&#x2B07; Export CSV</a>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>Area</th><th>Detection</th><th>Issued</th><th>Expires</th><th>Status</th></tr></thead>
        <tbody>{rows_html or '<tr><td colspan="6" style="color:#444;padding:20px;text-align:center">No warnings logged yet â€” data accumulates as warnings occur.</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <div class="section-title">Recent Watches (Last 50)</div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>Area</th><th>Issued</th><th>Expires</th><th>Status</th></tr></thead>
        <tbody>{watch_rows_html or '<tr><td colspan="5" style="color:#444;padding:20px;text-align:center">No watches logged yet.</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  </div><!-- end #tab-tornado -->

  <!-- â•â• WINTER STORMS TAB â•â• -->
  <div id="tab-winter" class="tab-content">

  <div class="stats-row">
    <div class="stat-card ice-blue">  <div class="num">{w_total}</div>     <div class="lbl">Total Alerts</div></div>
    <div class="stat-card ice-purple"><div class="num">{w_blizzards}</div>  <div class="lbl">Blizzard Warnings</div></div>
    <div class="stat-card ice-red">   <div class="num">{w_ice}</div>        <div class="lbl">Ice Storm Warnings</div></div>
    <div class="stat-card blue">      <div class="num">{w_wsw}</div>        <div class="lbl">Winter Storm Warnings</div></div>
    <div class="stat-card red">       <div class="num">{w_tna}</div>        <div class="lbl">Travel Not Advised</div></div>
    <div class="stat-card green">     <div class="num">{w_month}</div>      <div class="lbl">This Month</div></div>
  </div>

  <div class="section">
    <div class="section-title">Recent Winter Alerts (Last 100)</div>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Date</th><th>Area</th><th>Severity</th><th>Event Type</th><th>Issued</th><th>Expires</th><th>Status</th></tr></thead>
        <tbody>{winter_rows_html or '<tr><td colspan="7" style="color:#444;padding:20px;text-align:center">No winter alerts logged yet â€” data accumulates as events occur.</td></tr>'}</tbody>
      </table>
    </div>
  </div>

  </div><!-- end #tab-winter -->

</div>
<script>
  function switchTab(name, btn) {{
    document.querySelectorAll('.tab-content').forEach(function(el){{ el.classList.remove('active'); }});
    document.querySelectorAll('.tab-btn').forEach(function(el){{ el.classList.remove('active'); }});
    document.getElementById('tab-' + name).classList.add('active');
    btn.classList.add('active');
  }}
  // Auto-refresh every 5 minutes so stats stay current
  setTimeout(function(){{ location.reload(); }}, 300000);
  document.addEventListener('DOMContentLoaded', function() {{
    var el = document.createElement('div');
    el.style.cssText = 'text-align:center;font-size:0.6rem;color:#333;padding:8px 0 16px';
    el.textContent = 'Auto-refreshes every 5 min â€” Last loaded: ' + new Date().toLocaleTimeString();
    document.querySelector('.page').appendChild(el);
  }});
  // Auto-reload when server restarts
  (function(){{ var s=null; setInterval(function(){{
    fetch('/health').then(function(r){{return r.json();}}).then(function(d){{
      if(!s){{s=d.server_start;return;}} if(d.server_start!==s)location.reload();
    }}).catch(function(){{}});
  }}, 30000); }})();
</script>
<div style="position:fixed;bottom:10px;right:14px;font-size:0.6rem;font-weight:700;letter-spacing:0.12em;color:#fff;opacity:0.4;text-transform:uppercase;pointer-events:none;user-select:none">by Brendan G</div>
</body>
</html>"""


@app.route("/stream")
def stream():
    def generate():
        last_sent = None
        ping_counter = 0
        while True:
            with _lock:
                w_snap  = list(_warnings)
                wt_snap = list(_watches)
                lp_snap = _last_poll
            with _lsr_lock:
                lsr_snap = list(_lsrs)
            payload = json.dumps({"warnings": w_snap, "watches": wt_snap,
                                  "last_poll": lp_snap, "lsrs": lsr_snap,
                                  "server_start": _server_start})
            if payload != last_sent:
                last_sent = payload
                yield "data: " + payload + "\n\n"
            ping_counter += 1
            if ping_counter >= 15:
                yield "data: ping\n\n"
                ping_counter = 0
            time.sleep(2)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/health")
def health():
    """Lightweight heartbeat â€” clients poll this to detect server restarts."""
    return json.dumps({"status": "ok", "server_start": _server_start}), 200, \
           {"Content-Type": "application/json",
            "Cache-Control": "no-cache"}


@app.route("/manifest.json")
def pwa_manifest():
    manifest = {
        "name": "Tornado Monitor",
        "short_name": "TornadoWatch",
        "description": "Live NWS Tornado Warning Dashboard â€” tornadowatch.org",
        "start_url": "/",
        "display": "standalone",
        "orientation": "any",
        "background_color": "#060608",
        "theme_color": "#cc0000",
        "icons": [
            {
                "src": "/icon.svg",
                "sizes": "any",
                "type": "image/svg+xml",
                "purpose": "any maskable"
            }
        ]
    }
    return Response(json.dumps(manifest), mimetype="application/manifest+json")


@app.route("/icon.svg")
def pwa_icon():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="80" fill="#0a0a0f"/>
  <text x="256" y="360" font-size="320" text-anchor="middle" font-family="serif">ðŸŒªï¸</text>
</svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/map-data")
def map_data():
    """Return active Tornado Warnings as GeoJSON, including test-injected warnings."""
    try:
        resp = requests.get(
            NWS_URL,
            params={"event": "Tornado Warning", "status": "actual"},
            headers=NWS_HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()
        features = raw.get("features", [])
        # Resolve any null geometries via affectedZones
        for f in features:
            if not f.get("geometry"):
                f["geometry"] = resolve_alert_geometry(f)
    except Exception as exc:
        log.warning("map-data NWS fetch failed: %s", exc)
        features = []

    # Blend in test-injected tornado warnings using the geometry locked in at inject time
    with _lock:
        test_warns = [w for w in _warnings if w.get("_test")]
    for w in test_warns:
        features.append({
            "type": "Feature",
            "geometry": w.get("geometry") or _test_polygon(w.get("area", "")),
            "properties": {
                "areaDesc": w.get("area", ""),
                "expires":  "",
                "headline": w.get("headline", ""),
                "parameters": {"tornadoDetection": [w.get("detection", "RADAR INDICATED")]},
                "_test": True,
            },
        })

    raw = {"type": "FeatureCollection", "features": features}
    return Response(json.dumps(raw), mimetype="application/geo+json",
                    headers={"Access-Control-Allow-Origin": "*"})


@app.route("/map")
def map_view():
    return MAP_PAGE


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#  HURRICANE PAGE
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
HURRICANE_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="theme-color" content="#0055aa">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸŒ€</text></svg>">
<title>Hurricane Tracker â€” tornadowatch.org</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #060608; color: #fff;
    font-family: 'Segoe UI', Arial, sans-serif;
    height: 100vh; display: flex; flex-direction: column; overflow: hidden;
  }

  /* â”€â”€ Top bar (mirrors tornado topbar) â”€â”€ */
  #topbar {
    display: flex; align-items: stretch;
    border-bottom: 3px solid #0055aa; flex-shrink: 0;
  }
  #branding {
    background: #0055aa; padding: 10px 24px;
    display: flex; flex-direction: column; justify-content: center; flex-shrink: 0;
  }
  #branding .title { font-size: 1.1rem; font-weight: 900; letter-spacing: 0.12em; text-transform: uppercase; }
  #branding .sub   { font-size: 0.65rem; letter-spacing: 0.1em; color: #aaccff; margin-top: 1px; }
  #count-box {
    background: #00091a; padding: 10px 20px;
    display: flex; align-items: center; gap: 10px;
    border-right: 1px solid #001133; flex-shrink: 0;
  }
  #storm-count { font-size: 3.2rem; font-weight: 900; color: #44aaff; line-height: 1; }
  #count-label { font-size: 0.72rem; color: #aaa; text-transform: uppercase; letter-spacing: 0.08em; line-height: 1.4; }
  #stats-box { display: flex; gap: 0; flex-shrink: 0; }
  .stat {
    padding: 10px 18px; border-right: 1px solid #0d1520;
    display: flex; flex-direction: column; justify-content: center;
  }
  .stat .num { font-size: 1.6rem; font-weight: 800; }
  .stat .lbl { font-size: 0.6rem; color: #557; text-transform: uppercase; letter-spacing: 0.08em; }
  .stat.hurr   .num { color: #ffaa00; }
  .stat.major  .num { color: #ff4444; }
  .stat.ts     .num { color: #44bbff; }
  .stat.basins .num { color: #44ffaa; }
  #clock-box {
    margin-left: auto; padding: 10px 20px;
    text-align: right; display: flex; flex-direction: column; justify-content: center;
  }
  #clock-box #time-display { font-size: 1.2rem; color: #ccc; font-weight: 300; }
  #clock-box #poll-time    { font-size: 0.62rem; color: #334; margin-top: 2px; }

  /* â”€â”€ Ticker â”€â”€ */
  #ticker-wrap {
    background: #00060e; border-bottom: 1px solid #001133;
    overflow: hidden; height: 28px; flex-shrink: 0;
    display: flex; align-items: center;
  }
  #ticker-label {
    background: #0055aa; color: #fff;
    font-size: 0.65rem; font-weight: 800; letter-spacing: 0.1em;
    padding: 0 10px; height: 100%; display: flex; align-items: center; flex-shrink: 0;
  }
  #ticker-track { flex: 1; overflow: hidden; white-space: nowrap; position: relative; }
  #ticker-inner {
    font-size: 0.85rem; font-weight: 600; color: #aaccff; white-space: nowrap;
    display: inline-block; will-change: transform;
    backface-visibility: hidden; -webkit-backface-visibility: hidden;
  }
  @keyframes ticker { from { transform: translate3d(var(--ts), 0, 0); } to { transform: translate3d(var(--te), 0, 0); } }

  /* â”€â”€ Nav bar â”€â”€ */
  #navbar {
    background: #0a0a0f; border-bottom: 1px solid #1a1a1a;
    display: flex; align-items: center; padding: 0 20px; flex-shrink: 0; height: 36px;
  }
  .nav-link {
    color: #555; text-decoration: none; font-size: 0.72rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase; padding: 0 16px;
    height: 100%; display: flex; align-items: center;
    border-bottom: 2px solid transparent; transition: color 0.15s;
  }
  .nav-link:hover  { color: #aaa; border-bottom-color: #444; }
  .nav-link.active { color: #44aaff; border-bottom-color: #0055aa; }
  .nav-divider { width: 1px; height: 16px; background: #1a1a1a; margin: 0 4px; }

  /* â”€â”€ Main content area â”€â”€ */
  #main-content {
    flex: 1; display: flex; flex-direction: column; overflow: hidden; min-height: 0;
  }

  /* â”€â”€ Storm cards section â”€â”€ */
  #storms-section {
    flex: 1; min-height: 0; overflow-y: auto;
    padding: 12px 18px;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
    gap: 10px; align-content: start;
  }
  #no-storms {
    grid-column: 1/-1; display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    height: 100%; min-height: 160px; gap: 14px;
  }
  #no-storms .icon  { font-size: 3.5rem; opacity: 0.25; }
  #no-storms .label { font-size: 1.6rem; font-weight: 700; letter-spacing: 0.06em; color: #44aaff; }
  #no-storms .sub   { font-size: 0.8rem; color: #334; }

  /* â”€â”€ Storm card â”€â”€ */
  .card { border-radius: 8px; padding: 12px 16px; border-left: 6px solid; position: relative; cursor: pointer; }
  /* Category-specific card styles */
  .card.cat-c5 { background: #180020; border-color: #cc44ff; animation: card-flash-c5 1.4s ease-in-out infinite; box-shadow: 0 0 20px 4px #cc44ff44; }
  .card.cat-c4 { background: #220000; border-color: #ff2222; animation: card-flash-c4 1.4s ease-in-out infinite; box-shadow: 0 0 20px 4px #ff222244; }
  .card.cat-c3 { background: #1a0800; border-color: #ff6600; }
  .card.cat-c2 { background: #1a1000; border-color: #ffaa00; }
  .card.cat-c1 { background: #181500; border-color: #ffee44; }
  .card.cat-ts { background: #001020; border-color: #44bbff; }
  .card.cat-td { background: #000c18; border-color: #5588ff; }
  .card.cat-lo { background: #0d0d0d; border-color: #888; }
  @keyframes card-flash-c5 {
    0%,100% { background: #180020; box-shadow: 0 0 20px 4px #cc44ff44; }
    50%     { background: #300045; box-shadow: 0 0 36px 10px #cc44ff88; }
  }
  @keyframes card-flash-c4 {
    0%,100% { background: #220000; box-shadow: 0 0 20px 4px #ff222244; }
    50%     { background: #440000; box-shadow: 0 0 36px 10px #ff222288; }
  }

  .card-badge {
    font-size: 0.6rem; font-weight: 800; letter-spacing: 0.12em;
    padding: 2px 8px; border-radius: 3px; text-transform: uppercase;
    margin-bottom: 5px; display: inline-block;
  }
  .card-name     { font-size: 1.1rem; font-weight: 900; margin-bottom: 3px; line-height: 1.2; }
  .card-loc      { font-size: 0.68rem; color: #445; margin-bottom: 6px; }
  .card-stats    { display: flex; gap: 18px; font-size: 0.68rem; color: #667; flex-wrap: wrap; margin-bottom: 5px; }
  .card-stats strong { color: #bbb; }
  .card-move     { font-size: 0.65rem; color: #445; }
  .card-times    { font-size: 0.64rem; color: #334; display: flex; gap: 14px; flex-wrap: wrap; margin-top: 5px; }
  .card-times strong { color: #667; }
  .basin-pill {
    position: absolute; top: 10px; right: 12px;
    font-size: 0.56rem; font-weight: 700; letter-spacing: 0.08em;
    padding: 2px 7px; border-radius: 3px; background: #0d111a; color: #445;
  }

  /* â”€â”€ Body strobe for Cat 4/5 â”€â”€ */
  body.major-active { animation: body-strobe 1.4s ease-in-out infinite; }
  @keyframes body-strobe { 0%,100%{background:#060608} 50%{background:#100018} }

  /* â”€â”€ Map section (collapsible, like LSR) â”€â”€ */
  #map-divider {
    flex-shrink: 0; background: #00060e; border-top: 2px solid #0055aa;
    border-bottom: 1px solid #001133; padding: 5px 18px;
    display: flex; align-items: center; justify-content: space-between;
    font-size: 0.72rem; font-weight: 800; letter-spacing: 0.12em;
    color: #44aaff; text-transform: uppercase; cursor: pointer; flex-shrink: 0;
  }
  #map-divider:hover { background: #00080f; }
  #map-section { flex-shrink: 0; height: 340px; display: none; position: relative; }
  #hurr-map    { width: 100%; height: 100%; }

  /* â”€â”€ Leaflet dark popup â”€â”€ */
  .leaflet-popup-content-wrapper { background: #07090f; color: #fff; border: 1px solid #004488; border-radius: 7px; }
  .leaflet-popup-tip { background: #07090f; }
  .leaflet-popup-content { font-size: 0.8rem; line-height: 1.7; min-width: 200px; }
  .pop-cat  { font-size: 0.6rem; font-weight: 900; letter-spacing: 0.1em; margin-bottom: 4px; }
  .pop-name { font-size: 1rem; font-weight: 900; color: #fff; margin-bottom: 8px; }
  .pop-row  { display: flex; justify-content: space-between; gap: 16px; }
  .pop-row span:first-child { color: #445; }
</style>
</head>
<body>

<div id="topbar">
  <div id="branding">
    <div class="title">Hurricane Tracker</div>
    <div class="sub">tornadowatch.org &mdash; NHC Live Feed</div>
  </div>
  <div id="count-box">
    <div id="storm-count">0</div>
    <div id="count-label">Active<br>Cyclones</div>
  </div>
  <div id="stats-box">
    <div class="stat hurr">  <div class="num" id="stat-hurr">0</div>  <div class="lbl">Hurricanes</div></div>
    <div class="stat major"> <div class="num" id="stat-major">0</div> <div class="lbl">Major (3+)</div></div>
    <div class="stat ts">    <div class="num" id="stat-ts">0</div>    <div class="lbl">Trop. Storms</div></div>
    <div class="stat basins"><div class="num" id="stat-basins">0</div><div class="lbl">Basins</div></div>
  </div>
  <div id="clock-box">
    <div id="time-display"></div>
    <div id="poll-time">Connecting...</div>
  </div>
</div>

<div id="navbar">
  <a href="/" class="nav-link">&#x1F32A; Tornadoes</a>
  <div class="nav-divider"></div>
  <a href="/winter" class="nav-link">&#x1F328; Winter</a>
  <div class="nav-divider"></div>
  <a href="/hurricane" class="nav-link active">&#x1F300; Hurricanes</a>
  <div class="nav-divider"></div>
  <a href="/history" class="nav-link">&#x1F4CA; History</a>
  <div class="nav-divider"></div>
  <a href="/map" class="nav-link">&#x1F5FA; Map</a>
</div>

<div id="ticker-wrap">
  <div id="ticker-label">LIVE</div>
  <div id="ticker-track"><div id="ticker-inner">Connecting to NHC feed...</div></div>
</div>

<div id="main-content">
  <div id="storms-section">
    <div id="no-storms">
      <div class="icon">&#x1F300;</div>
      <div class="label">No Active Tropical Cyclones</div>
      <div class="sub">The tropics are quiet &mdash; data refreshes every 5 minutes from NHC.</div>
    </div>
  </div>

  <div id="map-divider" onclick="toggleMap()">
    <span>&#x1F5FA; Track Map</span>
    <span id="map-toggle-icon" style="font-size:0.8rem">&#x25BC;</span>
  </div>
  <div id="map-section">
    <div id="hurr-map"></div>
  </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  // â”€â”€ Constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var CAT_COLOR = {
    C5:'#cc44ff', C4:'#ff2222', C3:'#ff6600',
    C2:'#ffaa00', C1:'#ffee44', TS:'#44bbff', TD:'#5588ff', LO:'#888888'
  };
  var CAT_TEXT = {
    C5:'#fff', C4:'#fff', C3:'#fff', C2:'#000', C1:'#000',
    TS:'#000', TD:'#fff', LO:'#fff'
  };
  var CAT_LABEL = {
    C5:'Cat 5 Hurricane', C4:'Cat 4 Hurricane', C3:'Cat 3 Hurricane',
    C2:'Cat 2 Hurricane', C1:'Cat 1 Hurricane',
    TS:'Tropical Storm', TD:'Tropical Depression', LO:'Low / Other'
  };
  var CAT_CLASS = {
    C5:'cat-c5', C4:'cat-c4', C3:'cat-c3', C2:'cat-c2', C1:'cat-c1',
    TS:'cat-ts', TD:'cat-td', LO:'cat-lo'
  };
  var BASIN_NAME = {
    AL:'Atlantic', EP:'E. Pacific', CP:'C. Pacific',
    WP:'W. Pacific', IO:'Indian Ocean', SH:'S. Hemisphere'
  };

  // â”€â”€ Clock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function tick() {
    var now = new Date();
    document.getElementById('time-display').textContent =
      now.toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric'}) + '  ' +
      now.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',second:'2-digit'});
  }
  tick(); setInterval(tick, 1000);

  // â”€â”€ Map â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var mapOpen   = false;
  var mapInited = false;
  var stormMap, stormLayer;

  function initMap() {
    if (mapInited) return;
    mapInited = true;
    stormMap = L.map('hurr-map', { zoomControl: true }).setView([25, -60], 3);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://carto.com/">CARTO</a>',
      subdomains: 'abcd', maxZoom: 19
    }).addTo(stormMap);
    stormLayer = L.layerGroup().addTo(stormMap);
  }

  function toggleMap() {
    mapOpen = !mapOpen;
    var sec = document.getElementById('map-section');
    var ico = document.getElementById('map-toggle-icon');
    sec.style.display = mapOpen ? 'block' : 'none';
    ico.textContent   = mapOpen ? 'â–²' : 'â–¼';
    if (mapOpen) {
      initMap();
      setTimeout(function(){ stormMap.invalidateSize(); }, 50);
    }
  }

  // â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function esc(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function dirText(deg) {
    var dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
    return dirs[Math.round(deg / 22.5) % 16];
  }
  function arrowTip(lat, lon, bearing, dist) {
    var rad = bearing * Math.PI / 180;
    return [lat + dist * Math.cos(rad), lon + dist * Math.sin(rad) / Math.cos(lat * Math.PI / 180)];
  }

  // â”€â”€ Ticker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function updateTicker(storms) {
    var parts = [];
    storms.forEach(function(s) {
      parts.push((CAT_LABEL[s.category] || s.category).toUpperCase() + ' ' + s.name.toUpperCase() +
        '  ' + s.winds_mph + ' MPH  ' + s.lat_str + ' ' + s.lon_str);
    });
    var text  = parts.length ? parts.join('   â€¢   ') : 'No active tropical cyclones at this time.';
    var inner = document.getElementById('ticker-inner');
    var track = document.getElementById('ticker-track');
    inner.style.animation = 'none';
    inner.textContent = text;
    requestAnimationFrame(function() {
      var tW = Math.round(track.offsetWidth);
      var iW = Math.round(inner.scrollWidth);
      var dur = ((tW + iW) / 80).toFixed(1) + 's';
      inner.style.setProperty('--ts', tW + 'px');
      inner.style.setProperty('--te', '-' + iW + 'px');
      inner.style.animation = 'ticker ' + dur + ' linear infinite';
    });
  }

  // â”€â”€ Render map â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function renderMap(storms) {
    if (!mapInited || !stormLayer) return;
    stormLayer.clearLayers();
    var bounds = [];
    storms.forEach(function(s) {
      if (!s.lat && !s.lon) return;
      var col = CAT_COLOR[s.category] || '#888';
      bounds.push([s.lat, s.lon]);
      if (s.movement_spd > 0) {
        var tip = arrowTip(s.lat, s.lon, s.movement_dir, 3);
        L.polyline([[s.lat, s.lon], tip], { color: col, weight: 2, opacity: 0.5, dashArray: '5 5' }).addTo(stormLayer);
      }
      L.circleMarker([s.lat, s.lon], {
        radius: s.category.startsWith('C') ? 14 : 9, color: col,
        weight: 2, fillColor: col, fillOpacity: 0.2, opacity: 0.9
      }).bindPopup(
        '<div class="pop-cat" style="color:' + col + '">' + esc(CAT_LABEL[s.category]) + '</div>' +
        '<div class="pop-name">' + esc(s.name) + '</div>' +
        '<div class="pop-row"><span>Winds</span><span>' + s.winds_mph + ' mph (' + s.winds_kt + ' kt)</span></div>' +
        '<div class="pop-row"><span>Pressure</span><span>' + s.pressure + ' mb</span></div>' +
        '<div class="pop-row"><span>Location</span><span>' + esc(s.lat_str) + ' ' + esc(s.lon_str) + '</span></div>' +
        (s.movement_spd ? '<div class="pop-row"><span>Moving</span><span>' + dirText(s.movement_dir) + ' at ' + s.movement_spd + ' mph</span></div>' : '') +
        '<div style="font-size:0.6rem;color:#334;margin-top:6px">' + esc(s.last_update) + '</div>'
      ).addTo(stormLayer);
      L.marker([s.lat, s.lon], {
        icon: L.divIcon({
          className: '',
          html: '<div style="color:' + col + ';font-size:0.65rem;font-weight:900;white-space:nowrap;' +
                'text-shadow:0 0 4px #000,0 0 8px #000;transform:translate(-50%,-26px)">' + esc(s.name) + '</div>',
          iconAnchor: [0, 0]
        }), interactive: false
      }).addTo(stormLayer);
    });
    if (bounds.length === 1) stormMap.setView(bounds[0], 5);
    else if (bounds.length > 1) stormMap.fitBounds(bounds, { padding: [60, 60] });
  }

  // â”€â”€ Render storm cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function renderStorms(storms) {
    var sec  = document.getElementById('storms-section');
    var none = document.getElementById('no-storms');
    sec.querySelectorAll('.card').forEach(function(c){ c.remove(); });

    if (!storms.length) {
      none.style.display = 'flex';
      document.body.classList.remove('major-active');
      return;
    }
    none.style.display = 'none';

    // Strongest first
    var sorted = storms.slice().sort(function(a,b){ return b.winds_mph - a.winds_mph; });
    var hasMajor = sorted.some(function(s){ return s.category==='C4'||s.category==='C5'; });
    if (hasMajor) document.body.classList.add('major-active');
    else          document.body.classList.remove('major-active');

    sorted.forEach(function(s) {
      var col  = CAT_COLOR[s.category] || '#888';
      var txtc = CAT_TEXT[s.category]  || '#fff';
      var div  = document.createElement('div');
      div.className = 'card ' + (CAT_CLASS[s.category] || 'cat-lo');
      div.innerHTML =
        '<span class="basin-pill">' + esc(BASIN_NAME[s.basin] || s.basin) + '</span>' +
        '<div class="card-badge" style="background:' + col + ';color:' + txtc + '">' +
          esc(CAT_LABEL[s.category] || s.category) + '</div>' +
        '<div class="card-name" style="color:' + col + '">' + esc(s.name) + '</div>' +
        '<div class="card-loc">&#x1F4CD; ' + esc(s.lat_str) + '&nbsp;&nbsp;' + esc(s.lon_str) + '</div>' +
        '<div class="card-stats">' +
          '<span><strong>' + s.winds_mph + ' mph</strong> winds</span>' +
          '<span><strong>' + s.winds_kt  + ' kt</strong> knots</span>' +
          '<span><strong>' + s.pressure  + ' mb</strong> pressure</span>' +
        '</div>' +
        (s.movement_spd
          ? '<div class="card-move">&#x2794; Moving <strong style="color:#aaa">' +
            dirText(s.movement_dir) + '</strong> at ' + s.movement_spd + ' mph</div>'
          : '') +
        '<div class="card-times"><span><strong>Updated:</strong> ' + esc(s.last_update) + '</span></div>';
      // Click to open map and pan
      (function(storm) {
        div.onclick = function() {
          if (!mapOpen) toggleMap();
          setTimeout(function(){
            if (stormMap) stormMap.setView([storm.lat, storm.lon], 6);
          }, mapOpen ? 0 : 100);
        };
      })(s);
      sec.appendChild(div);
    });
  }

  // â”€â”€ Stats bar â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  function updateStats(storms) {
    var hurr  = storms.filter(function(s){ return s.category.startsWith('C'); });
    var major = storms.filter(function(s){ return s.category==='C3'||s.category==='C4'||s.category==='C5'; });
    var ts    = storms.filter(function(s){ return s.category==='TS'; });
    var basins= new Set(storms.map(function(s){ return s.basin; }));
    document.getElementById('storm-count').textContent  = storms.length;
    document.getElementById('stat-hurr').textContent    = hurr.length;
    document.getElementById('stat-major').textContent   = major.length;
    document.getElementById('stat-ts').textContent      = ts.length;
    document.getElementById('stat-basins').textContent  = basins.size;
  }

  // â”€â”€ Fetch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  var lastStorms = [];
  function load() {
    fetch('/hurricane-data')
      .then(function(r){ return r.json(); })
      .then(function(d) {
        lastStorms = d.storms || [];
        updateStats(lastStorms);
        updateTicker(lastStorms);
        renderStorms(lastStorms);
        renderMap(lastStorms);
        document.getElementById('poll-time').textContent = 'NHC data: ' + (d.updated || 'â€”');
      })
      .catch(function(){
        document.getElementById('poll-time').textContent = 'Failed to reach NHC';
      });
  }

  load();
  setInterval(load, 8000);
  // Re-render ticker/ages every 60s
  setInterval(function(){ updateTicker(lastStorms); }, 60000);

  // Auto-reload when server restarts
  (function(){ var s=null; setInterval(function(){
    fetch('/health').then(function(r){return r.json();}).then(function(d){
      if(!s){s=d.server_start;return;} if(d.server_start!==s)location.reload();
    }).catch(function(){});
  }, 30000); })();
</script>
<style>.home-btn{position:fixed;bottom:14px;right:14px;z-index:200;background:#111;border:1px solid #2a2a2a;color:#555;font-size:.72rem;padding:5px 13px;border-radius:5px;text-decoration:none;opacity:.6;transition:opacity .2s}.home-btn:hover{opacity:1;color:#aaa;border-color:#555}</style>
<a class="home-btn" href="http://localhost:8080/">Home</a>
</body>
</html>"""


@app.route("/hurricane")
def hurricane():
    return HURRICANE_PAGE


@app.route("/hurricane-data")
def hurricane_data():
    with _hurricane_lock:
        snap = list(_storms)
    now = datetime.now(tz=CST).strftime("%-I:%M %p %Z")
    return json.dumps({"storms": snap, "updated": now}), 200, \
           {"Content-Type": "application/json"}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#  WINTER STORM ROUTES
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.route("/winter")
def winter():
    return WINTER_PAGE


@app.route("/winter-data")
def winter_data():
    with _winter_lock:
        snap = list(_winter_alerts)
    # strip geometry before sending to the dashboard cards (saves bandwidth)
    slim = [{k: v for k, v in a.items() if k != "geometry"} for a in snap]
    now = datetime.now(tz=CST).strftime("%-I:%M %p %Z")
    return json.dumps({"alerts": slim, "updated": now}), 200, \
           {"Content-Type": "application/json"}


@app.route("/winter-geo")
def winter_geo():
    """Return active winter alerts with geometry for the map layer."""
    with _winter_lock:
        snap = list(_winter_alerts)
    # geometry is already included in each alert dict from fetch_winter_alerts()
    return json.dumps({"alerts": snap}), 200, \
           {"Content-Type": "application/json"}


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  VECTOR STATUS CARD
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

VECTOR_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vector</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg'><text y='32' font-size='32'>ðŸ¤–</text></svg>">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #08080f; color: #ddd;
  font-family: 'Segoe UI', Arial, sans-serif;
  min-height: 100vh; display: flex; flex-direction: column;
  padding-bottom: 48px;
}
header {
  background: #0d0d1a; border-bottom: 2px solid #2a2a4a;
  padding: 10px 18px; display: flex; align-items: center;
  justify-content: space-between; flex-shrink: 0;
}
header h1 { font-size: 1.1rem; font-weight: 800; letter-spacing: 0.08em;
            text-transform: uppercase; color: #8888ff; }
#clock { font-size: 0.75rem; color: #555; }

/* â”€â”€ Conversation state pill â”€â”€ */
#conv-state {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 12px; border-radius: 20px; font-size: 0.7rem;
  font-weight: 800; letter-spacing: 0.12em; text-transform: uppercase;
  transition: background 0.3s, color 0.3s;
}
#conv-state.idle     { background: #1a1a1a; color: #444; }
#conv-state.heard    { background: #2a2600; color: #ffee66; }
#conv-state.thinking { background: #002a2a; color: #44ddcc; animation: pulse-teal 1s ease-in-out infinite; }
#conv-state.speaking { background: #00152a; color: #66ccff; animation: pulse-blue 0.8s ease-in-out infinite; }
#conv-state.ready    { background: #002200; color: #44cc88; }
@keyframes pulse-teal { 0%,100%{opacity:1} 50%{opacity:0.4} }
@keyframes pulse-blue { 0%,100%{opacity:1} 50%{opacity:0.5} }

/* â”€â”€ Layout â”€â”€ */
.main { flex: 1; display: grid; padding: 14px; gap: 14px;
        grid-template-columns: 1fr 1fr;
        grid-template-rows: auto auto auto; }
@media (max-width: 700px) { .main { grid-template-columns: 1fr; } }

/* â”€â”€ Cards â”€â”€ */
.card { background: #0e0e1c; border: 1px solid #1e1e36;
        border-radius: 8px; overflow: hidden; }
.card-head { padding: 8px 14px; font-size: 0.65rem; font-weight: 800;
             letter-spacing: 0.12em; text-transform: uppercase;
             color: #555; border-bottom: 1px solid #151528; }

/* â”€â”€ Camera â”€â”€ */
#cam-card { grid-column: 1; grid-row: 1 / 3; }
#cam-wrap { position: relative; background: #000;
            display: flex; align-items: center; justify-content: center;
            min-height: 220px; }
#cam-img { width: 100%; display: block; }
#cam-offline { position: absolute; color: #333; font-size: 0.8rem; text-align: center; }

/* â”€â”€ Status â”€â”€ */
#status-card { grid-column: 2; grid-row: 1; }
.status-grid { display: grid; grid-template-columns: 1fr 1fr;
               gap: 10px; padding: 12px 14px; }
.stat { display: flex; flex-direction: column; gap: 3px; }
.stat-label { font-size: 0.58rem; font-weight: 700; letter-spacing: 0.1em;
              text-transform: uppercase; color: #444; }
.stat-value { font-size: 0.9rem; font-weight: 600; color: #bbb; }
.stat-value.green  { color: #44cc88; }
.stat-value.yellow { color: #ffcc44; }
.stat-value.red    { color: #ff4444; }
.stat-value.dim    { color: #555; }

/* â”€â”€ Battery bar â”€â”€ */
.batt-wrap { display: flex; align-items: center; gap: 8px; }
.batt-bar  { flex: 1; height: 6px; background: #1a1a2a; border-radius: 3px; overflow: hidden; }
.batt-fill { height: 100%; border-radius: 3px; transition: width 0.4s; }

/* â”€â”€ Activity feed â”€â”€ */
#activity-card { grid-column: 2; grid-row: 2; }
#activity-list { padding: 8px 14px; max-height: 260px;
                 overflow-y: auto; display: flex; flex-direction: column; gap: 6px; }
.evt { display: flex; gap: 8px; align-items: flex-start; font-size: 0.78rem; line-height: 1.4; }
.evt-icon { flex-shrink: 0; font-size: 0.9rem; margin-top: 1px; }
.evt-body { flex: 1; }
.evt-type { font-size: 0.58rem; font-weight: 700; letter-spacing: 0.08em;
            text-transform: uppercase; color: #555; margin-bottom: 1px; }
.evt-text { color: #ccc; }
.evt-text.dim { color: #555; font-style: italic; }
.evt-age  { font-size: 0.58rem; color: #383848; margin-top: 2px; }

.evt.heard   .evt-text { color: #ffee66; }
.evt.said    .evt-text { color: #66ccff; }
.evt.sensor  .evt-text { color: #cc88ff; }
.evt.face    .evt-text { color: #44dd88; }
.evt.weather .evt-text { color: #ff8844; }
.evt.thinking .evt-text { color: #555; font-style: italic; }

/* â”€â”€ Mood / last said strip â”€â”€ */
#mood-card { grid-column: 1 / 3; grid-row: 3; }
.mood-inner { padding: 10px 14px; font-size: 0.8rem; color: #777; font-style: italic;
              white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.mood-inner span { color: #aaa; }

/* â”€â”€ Home button â”€â”€ */
.home-btn {
  position: fixed; bottom: 14px; right: 14px; z-index: 200;
  background: #111; border: 1px solid #2a2a2a; color: #555;
  font-size: 0.72rem; padding: 5px 13px; border-radius: 5px;
  text-decoration: none; opacity: 0.6; transition: opacity 0.2s;
}
.home-btn:hover { opacity: 1; color: #aaa; border-color: #555; }
</style>
</head>
<body>
<header>
  <h1>ðŸ¤– Vector</h1>
  <div id="conv-state" class="idle">â¬¤ Idle</div>
  <div id="clock"></div>
</header>

<div class="main">

  <!-- Camera -->
  <div class="card" id="cam-card">
    <div class="card-head">Camera</div>
    <div id="cam-wrap">
      <img id="cam-img" src="" style="display:none" alt="Vector camera">
      <div id="cam-offline">Camera offline</div>
    </div>
  </div>

  <!-- Status -->
  <div class="card" id="status-card">
    <div class="card-head">Status</div>
    <div class="status-grid">
      <div class="stat">
        <div class="stat-label">Battery</div>
        <div class="batt-wrap">
          <div class="batt-bar"><div class="batt-fill" id="batt-fill" style="width:0%"></div></div>
          <div class="stat-value" id="batt-pct">â€”</div>
        </div>
      </div>
      <div class="stat">
        <div class="stat-label">Charger</div>
        <div class="stat-value" id="charger-val">â€”</div>
      </div>
      <div class="stat">
        <div class="stat-label">Carried</div>
        <div class="stat-value" id="pickup-val">â€”</div>
      </div>
      <div class="stat">
        <div class="stat-label">Brain</div>
        <div class="stat-value" id="model-val">â€”</div>
      </div>
      <div class="stat">
        <div class="stat-label">Seeing</div>
        <div class="stat-value" id="face-val">â€”</div>
      </div>
      <div class="stat">
        <div class="stat-label">Quiet mode</div>
        <div class="stat-value" id="quiet-val">â€”</div>
      </div>
    </div>
  </div>

  <!-- Activity feed -->
  <div class="card" id="activity-card">
    <div class="card-head">Activity</div>
    <div id="activity-list"></div>
  </div>

  <!-- Mood strip -->
  <div class="card" id="mood-card">
    <div class="card-head">Mood</div>
    <div class="mood-inner" id="mood-text">Loading...</div>
  </div>

</div>

<a class="home-btn" href="http://localhost:8080/">Home</a>

<script>
var VECTOR_AI = '{{ vector_ai_url }}';

// â”€â”€ Clock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function tick() {
  var n = new Date();
  document.getElementById('clock').textContent =
    n.toLocaleDateString('en-US',{month:'2-digit',day:'2-digit',year:'numeric'}) + '  ' +
    n.toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',second:'2-digit'});
}
tick(); setInterval(tick, 1000);


// â”€â”€ Camera â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
var camImg = document.getElementById('cam-img');
var camOff = document.getElementById('cam-offline');
var camStarted = false;

function startCam() {
  if (camStarted) return;
  camStarted = true;
  camImg.src = VECTOR_AI + '/v1/cam?' + Date.now();
  camImg.onload  = function() { camImg.style.display='block'; camOff.style.display='none'; };
  camImg.onerror = function() {
    camImg.style.display='none'; camOff.style.display='block';
    camStarted = false; setTimeout(startCam, 8000);
  };
}
startCam();

// â”€â”€ Status polling â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function fetchStatus() {
  fetch(VECTOR_AI + '/v1/status').then(function(r){ return r.json(); }).then(function(d) {
    // Battery
    var pct = d.battery_pct;
    if (pct !== null && pct !== undefined) {
      var p = Math.round(pct * 100);
      document.getElementById('batt-pct').textContent  = p + '%';
      var fill = document.getElementById('batt-fill');
      fill.style.width = p + '%';
      fill.style.background = p > 50 ? '#44cc88' : p > 20 ? '#ffcc44' : '#ff4444';
    }
    // Charger
    var chEl = document.getElementById('charger-val');
    if (d.on_charger === true)       { chEl.textContent='On charger'; chEl.className='stat-value green'; }
    else if (d.on_charger === false) { chEl.textContent='Off charger'; chEl.className='stat-value yellow'; }
    else                             { chEl.textContent='â€”'; chEl.className='stat-value dim'; }
    // Picked up
    var puEl = document.getElementById('pickup-val');
    if (d.is_picked_up === true)  { puEl.textContent='Picked up'; puEl.className='stat-value yellow'; }
    else if (d.is_picked_up === false) { puEl.textContent='On desk'; puEl.className='stat-value green'; }
    else { puEl.textContent='â€”'; puEl.className='stat-value dim'; }
    // Model
    var mEl = document.getElementById('model-val');
    if (d.model_loaded) { mEl.textContent='Loaded'; mEl.className='stat-value green'; }
    else                { mEl.textContent='Cold'; mEl.className='stat-value dim'; }
    // Face
    var fEl = document.getElementById('face-val');
    var face = d.face;
    if (face && !face.is_stranger && face.name) {
      fEl.textContent = face.name; fEl.className = 'stat-value green';
    } else if (face && face.is_stranger) {
      fEl.textContent = 'Stranger'; fEl.className = 'stat-value yellow';
    } else {
      fEl.textContent = 'Nobody'; fEl.className = 'stat-value dim';
    }
    // Quiet mode
    var qEl = document.getElementById('quiet-val');
    if (d.quiet_mode) { qEl.textContent='On'; qEl.className='stat-value yellow'; }
    else              { qEl.textContent='Off'; qEl.className='stat-value dim'; }
    // Mood
    if (d.mood) document.getElementById('mood-text').innerHTML =
      '<span>Vector feels:</span> ' + esc(d.mood);
  }).catch(function(){});
}
fetchStatus();
setInterval(fetchStatus, 15000);

// â”€â”€ Activity SSE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
var _evtRetry = null;
var _ageTimer = null;

// â”€â”€ Conversation state pill â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
var _readyTimer = null;
var _stateEl = document.getElementById('conv-state');
var STATE_LABELS = {
  idle:     'â¬¤ Idle',
  heard:    'ðŸŽ¤ Heard',
  thinking: 'âŸ³ Thinking',
  speaking: 'ðŸ”Š Speaking',
  ready:    'âœ“ Ready',
};
function setConvState(s) {
  _stateEl.className = 'idle'; // reset
  _stateEl.className = s;
  _stateEl.textContent = STATE_LABELS[s] || s;
  if (_readyTimer) { clearTimeout(_readyTimer); _readyTimer = null; }
  // Auto-fade back to idle after 8s of "ready"
  if (s === 'ready') {
    _readyTimer = setTimeout(function(){ setConvState('idle'); }, 8000);
  }
}

var ICONS = {
  heard:    'ðŸŽ¤',
  thinking: 'ðŸ’­',
  said:     'ðŸ”Š',
  sensor:   'âœ‹',
  face:     'ðŸ‘¤',
  weather:  'ðŸŒªï¸',
  battery:  'ðŸ”‹',
  ready:    null,
  ping:     null,
};
var LABELS = {
  heard: 'Heard', thinking: 'Thinking', said: 'Said',
  sensor: 'Sensor', face: 'Face', weather: 'Weather', battery: 'Battery',
};

var _events = [];   // {type, ts, ...data}

function evtText(e) {
  if (e.type === 'heard')    return esc(e.text || '');
  if (e.type === 'said')     return esc(e.text || '');
  if (e.type === 'thinking') return 'Processing...';
  if (e.type === 'sensor') {
    var evName = e.event || '';
    var map = {pickup:'Picked up', putdown:'Put down', pet:'Being petted'};
    var t = map[evName] || evName;
    return t + (e.response ? ' â€” <em>' + esc(e.response) + '</em>' : '');
  }
  if (e.type === 'face') {
    return e.stranger ? 'Stranger detected' : 'Recognized: ' + esc(e.name || '');
  }
  if (e.type === 'weather') {
    return esc(e.event_type || '') + (e.area ? ' â€” ' + esc(e.area) : '');
  }
  if (e.type === 'battery') {
    var p = e.pct !== null && e.pct !== undefined ? Math.round(e.pct*100)+'%' : 'â€”';
    return p + (e.charging ? ' (charging)' : '');
  }
  return '';
}

function ageStr(ts) {
  if (!ts) return '';
  var s = Math.floor((Date.now()/1000) - ts);
  if (s < 5)   return 'just now';
  if (s < 60)  return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderEvents() {
  var list = document.getElementById('activity-list');
  // Show last 20, newest first
  var visible = _events.slice(-20).reverse();
  list.innerHTML = visible.map(function(e) {
    if (e.type === 'ping' || !ICONS[e.type]) return '';
    var icon  = ICONS[e.type] || 'â€¢';
    var label = LABELS[e.type] || e.type;
    var txt   = evtText(e);
    if (!txt && e.type !== 'thinking') return '';
    return '<div class="evt ' + esc(e.type) + '">'
      + '<div class="evt-icon">' + icon + '</div>'
      + '<div class="evt-body">'
      + '<div class="evt-type">' + esc(label) + '</div>'
      + '<div class="evt-text' + (e.type==='thinking'?' dim':'') + '">' + txt + '</div>'
      + '<div class="evt-age" data-ts="' + (e.ts||0) + '">' + ageStr(e.ts) + '</div>'
      + '</div></div>';
  }).join('');
}

function connectSSE() {
  if (_evtRetry) { clearTimeout(_evtRetry); _evtRetry = null; }
  var src = new EventSource(VECTOR_AI + '/v1/activity/stream');
  src.onmessage = function(e) {
    try {
      var d = JSON.parse(e.data);
      if (d.type === 'ping') return;

      // Drive conversation state pill
      if      (d.type === 'heard')    setConvState('heard');
      else if (d.type === 'thinking') setConvState('thinking');
      else if (d.type === 'said')     setConvState('speaking');
      else if (d.type === 'ready')    setConvState('ready');

      _events.push(d);
      if (_events.length > 200) _events = _events.slice(-200);
      renderEvents();
      if (d.type === 'battery') fetchStatus();
    } catch(err) {}
  };
  src.onerror = function() {
    src.close();
    _evtRetry = setTimeout(connectSSE, 6000);
  };
}
connectSSE();

// Update ages every 20 seconds without re-rendering the whole list
if (_ageTimer) clearInterval(_ageTimer);
_ageTimer = setInterval(function() {
  document.querySelectorAll('.evt-age[data-ts]').forEach(function(el) {
    var ts = parseFloat(el.getAttribute('data-ts'));
    if (ts) el.textContent = ageStr(ts);
  });
}, 20000);
</script>
</body>
</html>"""


@app.route("/vector")
def vector_card():
    return render_template_string(VECTOR_PAGE, vector_ai_url=VECTOR_AI_URL or "")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  Â§13  ENTRY POINT
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

if __name__ == "__main__":
    db_init()
    threading.Thread(target=poller,           daemon=True).start()
    threading.Thread(target=lsr_poller,       daemon=True).start()
    threading.Thread(target=hurricane_poller, daemon=True).start()
    threading.Thread(target=winter_poller,    daemon=True).start()
    log.info("Tornado Dashboard starting on port %d", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)


