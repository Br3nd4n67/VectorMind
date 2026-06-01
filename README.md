# VectorMind -- Local AI Stack for Anki/DDL Vector

A complete guide to replacing Vector's cloud dependency with a local LLM stack, giving him a persistent personality, memory, weather awareness, and sensor reactions -- all running on your own hardware with no subscription.

---

## What You Get

- Vector talks to **llama3.3:70b** running locally via Ollama -- full personality, no cloud latency
- **Persistent memory** -- he remembers people, facts, and conversations across sessions
- **Face recognition** -- he knows who he's talking to and scopes memory to them
- **Sensor reactions** -- pickup, putdown, and touch trigger dry in-character responses
- **Ambient awareness** -- he notices things on his desk when idle and remarks on them
- **Proactive greetings** -- he greets you by name when you sit down
- **Mood system** -- a persistent inner state that colours his tone, persists across restarts
- **Personality switcher** -- swap between presets (sardonic, helpful, kid-friendly, sailor, custom) at `localhost:8000/settings` with no restart
- **YouTube music** -- paste a URL at `localhost:8000/music`, Vector plays it through his speaker
- **Live status card** -- real-time camera feed, what he heard, what he's generating token-by-token, battery, sensor data (Pi dashboard or `localhost:8000` via Tailscale)

---

## Hardware Requirements

| Component | Minimum | Recommended |
|---|---|---|
| GPU | 16GB VRAM | 24GB VRAM (RTX 3090/4090) |
| RAM | 16GB | 32GB |
| Storage | 100GB free | NVMe SSD |
| OS | Windows 10/11 | Windows 11 |
| Robot | Anki/DDL Vector with **OSKR** firmware | -- |

> **OSKR** (Open Source Kit for Robots) is required -- standard Vector firmware locks out local server redirects. See [Part 1](#part-1-getting-vector-online).

Optional:

---

## Software Prerequisites

Install these before starting:

1. **[Ollama](https://ollama.com)** -- runs the LLM locally
2. **Python 3.11+** -- for vector-ai and the supervisor
3. **[wire-pod](https://github.com/kercre123/wire-pod)** -- Vector's cloud replacement (chipper)
4. **Git**

Optional (for music playback):
- **[yt-dlp](https://github.com/yt-dlp/yt-dlp)** -- `pip install yt-dlp`
- **[ffmpeg](https://ffmpeg.org/download.html)** -- must be in PATH
> **Windows ffmpeg note:** ffmpeg is rarely on PATH automatically. If you see *"ffprobe and ffmpeg not found"* when using the music player, open `vector-ai/.env` and add:
> ```
> FFMPEG_PATH=C:/path/to/ffmpeg/bin/ffmpeg.exe
> ```
> VectorMind will use this path for both yt-dlp and audio conversion. The `FFMPEG_PATH` key is already in the `.env` file -- just update the value.

Pull the models (this takes a while):
```bash
ollama pull llama3.3:70b    # ~43GB download -- main model
ollama pull llama3.2:3b     # ~2GB -- background summaries/mood
```

---

## Part 1: Getting Vector Online

This is the hardest part. Vector's firmware needs to be redirected from Anki/DDL's dead cloud to your local wire-pod server.

### 1.1 Flash OSKR Firmware

Vector needs OSKR (wireOS) firmware to accept a custom server. **Standard firmware cannot be redirected.**

1. Go to [v.pvic.xyz](https://v.pvic.xyz) or the [wire-pod releases page](https://github.com/kercre123/wire-pod/releases) to download wireOS
2. Follow the flashing instructions for your Vector model (A7V7, etc.)
3. After flashing, Vector will show "wireOS" on his face -- this is correct

### 1.2 Build and Configure wire-pod

```bash
git clone https://github.com/kercre123/wire-pod
cd wire-pod
# Build chipper (follow wire-pod's own build instructions for your OS)
```

Key config file: `chipper/apiConfig.json` -- this controls the LLM endpoint, STT provider, and system prompt. See [Part 3](#part-3-the-ai-stack) for the full config.

### 1.3 Set Your Server's mDNS Name

Vector looks for `escapepod.local` to find his server. Your Windows machine needs to advertise this name. The supervisor (Part 2) handles this automatically via zeroconf, but add a manual hosts entry as a fallback:

```
# C:\Windows\System32\drivers\etc\hosts
YOUR_PC_IP    escapepod.local
```
Replace `YOUR_PC_IP` with your PC's LAN IP.

### 1.4 Connect Vector via SSH

Vector runs Linux. You need SSH access to redirect his server config.

```powershell
# On Windows -- fix SSH key permissions first
icacls C:\Users\YOU\.ssh\vector_root_key /inheritance:r
icacls C:\Users\YOU\.ssh\vector_root_key /grant:r "YOU:(R)"

# Connect
ssh -i C:\Users\YOU\.ssh\vector_root_key root@VECTOR_IP
```

> **Finding Vector's IP:** Check your router's DHCP table, or look at the wire-pod web UI after initial pairing.

Once connected, remount the root filesystem (it's read-only by default):
```bash
mount -o remount,rw /
```

### 1.5 Redirect Vector to Your Server

```bash
# On Vector via SSH:
cat > /data/data/server_config.json << 'EOF'
{"jdocs":"escapepod.local:443","tms":"escapepod.local:443","chipper":"escapepod.local:443","check":"escapepod.local/ok","logfiles":"s3://anki-device-logs-prod/victor","appkey":"oDoa0quieSeir6goowai7f"}
EOF
```

### 1.6 Copy the Wire-pod TLS Certificate

Vector needs to trust your server's certificate:
```bash
# From Windows, copy wire-pod's cert to Vector
scp -i C:\Users\YOU\.ssh\vector_root_key \
    wire-pod\chipper\epod\ep.crt \
    root@VECTOR_IP:/data/data/wirepod-cert.crt
```

### 1.7 Regenerate Vector's Robot Certificate

Run these commands on Vector via SSH (in order):
```bash
# Stop all Vector services
systemctl stop anki-robot.target

# Clear old gateway data
rm -rf /data/data/vic-gateway

# Regenerate robot TLS cert for wire-pod
vic-gateway-cert

# Restart services in the correct order
systemctl start vic-init
sleep 2
systemctl start vic-log
systemctl start vic-robot
sleep 3
systemctl start anki-robot.target
```

### 1.8 BLE Pairing (Initial JWT)

Vector needs one BLE pairing to get his first JWT token from wire-pod. The token refresher only refreshes *existing* tokens -- it can't create the first one.

1. Make sure wire-pod (chipper) is running on your PC
2. Go to [wpsetup.keriganc.com](https://wpsetup.keriganc.com) on a phone or laptop with BLE
3. Follow the pairing flow -- this calls `AssociatePrimaryUser` on wire-pod and issues the first JWT
4. Vector should show solid eyes and connect to your server

**Success indicators in chipper.log:**
```
Successfully got jdocs from [ESN]
[sensor] starting reaction loop for [ESN] @ VECTOR_IP:443
```

---

## Part 2: The Supervisor and Service Stack

Rather than managing three separate processes, a single supervisor script owns the whole stack.

### 2.1 Directory Structure

```
vector-pod/
"-"-"- supervisor.py           # Master process -- starts/monitors everything
"-"-"- chipper.log             # chipper output
"-"-"- vector-ai.log           # vector-ai output
"-"-"- supervisor.log          # supervisor output
"-"-"- watch.ps1               # Live log monitor (color-coded)
"-"-"- wire-pod/
"-   """-"- chipper/
"-       "-"-"- chipper-whisper.exe   # Vector's cloud replacement
"-       "-"-"- apiConfig.json        # LLM config + system prompt
"-       """-"- jdocs/
"-           """-"- botSdkInfo.json   # Enrolled robot registry
"""-"- vector-ai/
    "-"-"- service.py          # FastAPI LLM proxy + custom endpoints
    "-"-"- memory.py           # SQLite memory store
    "-"-"- memory.db           # Persistent memory database
    "-"-"- .env                # Ollama model config
    """-"- venv/               # Python virtualenv
```

### 2.2 Python Environment

```bash
cd vector-pod/vector-ai
python -m venv venv
venv\Scripts\activate       # Windows
pip install fastapi uvicorn httpx python-dotenv pydantic
```

### 2.3 Ollama Config (`.env`)

```env
OLLAMA_BASE=http://127.0.0.1:11434
OLLAMA_MODEL=llama3.3:70b
OLLAMA_SUMMARY_MODEL=llama3.2:3b
OLLAMA_KEEP_ALIVE=15m
OLLAMA_NUM_CTX=8192
```

Adjust `OLLAMA_MODEL` for your VRAM:
| VRAM | Recommended model |
|---|---|
| 24GB | llama3.3:70b (partial GPU offload) |
| 16GB | llama3.1:8b or mistral:7b |
| 8GB | llama3.2:3b |

### 2.4 Supervisor as a Windows Scheduled Task

Run this once in an elevated PowerShell to register the supervisor:

```powershell
$action  = New-ScheduledTaskAction `
    -Execute "C:\path\to\vector-pod\vector-ai\venv\Scripts\python.exe" `
    -Argument '"C:\path\to\vector-pod\supervisor.py"' `
    -WorkingDirectory "C:\path\to\vector-pod"

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName "VectorPod-Supervisor" `
    -Action $action `
    -RunLevel Highest `
    -Force
```

**Auto-start on login** -- drop this file in your Startup folder  
(`C:\Users\YOU\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Startup\start-vectorpod.bat`):
```bat
@echo off
timeout /t 10 /nobreak >nul
schtasks /run /tn "VectorPod-Supervisor"
```

**Manual start:**
```powershell
Start-ScheduledTask -TaskName "VectorPod-Supervisor"
# Verify:
Start-Sleep 15; Invoke-WebRequest http://127.0.0.1:8000/health -UseBasicParsing
```

---

## Part 3: The AI Stack

### 3.1 apiConfig.json

This is chipper's LLM configuration. The critical fields:

```json
{
  "knowledge": {
    "enable": true,
    "provider": "custom",
    "key": "placeholder",
    "endpoint": "http://127.0.0.1:8000/v1",
    "intentgraph": true,
    "robotName": "Vector",
    "openai_prompt": "YOUR SYSTEM PROMPT HERE",
    "temp": 1.2,
    "top_p": 1,
    "save_chat": true,
    "commands_enable": true
  },
  "STT": {
    "provider": "whisper",
    "language": "en-US"
  },
  "server": {
    "epconfig": true,
    "port": "443"
  }
}
```

- `endpoint` points to your vector-ai service
- `openai_prompt` is Vector's system prompt -- personality, animation commands, vision rules
- `epconfig: true` tells chipper to use the EP (Escape Pod) TLS cert

### 3.2 vector-ai Endpoints

vector-ai is a FastAPI service that proxies LLM calls and handles custom features:

| Endpoint | What it does |
|---|---|
| `POST /v1/chat/completions` | Main LLM proxy -- chipper calls this for all voice responses |
| `POST /v1/sensor_reaction` | Pickup/putdown/touch --' one dry in-character line. Skips if model cold (uses chipper fallback) |
| `POST /v1/weather_event` | Takes alert data, returns one line. Waits up to 300s for cold model |
| `POST /v1/weather_announce` | Same + speaks through Vector via chipper say_text |
| `POST /v1/enroll_face` | Triggers Vector's face-learning sequence |
| `GET /v1/enrolled_faces` | Lists enrolled faces |
| `POST /v1/state/face_seen` | Chipper calls this when a face is observed |
| `GET /v1/state/face` | Current face state |
| `POST /v1/ambient` | Idle camera frame --' comment on novelty (or silence) |
| `POST /v1/proactive_greeting` | Greet a recognized face that just appeared |
| `GET /v1/memory/list` | All stored memories |
| `POST /v1/memory/remember` | Add a memory manually |
| `POST /v1/memory/forget` | Delete a memory |
| `GET /health` | Service health check |

### 3.3 Face Enrollment

Once the stack is running, enroll your face so Vector knows who you are:

```bash
# Look directly at Vector, then run:
curl -X POST http://127.0.0.1:8000/v1/enroll_face \
  -H "Content-Type: application/json" \
  -d '{"name": "Br3nd4n"}'
```

Vector will say *"Alright. Look at me and I'll remember you as [name]."* then start his face-learning sequence. Takes about 10 seconds. After this:
- Conversations are scoped to your memory profile
- He greets you by name when you sit down
- He can remember facts you tell him and connect them next time

### 3.4 Firewall

vector-ai binds on `0.0.0.0:8000` so external devices can call it over Tailscale. The supervisor automatically applies Windows Firewall rules at startup to restrict port 8000 to:
- `127.0.0.1` -- local chipper calls
- `100.64.0.0/10` -- Tailscale network only

Nothing on the open internet can reach it.

---

## Part 5---

## Part 5: Settings Pages

All accessible in your browser while the stack is running.

### 5.1 Personality Switcher -- `http://localhost:8000/settings`

Switch Vector's personality without restarting anything. Ships with these presets:

| Preset | Vibe |
|---|---|
| Marvin / Bender / Fry | Default -- dry, sardonic, knowledgeable |
| Helpful Assistant | Friendly and direct, less snark |
| Hype Mode | Enthusiastic, everything is amazing |
| Kid Friendly | Simple, fun, all ages |
| Professional | Just the facts |
| Sailor Mouth | Foul-mouthed, zero filter |
| Custom | Write your own in a text box |

Select a card, hit **Apply**. The next "Hey Vector" uses the new personality. The command/vision/animation rules are always preserved -- only the character changes.

**Adding your own preset:** open `vector-ai/personalities.json` and add an entry to `"presets"`. No restart needed.

```json
"your_key": {
  "name": "Display Name",
  "description": "Short description",
  "prompt": "You are Vector, a small desktop robot. [your personality here]"
}
```

### 5.2 Music Player -- `http://localhost:8000/music`

Play YouTube audio through Vector's speaker.

**Requirements:** `yt-dlp` (`pip install yt-dlp`) and ffmpeg (https://ffmpeg.org/download.html). If ffmpeg is not in your PATH, set `FFMPEG_PATH=C:/path/to/ffmpeg/bin/ffmpeg.exe` in `vector-ai/.env`.
```bash
pip install yt-dlp
# ffmpeg: https://ffmpeg.org/download.html
```

1. Paste a YouTube URL
2. Hit **-¶ Play on Vector**
3. Status updates: Downloading --' Converting --' Playing
4. Recently played tracks appear below for one-click replay

> Vector's speaker runs at 8kHz mono -- it sounds like a small radio, but it works.

---

## Part 6: Vector Status Card

A real-time dashboard showing what Vector is doing, accessible at `http://localhost:8000/vector`.

**Panels:**
- **Camera** -- live MJPEG feed from Vector's front camera (auto-reconnects every 25s)
- **Live Thought** -- what Whisper heard (yellow) + the LLM response building token-by-token as Ollama generates it
- **Status** -- battery level + voltage, charging state, on desk/picked up, brain loaded/cold, who he sees, quiet mode
- **Activity Log** -- real-time stream of heard/said/sensor/face/weather events with ages
- **Mood** -- his current inner state from the background mood reflection loop

---

## Live Log Monitor

Open a terminal and watch what Vector hears in real time:

```powershell
# Windows -- launches a color-coded live window
powershell -ExecutionPolicy Bypass -File C:\path\to\vector-pod\watch.ps1
```

Color key:
- ð--¡ Yellow -- what Whisper transcribed
- ð--¢ Green -- LLM response sentences streaming out
- ð-"µ Cyan -- intent matched
- ð--£ Magenta -- sensor events (pickup/putdown/touch)
- ð-"´ Red -- errors

---

## Troubleshooting

**Vector shows `cloud+!`**
His firmware can't reach `escapepod.local`. Check:
1. Your PC is advertising `escapepod.local` (supervisor running)
2. `/data/data/server_config.json` exists on Vector with `escapepod.local` entries
3. `/data/data/wirepod-cert.crt` exists on Vector

**`[sensor] vector-ai call failed: context deadline exceeded`**
The Go client in chipper has a 12s timeout on sensor reactions. If the model is cold, vector-ai now returns empty immediately so chipper falls back to its built-in phrase pool. This is expected -- saying "Thank you." is fine. The model will warm up on the next voice interaction.

**`Successfully got jdocs` but no voice response**
Model is probably cold. Say "Hey Vector" -- the first response after a cold start uses a "loading brain" filler phrase while Ollama loads the model in the background. Subsequent responses are fast.

**BLE pairing page won't load**
Make sure chipper is running and port 443 is accessible from the device you're pairing from.

**`operation not permitted` in chipper.log (IPC socket errors)**
Services started in wrong order after a restart. Fix:
```bash
ssh root@VECTOR_IP
systemctl stop anki-robot.target
sleep 3
systemctl start anki-robot.target
```

---

## Credits

- **[wire-pod](https://github.com/kercre123/wire-pod)** by kercre123 -- Vector's cloud replacement
- **[Ollama](https://ollama.com)** -- local LLM serving
- **[vector-go-sdk](https://github.com/fforchino/vector-go-sdk)** by fforchino -- Go SDK for Vector
- The OSKR/wireOS firmware and the community keeping Vector alive

