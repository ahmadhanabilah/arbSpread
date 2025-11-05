# unified_backend.py
import os, json, asyncio, logging, subprocess, csv, re
from typing import Dict, Any
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

from helper_lighter_web import LighterAPI
from helper_extended_web import ExtendedAPI
from telegram_api import send_telegram_message

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("unified_backend")

# --- Auth ---
ADMIN_USER = os.getenv("PANEL_USER", "admin")
ADMIN_PASS = os.getenv("PANEL_PASS", "changeme")

security = HTTPBasic()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != ADMIN_USER or credentials.password != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

# --- Paths ---
CONFIG_PATH = "config.json"
ENV_PATH = ".env"

# --- App Init ---
app = FastAPI(title="arbSpread Unified Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import base64

class SSEAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # only patch if ?auth= present and no Authorization header
        if "auth" in request.query_params and "authorization" not in request.headers:
            auth = request.query_params["auth"]
            # Inject header into scope (works reliably)
            request.scope["headers"].append(
                (b"authorization", f"Basic {auth}".encode())
            )
        return await call_next(request)

app.add_middleware(SSEAuthMiddleware)

# Optional: serve frontend build later
if os.path.exists("frontend/dist"):
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="frontend")

# --- File utils ---
def read_json(path): return json.load(open(path))
def write_json(path, data): json.dump(data, open(path, "w"), indent=2)
def read_text(path): return open(path, encoding="utf-8").read() if os.path.exists(path) else ""
def write_text(path, text): open(path, "w", encoding="utf-8").write(text)

# --- Screen helpers ---
# allow only letters, numbers, underscore, hyphen; length 1..20
SYMBOL_RE = re.compile(r"^[A-Za-z0-9_-]{1,20}$")

def list_running_screens():
    try:
        res = subprocess.run(["screen", "-ls"], capture_output=True, text=True, check=False)
        out = res.stdout or ""
        sessions = []
        for line in out.splitlines():
            # typical line: "\t1234.arb_BTC\t(Detached)"
            parts = line.strip().split()
            if not parts:
                continue
            # find token with dot e.g. 1234.arb_BTC
            for token in parts:
                if "." in token:
                    # token like 1234.arb_BTC
                    try:
                        name = token.split(".", 1)[1]
                        if name.startswith("arb_"):
                            sessions.append(name)
                    except Exception:
                        continue
        return sessions
    except Exception:
        return []

def start_screen(symbolL: str, symbolE: str):
    session_name = f"arb_{symbolL}_{symbolE}"
    running = list_running_screens()
    if session_name in running:
        return False

    cmd = ["screen", "-dmS", session_name, "python3", "-u", "main.py", symbolL, symbolE]
    subprocess.run(cmd, check=False)
    return True

def stop_screen(symbolL: str, symbolE: str):
    try:
        res = subprocess.run(["screen", "-ls"], capture_output=True, text=True, check=False)
        out = res.stdout or ""
        for line in out.splitlines():
            if f"arb_{symbolL}_{symbolE}" in line and "." in line:
                tokens = line.strip().split()
                for token in tokens:
                    if f"arb_{symbolL}_{symbolE}" in token and "." in token:
                        sid = token.split(".", 1)[0].strip()
                        # quit the session safely
                        subprocess.run(["screen", "-S", sid, "-X", "quit"], check=False)
                        return True
        return False
    except Exception as e:
        logger.error(f"stop_screen error: {e}")
        return False


# --- Models ---
class ConfigPayload(BaseModel):
    data: Dict[str, Any]
class EnvPayload(BaseModel):
    text: str

# =====================================================
# ================ PANEL ROUTES =======================
# =====================================================

@app.get("/api/symbols", dependencies=[Depends(require_auth)])
async def get_symbols():
    cfg = read_json(CONFIG_PATH)
    syms = [{"symbolL": s["symbolL"], "symbolE": s["symbolE"]} for s in cfg.get("symbols", [])]
    return {"symbols": syms, "running": list_running_screens()}

@app.post("/api/start", dependencies=[Depends(require_auth)])
async def start_bot(symbolL: str, symbolE: str):
    start_screen(symbolL, symbolE)
    return {"ok": True}

@app.post("/api/stop", dependencies=[Depends(require_auth)])
async def stop_bot(symbolL: str, symbolE: str):
    return {"ok": stop_screen(symbolL, symbolE)}

@app.get("/api/config", dependencies=[Depends(require_auth)])
async def get_config():
    return read_json(CONFIG_PATH)

@app.put("/api/config", dependencies=[Depends(require_auth)])
async def save_config(payload: ConfigPayload):
    write_json(CONFIG_PATH, payload.data)
    return {"ok": True}

@app.get("/api/env", response_class=PlainTextResponse, dependencies=[Depends(require_auth)])
async def get_env():
    return read_text(ENV_PATH)

@app.put("/api/env", dependencies=[Depends(require_auth)])
async def save_env(payload: EnvPayload):
    write_text(ENV_PATH, payload.text)
    return {"ok": True}

@app.post("/api/test-credentials", dependencies=[Depends(require_auth)])
async def test_credentials():
    symbol = "BTC"
    qty = 0.0001
    L, E = LighterAPI(), ExtendedAPI()
    await asyncio.gather(L.init(), E.init(), L.getAllSymbols())
    logs = []
    try:
        l_buy = await L.client.create_order  # dummy call placeholder
        logs.append("✅ Credential test simulated.")
    except Exception as e:
        logs.append(f"❌ Error: {e}")
    await send_telegram_message("🧪 Credential Test Result:\n" + "\n".join(logs))
    return {"ok": True, "logs": logs}




# =====================================================
# ================ DATA ROUTES ========================
# =====================================================
@app.get("/api/auth_check", dependencies=[Depends(require_auth)])
async def auth_check():
    """Simple endpoint to verify username/password"""
    return {"ok": True}

@app.get("/get_ext")
async def get_ext():
    return _read_csv_json("trades_merged_ext.csv")

@app.get("/get_lig")
async def get_lig():
    return _read_csv_json("trades_merged_lig.csv")

@app.get("/get_pnl_ext")
async def get_pnl_ext():
    return _read_csv_json("trades_daily_pnl_ext.csv")

@app.get("/get_pnl_lig")
async def get_pnl_lig():
    return _read_csv_json("trades_daily_pnl_lig.csv")

from fastapi import Request
from fastapi.responses import StreamingResponse
import asyncio

@app.get("/api/live_stream/{symbolL}/{symbolE}", dependencies=[Depends(require_auth)])
async def stream_live(request: Request, symbolL: str, symbolE: str):
    """Stream live.txt updates in real-time using Server-Sent Events (SSE)."""
    live_path = f"logs/{symbolL}_{symbolE}_live.txt"

    async def event_stream():
        last_line = None
        while True:
            if await request.is_disconnected():
                break
            try:
                if os.path.exists(live_path):
                    with open(live_path, "r", encoding="utf-8") as f:
                        data = f.read().strip()

                    if data and data != last_line:
                        last_line = data
                        # ✅ Encode newlines so the browser doesn't split the event
                        safe_data = data.replace("\n", "\\n")
                        yield f"data: {safe_data}\n\n"

                await asyncio.sleep(0.5)  # check twice per second
            except Exception as e:
                yield f"data: [error] {e}\n\n"
                await asyncio.sleep(2)

    return StreamingResponse(event_stream(), media_type="text/event-stream")

@app.get("/api/logs/{symbolL}/{symbolE}", dependencies=[Depends(require_auth)])
async def get_logs(symbolL: str, symbolE: str, lines: int = 100):
    log_path = f"logs/{symbolL}_{symbolE}.log"
    if not os.path.exists(log_path):
        return PlainTextResponse(f"No log file found for {symbolL}_{symbolE}", status_code=404)
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            data = f.readlines()[:lines]
        return PlainTextResponse("".join(data))
    except Exception as e:
        return PlainTextResponse(f"Error reading log: {e}", status_code=500)






def _read_csv_json(filename):
    if not os.path.exists(filename):
        return JSONResponse({"error": f"{filename} not found"}, status_code=404)
    with open(filename, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# =====================================================
# ================ BACKGROUND JOB =====================
# =====================================================

async def background_sync():
    EXT = ExtendedAPI()
    LIG = LighterAPI()
    await asyncio.gather(EXT.init(), LIG.init())
    while True:
        try:
            await EXT.getTrades()
            EXT.mergeTrades()
            EXT.calculateDailyPnL()

            await LIG.getTrades()
            LIG.mergeTrades()
            LIG.calculateDailyPnL()
            logger.info("✅ Background data sync complete.")
        except Exception as e:
            logger.error(f"⚠️ Sync error: {e}")
        await asyncio.sleep(10)

@app.on_event("startup")
async def start_background_task():
    asyncio.create_task(background_sync())

# =====================================================
# ================ RUN ================================
# =====================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
