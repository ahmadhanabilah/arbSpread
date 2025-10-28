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

def start_screen(symbol: str):
    if not SYMBOL_RE.match(symbol):
        raise ValueError("Invalid symbol name. Allowed: A-Z a-z 0-9 _ - (1-20 chars).")

    session_name = f"arb_{symbol}"
    running = list_running_screens()
    if session_name in running:
        return False

    cmd = ["screen", "-dmS", session_name, "python3", "-u", "main.py", symbol]
    subprocess.run(cmd, check=False)
    return True

def stop_screen(symbol: str):
    if not SYMBOL_RE.match(symbol):
        raise ValueError("Invalid symbol name. Allowed: A-Z a-z 0-9 _ - (1-20 chars).")

    try:
        res = subprocess.run(["screen", "-ls"], capture_output=True, text=True, check=False)
        out = res.stdout or ""
        for line in out.splitlines():
            if f"arb_{symbol}" in line and "." in line:
                tokens = line.strip().split()
                for token in tokens:
                    if f"arb_{symbol}" in token and "." in token:
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
    syms = [s["symbol"] for s in cfg.get("symbols", [])]
    return {"symbols": syms, "running": list_running_screens()}

@app.post("/api/start/{symbol}", dependencies=[Depends(require_auth)])
async def start_bot(symbol: str):
    start_screen(symbol)
    return {"ok": True}

@app.post("/api/stop/{symbol}", dependencies=[Depends(require_auth)])
async def stop_bot(symbol: str):
    return {"ok": stop_screen(symbol)}

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

@app.get("/get_ext")
async def get_ext():
    return _read_csv_json("trades_ext.csv")

@app.get("/get_lig")
async def get_lig():
    return _read_csv_json("trades_merged_lig.csv")

@app.get("/get_pnl_ext")
async def get_pnl_ext():
    return _read_csv_json("trades_daily_pnl_ext.csv")

@app.get("/get_pnl_lig")
async def get_pnl_lig():
    return _read_csv_json("trades_daily_pnl_lig.csv")

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
            await EXT.init_getTrades()
            EXT.calculateDailyPnL()
            await LIG.init_getTrades()
            await LIG.getTrades()
            LIG.mergeTrades()
            LIG.calculateDailyPnL()
            logger.info("✅ Background data sync complete.")
        except Exception as e:
            logger.error(f"⚠️ Sync error: {e}")
        await asyncio.sleep(60)

@app.on_event("startup")
async def start_background_task():
    asyncio.create_task(background_sync())

# =====================================================
# ================ RUN ================================
# =====================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
