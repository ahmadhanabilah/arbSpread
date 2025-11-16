#!/bin/bash
# === ArbSpread Auto Updater & Restarter ===
# Usage: bash update.sh

REPO_DIR="/root/arbSpread"
BACKEND_SCREEN="web-backend"
BACKEND_DATA_SCREEN="web-backend-data"
FRONTEND_SCREEN="web-frontend"
BACKEND_DIR="$REPO_DIR/backend"
FRONTEND_DIR="$REPO_DIR/frontend"
VENV_PATH="/root/arbSpread/.venv/bin/activate"

echo ""
echo "🔄 [1/6] Pulling latest ArbSpread repo..."
cd "$REPO_DIR" || exit
git fetch origin main
git reset --hard origin/main
echo "✅ Repository synced with main branch."

# -----------------------------
# STEP 2 — Update SDKs
# -----------------------------
echo ""
echo "📦 [2/6] Updating Lighter & Extended SDKs..."
source "$VENV_PATH"

# Update the Extended SDK from PyPI
echo "⏫ Updating Extended SDK (x10-python-trading-starknet)..."
pip install --upgrade x10-python-trading-starknet >/dev/null 2>&1 && echo "✅ Extended SDK updated."

# Update the Lighter SDK from GitHub
echo "⏫ Updating Lighter SDK from GitHub..."
pip install --upgrade git+https://github.com/elliottech/lighter-python.git >/dev/null 2>&1 && echo "✅ Lighter SDK updated."

# -----------------------------
# STEP 3 — Restart backend & frontend
# -----------------------------
echo ""
echo "🧨 Killing all running screen sessions..."
screen -ls | grep -Eo '[0-9]+\.[^[:space:]]+' | while read -r session; do
    echo "   🔪 Killing $session"
    screen -S "$session" -X quit
done
sleep 1
echo "✅ All screens terminated."

# -----------------------------
# STEP 4 — Start unified_backend.py
# -----------------------------
cd "$BACKEND_DIR"
echo "▶️ Starting backend (unified) in screen: $BACKEND_SCREEN"
screen -dmS "$BACKEND_SCREEN" bash -c "
source $VENV_PATH;
python3 unified_backend.py;
"

# -----------------------------
# STEP 5 — Start data_backend.py
# -----------------------------
echo "▶️ Starting data backend in screen: $BACKEND_DATA_SCREEN"
screen -dmS "$BACKEND_DATA_SCREEN" bash -c "
source $VENV_PATH;
python3 data_backend.py;
"

# STEP 6 — Start frontend
# -----------------------------
cd "$FRONTEND_DIR"
echo "▶️ Starting frontend in screen: $FRONTEND_SCREEN"
screen -dmS "$FRONTEND_SCREEN" bash -c "
source ~/.nvm/nvm.sh;
npm run dev -- --host --port 3000;
"


sleep 2

echo ""
echo "✅ [6/6] Update complete!"
echo "   - ArbSpread repo synced"
echo "   - Lighter SDK (GitHub) refreshed"
echo "   - Extended SDK (PyPI) upgraded"
echo "   - Backends & frontend restarted"
echo "   - 🧩 Running screens:"
echo "       * $BACKEND_SCREEN → unified_backend.py"
echo "       * $BACKEND_DATA_SCREEN → data_backend.py"
echo "       * $FRONTEND_SCREEN → frontend (port 3000)"
echo ""
