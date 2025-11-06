#!/bin/bash
# === ArbSpread Auto Updater & Restarter ===
# Usage: bash update.sh

REPO_DIR="/root/arbSpread"
BACKEND_SCREEN="web-backend"
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
# STEP 4 — Restart backend & frontend
# -----------------------------
echo "🧨 Killing all running screen sessions..."
screen -ls | awk '/Detached|Attached/ {print $1}' | xargs -r -n 1 screen -S {} -X quit
sleep 1
echo "✅ All screens terminated."

echo ""
echo "🧹 [5/6] Restarting backend and frontend screens..."

screen -S "$BACKEND_SCREEN" -X quit 2>/dev/null
screen -S "$FRONTEND_SCREEN" -X quit 2>/dev/null
sleep 1

# Start backend
cd "$BACKEND_DIR"
echo "▶️ Starting backend in screen: $BACKEND_SCREEN"
screen -dmS "$BACKEND_SCREEN" bash -c "
source $VENV_PATH;
python3 unified_backend.py;
"

# Start frontend
cd "$FRONTEND_DIR"
echo "▶️ Starting frontend in screen: $FRONTEND_SCREEN"
screen -dmS "$FRONTEND_SCREEN" bash -c '
npm run dev -- --host;
'

sleep 2

echo ""
echo "✅ [6/6] Update complete!"
echo "   - ArbSpread repo synced"
echo "   - Lighter SDK (GitHub) refreshed"
echo "   - Extended SDK (PyPI) upgraded"
echo "   - Backend & frontend restarted"
echo ""
