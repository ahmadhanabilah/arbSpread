#!/bin/bash
set -e

echo "🚀 ArbSpread Initial Setup Started"

# === 1) Basic dependencies ===
echo "📌 Updating apt..."
sudo apt update

echo "📌 Installing Python, Screen, Git..."
sudo apt install -y python3 python3-pip python3-venv screen git curl build-essential

# === 2) Install NVM + Node.js ===
if [ ! -d "$HOME/.nvm" ]; then
  echo "📌 Installing NVM..."
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/master/install.sh | bash
fi

export NVM_DIR="$HOME/.nvm"
# shellcheck disable=SC1091
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

echo "📌 Installing Node LTS & latest..."
nvm install --lts
nvm install node

# === 3) Version check ===
echo "🧾 Version check:"
node -v
npm -v
python3 --version
pip3 --version
screen --version
git --version

# === 4) Clone repo ===
if [ ! -d "/root/arbSpread" ]; then
  echo "📌 Cloning ArbSpread..."
  git clone https://github.com/ahmadhanabilah/arbSpread.git /root/arbSpread
else
  echo "⚠️ Repo already exists — skipping clone"
fi

cd /root/arbSpread

# === 5) Python venv ===
if [ ! -d ".venv" ]; then
  echo "📌 Creating virtual environment..."
  python3 -m venv .venv
fi

echo "📌 Activating venv..."
source .venv/bin/activate

echo "📌 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# === 6) Frontend ===
if [ ! -d "frontend" ]; then
  echo "📌 Creating Vite React frontend..."
  npm create vite@latest frontend -- --template react
fi

cd frontend
echo "📌 Installing frontend dependencies..."
npm install
npm install react-icons marked

cd ..

# === 7) Copy backend/.env_example → backend/.env (only if .env doesn't exist) ===
BACKEND_ENV_DIR="/root/arbSpread/backend"
if [ ! -f "$BACKEND_ENV_DIR/.env" ]; then
  echo "📌 Creating backend .env from .env_example..."
  if [ -f "$BACKEND_ENV_DIR/.env_example" ]; then
    cp "$BACKEND_ENV_DIR/.env_example" "$BACKEND_ENV_DIR/.env"
    echo "➡️ backend/.env created"
  else
    echo "❌ backend/.env_example NOT FOUND — please add manually"
  fi
else
  echo "⚠️ backend/.env already exists — skip copy"
fi


# === 8) Run updater / launcher ===
if [ -f "run.sh" ]; then
  echo "🚀 Starting ArbSpread using run.sh..."
  bash run.sh
else
  echo "❌ run.sh not found — please rename update.sh to run.sh"
fi

echo ""
echo "🎉 INIT FINISHED — ArbSpread is running now!"
echo "-------------------------------------------------------------"
echo "Edit .env anytime via website / file"
echo "To restart later => bash run.sh"
echo "-------------------------------------------------------------"
