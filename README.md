# 🚀 Project Setup Guide

Follow these steps to set up and run the project on a new VPS.

---

## 1. Install Git Python, Screen, Node, NPM

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv screen git
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/master/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
nvm install --lts
nvm install node

# 4️⃣ Check versions
node -v
npm -v
python3 --version
pip3 --version
screen --version
git --version

```

---

## 2. Clone repo & Create Virtual Environment

```bash
git clone https://github.com/ahmadhanabilah/arbSpread.git
cd arbSpread
python3 -m venv .venv
source .venv/bin/activate
```

---

## 3. Install Requirements

```bash
pip install -r requirements.txt
```

## 4. Frontend

```bash
screen -S web-frontend
```

```bash
npm create vite@latest frontend -- --template react
cd frontend
npm install
npm install react-icons
```

move and replace /src to frontend

```bash
npm run dev -- --host --port 3000
```



## 5. Backend

```bash
screen -S web-backend
```

```bash
cd backend
```

rename .env_example to .env and fill credentials

```bash
source .venv/bin/activate
python3 unified_backend.py
```

---

✅ **Setup complete!**  
Your environment is now ready to run the project.

## Github Repo Update
```bash
git add .
git commit -m "Update"
git push origin main
```

