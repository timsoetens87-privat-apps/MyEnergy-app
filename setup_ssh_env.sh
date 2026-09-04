#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found on this host. Install Python 3 first." >&2
  exit 1
fi

# Keep Linux/NAS env separate from Windows .venv
if [ ! -d "venv" ] || [ ! -f "venv/bin/activate" ]; then
  echo "Creating Linux virtual environment in ./venv"
  python3 -m venv venv
fi

source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "SSH environment ready. Activate with: source venv/bin/activate"
