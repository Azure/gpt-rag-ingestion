#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Fail early if APP_CONFIG_ENDPOINT is not set or empty
# -----------------------------------------------------------------------------
if [[ -z "${APP_CONFIG_ENDPOINT:-}" ]]; then
   echo "⚠️ Warning: APP_CONFIG_ENDPOINT is not set or is empty. Skipping post-deploy steps."
  exit 0
  exit 0
fi

echo "🔧 Running post-deploy steps…"

###############################################################################
# Setup Python environment
###############################################################################
echo "📦 Creating temporary venv…"
python3 -m venv --without-pip config/.venv_temp
source config/.venv_temp/bin/activate
echo "⬇️ Manually bootstrapping pip…"
curl -sS https://bootstrap.pypa.io/get-pip.py | python

echo "⬇️  Installing requirements…"
pip install --upgrade pip
pip install -r config/requirements.txt


###############################################################################
# AI Search Setup
###############################################################################
echo
if [[ "${SEARCH_SETUP:-}" != "false" ]]; then
  echo "🔍 AI Search setup…"
  {
    echo "🚀 Running config.search.setup…"
    python -m config.search.setup
    echo "✅ Search setup script finished."
  } || {
    echo "❗️ Error during Search setup. Skipping it."
  }
else
  echo "⚠️  Skipping AI Search setup (SEARCH_SETUP is 'false')."
fi

###############################################################################
# Cleaning up
###############################################################################
echo 
echo "🧹 Cleaning Python environment up…"
deactivate
rm -rf config/.venv_temp