#!/usr/bin/env pwsh
# Stop on errors and enforce strict mode
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# -----------------------------------------------------------------------------
# Warn and exit gracefully if APP_CONFIG_ENDPOINT is not set or empty
# -----------------------------------------------------------------------------
if (-not $env:APP_CONFIG_ENDPOINT -or [string]::IsNullOrWhiteSpace($env:APP_CONFIG_ENDPOINT)) {
    Write-Host "⚠️ Warning: APP_CONFIG_ENDPOINT is not set or is empty. Skipping post-deployment steps."
    exit 0
}

Write-Host "🔧 Running post-deployment steps…`n"

# -----------------------------------------------------------------------------
# Find the Python executable
# -----------------------------------------------------------------------------
$python = $null

# Try python3 (exclude stubs in WindowsApps)
$cmd = Get-Command python3 -ErrorAction SilentlyContinue |
       Where-Object { -not ($_.Source -like '*WindowsApps*') }
if ($cmd) { $python = $cmd.Name }

# Fallback to python
if (-not $python) {
    $cmd = Get-Command python -ErrorAction SilentlyContinue |
           Where-Object { -not ($_.Source -like '*WindowsApps*') }
    if ($cmd) { $python = $cmd.Name }
}

# Fallback to Windows py launcher
if (-not $python) {
    $cmd = Get-Command py -ErrorAction SilentlyContinue
    if ($cmd) { $python = $cmd.Name }
}

if (-not $python) {
    Throw "Python executable not found. Install Python or ensure it's on PATH."
}

Write-Host "`n🐍 Using Python: $python"

# -----------------------------------------------------------------------------
# 0) Setup Python environment
# -----------------------------------------------------------------------------
Write-Host "`n📦 Creating temporary venv…"
& $python -m venv config/.venv_temp
& config/.venv_temp/Scripts/Activate.ps1  

Write-Host "⬇️ Installing requirements…"
& $python -m pip install --upgrade pip
& $python -m pip install -r config/requirements.txt

# -----------------------------------------------------------------------------
# 4) AI Search Setup
# -----------------------------------------------------------------------------
Write-Host ""
if (-not ($env:SEARCH_SETUP -and $env:SEARCH_SETUP.ToLower() -eq 'false')) {
    Write-Host "🔍 AI Search setup…"
    try {
        Write-Host "🚀 Running config.search.setup…"
        & $python -m config.search.setup
        Write-Host "✅ Search setup script finished."
    } catch {
        Write-Warning "❗️ Error during Search setup. Skipping it."
    }
} else {
    Write-Warning "⚠️ Skipping AI Search setup (SEARCH_SETUP is 'false')."
}

# -----------------------------------------------------------------------------
# Cleaning up
# -----------------------------------------------------------------------------
Write-Host "🧹 Cleaning Python environment up…"
# 'deactivate' is defined by the Activate.ps1 script
if (Get-Command deactivate -ErrorAction SilentlyContinue) {
    deactivate
}
Remove-Item -Recurse -Force config/.venv_temp
Write-Host "🧼 Temporary files removed. All done!"
Write-Host "`n🎉 Post-deployment script completed successfully!`n"