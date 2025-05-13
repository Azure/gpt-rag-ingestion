# predeploy-dataingest.ps1 — validate env, optionally load App Config, then build & push

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host ""
Write-Host "🔍 Fetching all 'azd' environment values…"
$envValues = azd env get-values

# Temporarily allow missing matches
$oldPref = $ErrorActionPreference
$ErrorActionPreference = 'SilentlyContinue'
$azcrName     = ($envValues | Select-String '^AZURE_CONTAINER_REGISTRY_NAME=').Line -replace '.*=', '' -replace '"',''
$azcrEndpoint = ($envValues | Select-String '^AZURE_CONTAINER_REGISTRY_ENDPOINT=').Line -replace '.*=', '' -replace '"',''
$rg           = ($envValues | Select-String '^AZURE_RESOURCE_GROUP=').Line -replace '.*=', '' -replace '"',''
$appConfigEP  = ($envValues | Select-String '^AZURE_APP_CONFIG_ENDPOINT=').Line -replace '.*=', '' -replace '"',''
$ErrorActionPreference = $oldPref

# Check for missing
$missing = @()
if (-not $azcrName)     { $missing += 'AZURE_CONTAINER_REGISTRY_NAME' }
if (-not $azcrEndpoint) { $missing += 'AZURE_CONTAINER_REGISTRY_ENDPOINT' }
if (-not $rg)           { $missing += 'AZURE_RESOURCE_GROUP' }
if (-not $appConfigEP)  { $missing += 'AZURE_APP_CONFIG_ENDPOINT' }

if ($missing.Count -gt 0) {
    Write-Host "`n⚠️  Missing required environment variables:" -ForegroundColor Yellow
    $missing | ForEach-Object { Write-Host "    • $_" }
    Write-Host "`nPlease set them before running this script, e.g.:"
    Write-Host "  azd env set <NAME> <VALUE>"
    exit 1
}

Write-Host "`n✅ All required azd env values are set.`n" -ForegroundColor Green

Write-Host "🔐 Logging into ACR ($azcrName)…" -ForegroundColor Green
az acr login --name $azcrName

Write-Host "🛢️  Defining TAG…" -ForegroundColor Blue
$tag = $env:TAG
if (-not $tag) {
    $tag = git rev-parse --short HEAD
}
azd env set TAG $tag
Write-Host "✅ TAG set to: $tag" -ForegroundColor Green

Write-Host "`n🛠️  Building Docker image…" -ForegroundColor Green
docker build `
  -t "$azcrEndpoint/azure-gpt-rag/dataingest-build:$tag" `
  .

Write-Host "`n📤 Pushing image…" -ForegroundColor Green
docker push "$azcrEndpoint/azure-gpt-rag/dataingest-build:$tag"

Write-Host "`n🧩 Ensuring runtime settings are complete…" -ForegroundColor Green
Write-Host "📦 Creating temporary virtual environment…" -ForegroundColor Blue
$venvPath = 'scripts/appconfig/.venv_temp'
python -m venv $venvPath

Write-Host "→ Activating venv…" -ForegroundColor Blue
& "$venvPath/Scripts/Activate.ps1"

Write-Host "⬇️  Installing requirements…" -ForegroundColor Blue
pip install --upgrade pip
pip install -r scripts/appconfig/requirements.txt
