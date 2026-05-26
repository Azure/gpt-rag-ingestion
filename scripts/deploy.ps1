<#
.SYNOPSIS
    Deploys the GPT-RAG ingestion Container App image.
#>

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
[Console]::InputEncoding = $utf8NoBom
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
$ProgressPreference = 'SilentlyContinue'

$label = 'gpt-rag'
$imageRepository = 'data-ingestion'
$appConfigKey = 'DATA_INGEST_APP_NAME'
$identitySuffix = 'dataingest'

function Write-Green($msg) { Write-Host $msg -ForegroundColor Green }
function Write-Blue($msg) { Write-Host $msg -ForegroundColor Cyan }
function Write-Yellow($msg) { Write-Host $msg -ForegroundColor Yellow }
function Write-ErrorColored($msg) { Write-Host $msg -ForegroundColor Red }

function Invoke-ExternalCommand {
    param(
        [Parameter(Mandatory=$true)][string]$FilePath,
        [Parameter()][string[]]$Arguments = @(),
        [Parameter(Mandatory=$true)][string]$What
    )

    $output = & $FilePath @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        Write-ErrorColored ("Failed: {0} (exit {1})" -f $What, $exitCode)
        if ($output) { Write-Host ($output | Out-String) }
        exit 1
    }
    return $output
}

function Get-CliOutputValue {
    param(
        [Parameter(Mandatory=$true)][AllowEmptyCollection()][object[]]$Output,
        [Parameter()][string]$ExpectedValue,
        [Parameter()][string]$Pattern
    )

    $lines = New-Object System.Collections.Generic.List[string]
    foreach ($item in @($Output)) {
        if ($null -eq $item) { continue }
        foreach ($line in ($item.ToString() -split "`r?`n")) {
            $trimmed = $line.Trim()
            if (-not [string]::IsNullOrWhiteSpace($trimmed)) {
                $lines.Add($trimmed)
            }
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($ExpectedValue)) {
        for ($i = $lines.Count - 1; $i -ge 0; $i--) {
            if ($lines[$i] -eq $ExpectedValue) { return $lines[$i] }
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($Pattern)) {
        for ($i = $lines.Count - 1; $i -ge 0; $i--) {
            if ($lines[$i] -match $Pattern) { return $lines[$i] }
        }
    }

    for ($i = $lines.Count - 1; $i -ge 0; $i--) {
        $line = $lines[$i]
        if ($line -match '^WARNING:' -or
            $line -match '^D:\\a\\_work\\' -or
            $line -match 'site-packages.*UserWarning:' -or
            $line -match '^from cryptography' -or
            $line -match '^Running\.') {
            continue
        }
        return $line
    }

    return ''
}

function Get-AzdEnvValue {
    param([Parameter(Mandatory=$true)][string]$Key)

    $envValues = & azd env get-values 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $envValues) { return $null }

    foreach ($line in $envValues -split "`n") {
        if ($line -match "^\s*$([regex]::Escape($Key))\s*=\s*`"?([^`"]+)`"?\s*$") {
            return $Matches[1].Trim()
        }
    }
    return $null
}

function Get-ConfigCandidates {
    param([Parameter(Mandatory=$true)][string]$Key)

    $candidates = New-Object System.Collections.Generic.List[string]
    foreach ($candidate in @($Key, $Key.ToUpperInvariant(), $Key.ToLowerInvariant())) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and -not $candidates.Contains($candidate)) {
            $candidates.Add($candidate)
        }
    }
    return $candidates
}

function Get-ConfigValue {
    param([Parameter(Mandatory=$true)][string]$Key)

    $lastOutput = $null
    foreach ($candidate in (Get-ConfigCandidates -Key $Key)) {
        Write-Blue ("Retrieving '{0}' from App Configuration..." -f $candidate)
        $output = & az appconfig kv show `
            --endpoint $APP_CONFIG_ENDPOINT `
            --key $candidate `
            --label $label `
            --auth-mode login `
            --only-show-errors `
            --query value -o tsv 2>&1
        $exitCode = $LASTEXITCODE
        $value = Get-CliOutputValue -Output $output
        if ($exitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($value)) {
            return $value
        }
        $lastOutput = $value
    }

    Write-Yellow ("Failed to retrieve key '{0}'. Last CLI output: {1}" -f $Key, $lastOutput)
    return $null
}

function Get-RequiredConfigValue {
    param([Parameter(Mandatory=$true)][string]$Key)

    $value = Get-ConfigValue -Key $Key
    if ([string]::IsNullOrWhiteSpace($value)) {
        Write-ErrorColored ("Missing required App Configuration key: {0}" -f $Key)
        exit 1
    }
    return $value
}

function Test-DockerReady {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { return $false }
    $output = & docker info 2>&1
    return ($LASTEXITCODE -eq 0 -and ($output | Out-String) -notmatch 'Docker Desktop is manually paused')
}

function Get-BuildMode {
    $mode = if ($env:BUILD_MODE) { $env:BUILD_MODE.Trim().ToLowerInvariant() } else { '' }

    if (-not $mode -and $env:USE_DOCKER) {
        switch ($env:USE_DOCKER.Trim().ToLowerInvariant()) {
            { $_ -in @('true','1','yes') } { $mode = 'local'; break }
            { $_ -in @('false','0','no') } { $mode = 'acr-task'; break }
        }
    }

    if (-not $mode) {
        $networkIsolation = ($env:NETWORK_ISOLATION -and $env:NETWORK_ISOLATION.Trim().ToLowerInvariant() -eq 'true')
        if ($networkIsolation -or $env:ACR_TASK_AGENT_POOL) {
            $mode = 'acr-task'
        } elseif (Test-DockerReady) {
            $mode = 'local'
        } else {
            $mode = 'acr-task'
        }
    }

    if ($mode -notin @('local','acr-task')) {
        Write-ErrorColored "Unsupported BUILD_MODE '$mode'. Use 'local' or 'acr-task'."
        exit 1
    }
    if ($mode -eq 'local' -and -not (Test-DockerReady)) {
        Write-ErrorColored 'BUILD_MODE=local requested, but Docker is not available.'
        exit 1
    }
    return $mode
}

function Get-ImageTag {
    if ($env:tag) { return $env:tag.Trim() }

    $gitTag = & git rev-parse --short HEAD 2>$null
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($gitTag)) {
        return $gitTag.Trim()
    }

    return "GPT$(Get-Random -Minimum 100000 -Maximum 999999)"
}

function Set-ContainerAppRegistry {
    param(
        [Parameter(Mandatory=$true)][string]$AppName,
        [Parameter(Mandatory=$true)][hashtable]$Values
    )

    $identityOutput = Invoke-ExternalCommand -FilePath 'az' -Arguments @(
        'containerapp','identity','show',
        '--name',$AppName,
        '--resource-group',$Values.AZURE_RESOURCE_GROUP,
        '--only-show-errors',
        '--query','type','-o','tsv'
    ) -What 'fetch container app identity'
    $identityType = Get-CliOutputValue -Output $identityOutput -Pattern 'UserAssigned|SystemAssigned|None'

    if ($identityType -like '*UserAssigned*') {
        if ([string]::IsNullOrWhiteSpace($Values.SUBSCRIPTION_ID) -or [string]::IsNullOrWhiteSpace($Values.RESOURCE_TOKEN)) {
            Write-ErrorColored 'SUBSCRIPTION_ID and RESOURCE_TOKEN are required for user-assigned registry identity.'
            exit 1
        }
        $registryIdentity = "/subscriptions/$($Values.SUBSCRIPTION_ID)/resourceGroups/$($Values.AZURE_RESOURCE_GROUP)/providers/Microsoft.ManagedIdentity/userAssignedIdentities/uai-ca-$($Values.RESOURCE_TOKEN)-$identitySuffix"
    } else {
        $registryIdentity = 'system'
    }

    Invoke-ExternalCommand -FilePath 'az' -Arguments @(
        'containerapp','registry','set',
        '--name',$AppName,
        '--resource-group',$Values.AZURE_RESOURCE_GROUP,
        '--server',$Values.CONTAINER_REGISTRY_LOGIN_SERVER,
        '--identity',$registryIdentity,
        '--only-show-errors'
    ) -What 'set container app registry' | Out-Null
}

function Confirm-ContainerAppImage {
    param(
        [Parameter(Mandatory=$true)][string]$AppName,
        [Parameter(Mandatory=$true)][string]$ResourceGroupName,
        [Parameter(Mandatory=$true)][string]$ExpectedImage
    )

    $imageOutput = Invoke-ExternalCommand -FilePath 'az' -Arguments @(
        'containerapp','show',
        '--name',$AppName,
        '--resource-group',$ResourceGroupName,
        '--only-show-errors',
        '--query','properties.template.containers[0].image','-o','tsv'
    ) -What 'fetch container app image'
    $actualImage = Get-CliOutputValue -Output $imageOutput -ExpectedValue $ExpectedImage

    if ([string]::IsNullOrWhiteSpace($actualImage) -or $actualImage -eq 'null') {
        Write-ErrorColored "Could not determine configured image for '$AppName'."
        exit 1
    }

    if ($actualImage -ne $ExpectedImage) {
        Write-ErrorColored "Container app '$AppName' is configured with '$actualImage' instead of '$ExpectedImage'."
        exit 1
    }
}

Write-Host ''
if ($env:APP_CONFIG_ENDPOINT -and $env:APP_CONFIG_ENDPOINT.Trim() -ne '') {
    $APP_CONFIG_ENDPOINT = $env:APP_CONFIG_ENDPOINT.Trim()
    Write-Green "Using APP_CONFIG_ENDPOINT from environment: $APP_CONFIG_ENDPOINT"
} else {
    Write-Blue 'Fetching APP_CONFIG_ENDPOINT from azd env...'
    $APP_CONFIG_ENDPOINT = Get-AzdEnvValue -Key 'APP_CONFIG_ENDPOINT'
}

if ([string]::IsNullOrWhiteSpace($APP_CONFIG_ENDPOINT)) {
    Write-ErrorColored 'Missing APP_CONFIG_ENDPOINT.'
    Write-Host 'Set it with: azd env set APP_CONFIG_ENDPOINT <your-endpoint>'
    Write-Host "Or set `$env:APP_CONFIG_ENDPOINT before running this script."
    exit 1
}
Write-Green "APP_CONFIG_ENDPOINT: $APP_CONFIG_ENDPOINT"

Write-Blue 'Checking Azure CLI login and subscription...'
$null = & az account show 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-ErrorColored "Not logged in. Please run 'az login'."
    exit 1
}
Write-Green 'Azure CLI is logged in.'

$buildMode = Get-BuildMode
Write-Green "Build mode: $buildMode"

Write-Blue "Loading App Configuration settings (label=$label)..."
$values = @{
    CONTAINER_REGISTRY_NAME = (Get-RequiredConfigValue -Key 'CONTAINER_REGISTRY_NAME')
    CONTAINER_REGISTRY_LOGIN_SERVER = (Get-RequiredConfigValue -Key 'CONTAINER_REGISTRY_LOGIN_SERVER')
    AZURE_RESOURCE_GROUP = (Get-RequiredConfigValue -Key 'AZURE_RESOURCE_GROUP')
    APP_NAME = (Get-RequiredConfigValue -Key $appConfigKey)
    SUBSCRIPTION_ID = (Get-ConfigValue -Key 'SUBSCRIPTION_ID')
    RESOURCE_TOKEN = (Get-ConfigValue -Key 'RESOURCE_TOKEN')
}
if ([string]::IsNullOrWhiteSpace($values.SUBSCRIPTION_ID)) {
    $subscriptionOutput = Invoke-ExternalCommand -FilePath 'az' -Arguments @('account','show','--only-show-errors','--query','id','-o','tsv') -What 'fetch subscription id'
    $values.SUBSCRIPTION_ID = Get-CliOutputValue -Output $subscriptionOutput -Pattern '^[0-9a-fA-F-]{36}$'
}

Write-Green 'All App Configuration values retrieved:'
Write-Host ("   CONTAINER_REGISTRY_NAME = {0}" -f $values.CONTAINER_REGISTRY_NAME)
Write-Host ("   CONTAINER_REGISTRY_LOGIN_SERVER = {0}" -f $values.CONTAINER_REGISTRY_LOGIN_SERVER)
Write-Host ("   AZURE_RESOURCE_GROUP = {0}" -f $values.AZURE_RESOURCE_GROUP)
Write-Host ("   APP_NAME = {0}" -f $values.APP_NAME)

if ($buildMode -eq 'local') {
    Write-Blue ("Logging into ACR ({0} in {1})..." -f $values.CONTAINER_REGISTRY_NAME, $values.AZURE_RESOURCE_GROUP)
    Invoke-ExternalCommand -FilePath 'az' -Arguments @('acr','login','--name',$values.CONTAINER_REGISTRY_NAME,'--resource-group',$values.AZURE_RESOURCE_GROUP) -What 'ACR login' | Out-Null
    Write-Green 'Logged into ACR.'
} else {
    Write-Green 'Using remote ACR build; local Docker login is not required.'
}

$tag = Get-ImageTag
Write-Green "Using image tag: $tag"
$fullImageName = "$($values.CONTAINER_REGISTRY_LOGIN_SERVER)/azure-gpt-rag/${imageRepository}:$tag"

if ($buildMode -eq 'local') {
    Write-Blue 'Building Docker image...'
    Invoke-ExternalCommand -FilePath 'docker' -Arguments @('build','--platform','linux/amd64','-t',$fullImageName,'.') -What 'Docker build' | Out-Null
    Write-Blue 'Pushing image...'
    Invoke-ExternalCommand -FilePath 'docker' -Arguments @('push',$fullImageName) -What 'Docker push' | Out-Null
    Write-Green 'Image pushed.'
} else {
    Write-Blue 'Building image remotely via az acr build...'
    $acrBuildArgs = @('acr','build','--registry',$values.CONTAINER_REGISTRY_NAME,'--image',"azure-gpt-rag/${imageRepository}:$tag",'--file','Dockerfile','--no-logs')
    if ($env:ACR_TASK_AGENT_POOL) {
        Invoke-ExternalCommand -FilePath 'az' -Arguments @('acr','agentpool','show','--registry',$values.CONTAINER_REGISTRY_NAME,'--name',$env:ACR_TASK_AGENT_POOL,'--resource-group',$values.AZURE_RESOURCE_GROUP,'--only-show-errors') -What 'validate ACR task agent pool' | Out-Null
        $acrBuildArgs += @('--agent-pool',$env:ACR_TASK_AGENT_POOL)
    }
    $acrBuildArgs += '.'
    Invoke-ExternalCommand -FilePath 'az' -Arguments $acrBuildArgs -What 'ACR remote build' | Out-Null
    Write-Green 'Remote build completed.'
}

Write-Blue 'Updating container app registry...'
Set-ContainerAppRegistry -AppName $values.APP_NAME -Values $values
Write-Green 'Container app registry updated.'

Write-Blue 'Updating container app image...'
$latestRevisionOutput = Invoke-ExternalCommand -FilePath 'az' -Arguments @(
    'containerapp','update',
    '--name',$values.APP_NAME,
    '--resource-group',$values.AZURE_RESOURCE_GROUP,
    '--image',$fullImageName,
    '--only-show-errors',
    '--query','properties.latestRevisionName','-o','tsv'
) -What 'update container app image'
$latestRevision = Get-CliOutputValue -Output $latestRevisionOutput -Pattern '^[A-Za-z0-9][A-Za-z0-9-]*--[A-Za-z0-9-]+$'
Write-Green 'Container app updated.'
if (-not [string]::IsNullOrWhiteSpace($latestRevision) -and $latestRevision -ne 'null') {
    Write-Green "Latest revision: $latestRevision"
}

Write-Blue 'Ensuring container app ingress target port is 8080...'
Invoke-ExternalCommand -FilePath 'az' -Arguments @(
    'containerapp','ingress','update',
    '--name',$values.APP_NAME,
    '--resource-group',$values.AZURE_RESOURCE_GROUP,
    '--target-port','8080',
    '--only-show-errors'
) -What 'update container app ingress' | Out-Null
Write-Green 'Ingress target port updated.'

Write-Blue 'Verifying container app image...'
Confirm-ContainerAppImage -AppName $values.APP_NAME -ResourceGroupName $values.AZURE_RESOURCE_GROUP -ExpectedImage $fullImageName
Write-Green 'Container app image verified.'
