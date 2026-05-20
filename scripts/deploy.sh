#!/usr/bin/env bash
# -------------------------------------------------------------------------
# deploy.sh — validate APP_CONFIG_ENDPOINT, load App Config (label=gpt-rag), then build & push
# -------------------------------------------------------------------------

set -euo pipefail

# Toggle DEBUG for verbose output
DEBUG=${DEBUG:-false}
if [[ "$DEBUG" == "true" ]]; then
  set -x
fi

# colors
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # no color

echo
# First, check shell environment
if [[ -n "${APP_CONFIG_ENDPOINT:-}" ]]; then
  echo -e "${GREEN}✅ Using APP_CONFIG_ENDPOINT from environment: ${APP_CONFIG_ENDPOINT}${NC}"
else
  echo -e "${BLUE}🔍 Fetching APP_CONFIG_ENDPOINT from azd env…${NC}"
  envValues="$(azd env get-values 2>/dev/null || true)"
  APP_CONFIG_ENDPOINT="$(echo "$envValues" \
    | grep -i '^APP_CONFIG_ENDPOINT=' \
    | cut -d '=' -f2- \
    | tr -d '"' \
    | tr -d '[:space:]' || true)"
fi

if [[ -z "${APP_CONFIG_ENDPOINT:-}" ]]; then
  echo -e "${YELLOW}⚠️  Missing APP_CONFIG_ENDPOINT.${NC}"
  echo -e "  • ${BLUE}Set it with:${NC} azd env set APP_CONFIG_ENDPOINT <your-endpoint>"
  echo -e "  • ${BLUE}Or export in shell:${NC} export APP_CONFIG_ENDPOINT=<your-endpoint> before running this script."
  exit 1
fi

echo -e "${GREEN}✅ APP_CONFIG_ENDPOINT: ${APP_CONFIG_ENDPOINT}${NC}"
echo

# derive App Configuration name from endpoint
configName="${APP_CONFIG_ENDPOINT#https://}"
configName="${configName%.azconfig.io}"
if [[ -z "$configName" ]]; then
  echo -e "${YELLOW}⚠️  Could not parse config name from endpoint '${APP_CONFIG_ENDPOINT}'.${NC}"
  exit 1
fi
echo -e "${GREEN}✅ App Configuration name: ${configName}${NC}"
echo

echo -e "${BLUE}🔐 Checking Azure CLI login and subscription…${NC}"
if ! az account show >/dev/null 2>&1; then
  echo -e "${YELLOW}⚠️  Not logged in. Please run 'az login'.${NC}"
  exit 1
fi
echo -e "${GREEN}✅ Azure CLI is logged in.${NC}"
echo

# label for your configuration keys
label="gpt-rag"

echo -e "${GREEN}⚙️ Loading App Configuration settings (label=${label})…${NC}"
echo

# helper to fetch a key (with label) from App Configuration via az CLI
get_config_value() {
  key="$1"
  echo -e "${BLUE}🛠️  Retrieving '$key' (label=${label}) from App Configuration…${NC}" >&2
  val="$(az appconfig kv show \
    --name "$configName" \
    --key "$key" \
    --label "$label" \
    --auth-mode login \
    --query value -o tsv 2>&1)" || status=$?
  if [[ -z "${val// /}" ]]; then
    echo -e "${YELLOW}⚠️  Failed to retrieve key '$key'. CLI output: $val${NC}" >&2
    return 1
  fi
  echo "$val"
}

# fetch required settings
containerRegistryName=""
containerRegistryLoginServer=""
resourceGroupName=""
dataIngestApp=""
missing_keys=()

if ! containerRegistryName="$(get_config_value "CONTAINER_REGISTRY_NAME")"; then
  missing_keys+=("CONTAINER_REGISTRY_NAME")
fi
if ! containerRegistryLoginServer="$(get_config_value "CONTAINER_REGISTRY_LOGIN_SERVER")"; then
  missing_keys+=("CONTAINER_REGISTRY_LOGIN_SERVER")
fi
if ! resourceGroupName="$(get_config_value "AZURE_RESOURCE_GROUP")"; then
  missing_keys+=("AZURE_RESOURCE_GROUP")
fi
if ! dataIngestApp="$(get_config_value "DATA_INGEST_APP_NAME")"; then
  missing_keys+=("DATA_INGEST_APP_NAME")
fi

if [[ ${#missing_keys[@]} -gt 0 ]]; then
  echo -e "${YELLOW}⚠️  Missing or invalid App Config keys: ${missing_keys[*]}${NC}"
  exit 1
fi

echo -e "${GREEN}✅ All App Configuration values retrieved:${NC}"
echo "   containerRegistryName = $containerRegistryName"
echo "   containerRegistryLoginServer = $containerRegistryLoginServer"
echo "   resourceGroupName = $resourceGroupName"
echo "   dataIngestApp = $dataIngestApp"
echo

docker_ready() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

buildMode="${BUILD_MODE:-}"
if [[ -z "$buildMode" ]]; then
  if [[ "${NETWORK_ISOLATION:-}" == "true" || -n "${ACR_TASK_AGENT_POOL:-}" ]]; then
    buildMode="acr-task"
  elif [[ "${USE_DOCKER:-}" == "true" ]] && docker_ready; then
    buildMode="local"
  elif docker_ready; then
    buildMode="local"
  else
    buildMode="acr-task"
  fi
fi

if [[ "$buildMode" != "local" && "$buildMode" != "acr-task" ]]; then
  echo -e "${YELLOW}⚠️  Unsupported BUILD_MODE '${buildMode}'. Use 'local' or 'acr-task'.${NC}"
  exit 1
fi
if [[ "$buildMode" == "local" ]] && ! docker_ready; then
  echo -e "${YELLOW}⚠️  BUILD_MODE=local requested, but Docker is not available.${NC}"
  exit 1
fi

echo -e "${GREEN}✅ Build mode: ${buildMode}${NC}"
if [[ "$buildMode" == "local" ]]; then
  echo -e "${GREEN}🔐 Logging into ACR (${containerRegistryName} in ${resourceGroupName})…${NC}"
  az acr login --name "${containerRegistryName}" --resource-group "${resourceGroupName}"
  echo -e "${GREEN}✅ Logged into ACR.${NC}"
  echo
else
  echo -e "${GREEN}✅ Using remote ACR build; local Docker login is not required.${NC}"
  echo
fi

echo -e "${BLUE}🛢️ Defining tag…${NC}"
if [[ -n "${tag:-}" ]]; then
  tag="${tag}"
  echo -e "${GREEN}Using tag from environment: ${tag}${NC}"
else
  if gitShort=$(git rev-parse --short HEAD 2>/dev/null); then
    if [[ -n "$gitShort" ]]; then
      tag="$gitShort"
      echo -e "${GREEN}Using Git short HEAD as tag: ${tag}${NC}"
    else
      echo -e "${YELLOW}Could not get Git short HEAD. Generating random tag.${NC}"
      rand=$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')
      rand=$(( rand % 900000 + 100000 ))
      tag="GPT${rand}"
      echo -e "${GREEN}Generated random tag: ${tag}${NC}"
    fi
  else
    echo -e "${YELLOW}Git command failed. Generating random tag.${NC}"
    rand=$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')
    rand=$(( rand % 900000 + 100000 ))
    tag="GPT${rand}"
    echo -e "${GREEN}Generated random tag: ${tag}${NC}"
  fi
fi

imageRef="${containerRegistryLoginServer}/azure-gpt-rag/data-ingestion:${tag}"

if [[ "$buildMode" == "local" ]]; then
  echo -e "${GREEN}🛠️  Building Docker image (local docker)…${NC}"
  docker build \
    --platform linux/amd64 \
    -t "$imageRef" \
    .

  echo
  echo -e "${GREEN}📤 Pushing image…${NC}"
  docker push "$imageRef"
  echo -e "${GREEN}✅ Image pushed.${NC}"
else
  echo -e "${GREEN}🛠️  Building image remotely via 'az acr build'…${NC}"
  acr_build_args=(acr build --registry "${containerRegistryName}" --image "azure-gpt-rag/data-ingestion:${tag}" --file Dockerfile)
  if [[ -n "${ACR_TASK_AGENT_POOL:-}" ]]; then
    az acr agentpool show --registry "${containerRegistryName}" --name "${ACR_TASK_AGENT_POOL}" --resource-group "${resourceGroupName}" --only-show-errors >/dev/null
    acr_build_args+=(--agent-pool "${ACR_TASK_AGENT_POOL}")
  fi
  acr_build_args+=(.)
  az "${acr_build_args[@]}"
  echo -e "${GREEN}✅ Remote build completed.${NC}"
fi

echo
echo -e "${GREEN}🔄 Updating container app…${NC}"
az containerapp update \
  --name "${dataIngestApp}" \
  --resource-group "${resourceGroupName}" \
  --image "$imageRef"
echo -e "${GREEN}✅ Container app updated.${NC}"

echo
echo -e "${GREEN}🌐 Updating container app ingress target port…${NC}"
az containerapp ingress update \
  --name "${dataIngestApp}" \
  --resource-group "${resourceGroupName}" \
  --target-port 8080
echo -e "${GREEN}✅ Ingress target port updated.${NC}"
