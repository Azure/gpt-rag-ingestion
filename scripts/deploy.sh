#!/usr/bin/env bash
set -euo pipefail

DEBUG=${DEBUG:-false}
if [[ "$DEBUG" == "true" ]]; then
  set -x
fi

YELLOW='\033[0;33m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

label="gpt-rag"
imageRepository="data-ingestion"
appConfigKey="DATA_INGEST_APP_NAME"
identitySuffix="dataingest"

info() { echo -e "${BLUE}$*${NC}" >&2; }
success() { echo -e "${GREEN}$*${NC}" >&2; }
warn() { echo -e "${YELLOW}$*${NC}" >&2; }
error() { echo -e "${RED}$*${NC}" >&2; }

select_cli_value() {
  local expected="${1:-}"
  local line trimmed fallback=""

  while IFS= read -r line; do
    trimmed="$(printf "%s" "$line" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    [[ -z "$trimmed" ]] && continue

    if [[ -n "$expected" && "$trimmed" == "$expected" ]]; then
      printf "%s" "$trimmed"
      return 0
    fi

    case "$trimmed" in
      WARNING:*|ERROR:*|Running.*|*site-packages*UserWarning:*|from\ cryptography*|D:\\a\\_work\\*)
        continue
        ;;
    esac

    fallback="$trimmed"
  done

  printf "%s" "$fallback"
}

unique_candidates() {
  local key="$1"
  local upper lower
  upper="$(printf "%s" "$key" | tr '[:lower:]' '[:upper:]')"
  lower="$(printf "%s" "$key" | tr '[:upper:]' '[:lower:]')"

  printf "%s\n" "$key"
  [[ "$upper" != "$key" ]] && printf "%s\n" "$upper"
  [[ "$lower" != "$key" && "$lower" != "$upper" ]] && printf "%s\n" "$lower"
}

get_azd_value() {
  local key="$1"
  local value=""

  if command -v azd >/dev/null 2>&1; then
    value="$(azd env get-values 2>/dev/null | awk -F= -v k="$key" '
      $1==k { v=$0; sub(/^[^=]*=/, "", v); gsub(/^"|"$/, "", v); print v; exit }' || true)"
  fi

  printf "%s" "$value" | tr -d '\r'
}

get_config_value() {
  local key="$1"
  local candidate output status last_output=""

  while IFS= read -r candidate; do
    [[ -z "$candidate" ]] && continue
    info "Retrieving '$candidate' from App Configuration..."
    set +e
    output="$(az appconfig kv show \
      --endpoint "$APP_CONFIG_ENDPOINT" \
      --key "$candidate" \
      --label "$label" \
      --auth-mode login \
      --only-show-errors \
      --query value -o tsv 2>&1)"
    status=$?
    set -e

    output="$(printf "%s" "$output" | select_cli_value)"
    if [[ $status -eq 0 && -n "${output//[[:space:]]/}" ]]; then
      printf "%s" "$output"
      return 0
    fi
    last_output="$output"
  done < <(unique_candidates "$key")

  warn "Failed to retrieve key '$key'. Last CLI output: $last_output"
  return 1
}

require_config_value() {
  local key="$1"
  local value
  if ! value="$(get_config_value "$key")"; then
    return 1
  fi
  printf "%s" "$value"
}

docker_ready() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

select_build_mode() {
  local mode="${BUILD_MODE:-}"
  mode="$(printf "%s" "$mode" | tr '[:upper:]' '[:lower:]' | xargs)"

  if [[ -z "$mode" && -n "${USE_DOCKER:-}" ]]; then
    case "$(printf "%s" "$USE_DOCKER" | tr '[:upper:]' '[:lower:]')" in
      true|1|yes) mode="local" ;;
      false|0|no) mode="acr-task" ;;
    esac
  fi

  if [[ -z "$mode" ]]; then
    if [[ "$(printf "%s" "${NETWORK_ISOLATION:-}" | tr '[:upper:]' '[:lower:]')" == "true" || -n "${ACR_TASK_AGENT_POOL:-}" ]]; then
      mode="acr-task"
    elif docker_ready; then
      mode="local"
    else
      mode="acr-task"
    fi
  fi

  if [[ "$mode" != "local" && "$mode" != "acr-task" ]]; then
    error "Unsupported BUILD_MODE '$mode'. Use 'local' or 'acr-task'."
    exit 1
  fi

  if [[ "$mode" == "local" ]] && ! docker_ready; then
    error "BUILD_MODE=local requested, but Docker is not available."
    exit 1
  fi

  printf "%s" "$mode"
}

determine_tag() {
  if [[ -n "${tag:-}" ]]; then
    printf "%s" "$tag"
    return
  fi

  local git_short=""
  git_short="$(git rev-parse --short HEAD 2>/dev/null || true)"
  if [[ -n "$git_short" ]]; then
    printf "%s" "$git_short"
  else
    printf "GPT%s" "$(( RANDOM % 900000 + 100000 ))"
  fi
}

set_containerapp_registry() {
  local app_name="$1"
  local identity_type

  identity_type="$(az containerapp identity show \
    --name "$app_name" \
    --resource-group "$resourceGroupName" \
    --only-show-errors \
    --query type -o tsv 2>&1 | select_cli_value)"

  if [[ "$identity_type" == *"UserAssigned"* ]]; then
    if [[ -z "${subscriptionId:-}" || -z "${resourceToken:-}" ]]; then
      error "SUBSCRIPTION_ID and RESOURCE_TOKEN are required for user-assigned registry identity."
      exit 1
    fi

    az containerapp registry set \
      --name "$app_name" \
      --resource-group "$resourceGroupName" \
      --server "$containerRegistryLoginServer" \
      --identity "/subscriptions/${subscriptionId}/resourceGroups/${resourceGroupName}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/uai-ca-${resourceToken}-${identitySuffix}" \
      --only-show-errors
  else
    az containerapp registry set \
      --name "$app_name" \
      --resource-group "$resourceGroupName" \
      --server "$containerRegistryLoginServer" \
      --identity system \
      --only-show-errors
  fi
}

verify_containerapp_image() {
  local app_name="$1"
  local expected_image="$2"
  local actual_image

  actual_image="$(az containerapp show \
    --name "$app_name" \
    --resource-group "$resourceGroupName" \
    --only-show-errors \
    --query 'properties.template.containers[0].image' -o tsv 2>&1 | select_cli_value "$expected_image")"

  if [[ -z "${actual_image//[[:space:]]/}" || "$actual_image" == "null" ]]; then
    error "Could not determine configured image for '$app_name'."
    exit 1
  fi

  if [[ "$actual_image" != "$expected_image" ]]; then
    error "Container app '$app_name' is configured with '$actual_image' instead of '$expected_image'."
    exit 1
  fi
}

echo
shell_app_config_endpoint=""
if [[ -n "${APP_CONFIG_ENDPOINT:-}" ]]; then
  shell_app_config_endpoint="$(printf "%s" "${APP_CONFIG_ENDPOINT}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
fi
azd_app_config_endpoint="$(get_azd_value "APP_CONFIG_ENDPOINT")"

if [[ -n "$shell_app_config_endpoint" && -n "$azd_app_config_endpoint" ]]; then
  shell_lower="$(printf "%s" "$shell_app_config_endpoint" | tr '[:upper:]' '[:lower:]')"
  azd_lower="$(printf "%s" "$azd_app_config_endpoint" | tr '[:upper:]' '[:lower:]')"
  if [[ "$shell_lower" != "$azd_lower" ]]; then
    warn "Warning: APP_CONFIG_ENDPOINT in your shell does not match the azd environment."
    warn "  shell  (\$APP_CONFIG_ENDPOINT)  : ${shell_app_config_endpoint}"
    warn "  azd env (APP_CONFIG_ENDPOINT)  : ${azd_app_config_endpoint}"
    warn "Using shell value: ${shell_app_config_endpoint}"
    warn "To use the azd env value instead, run: unset APP_CONFIG_ENDPOINT"
  fi
fi

if [[ -n "$shell_app_config_endpoint" ]]; then
  APP_CONFIG_ENDPOINT="$shell_app_config_endpoint"
  success "Using APP_CONFIG_ENDPOINT from environment: ${APP_CONFIG_ENDPOINT}"
else
  info "Fetching APP_CONFIG_ENDPOINT from azd env..."
  APP_CONFIG_ENDPOINT="$azd_app_config_endpoint"
fi

if [[ -z "${APP_CONFIG_ENDPOINT:-}" ]]; then
  warn "Missing APP_CONFIG_ENDPOINT."
  echo "Set it with: azd env set APP_CONFIG_ENDPOINT <your-endpoint>"
  echo "Or export APP_CONFIG_ENDPOINT=<your-endpoint> before running this script."
  exit 1
fi
success "APP_CONFIG_ENDPOINT: ${APP_CONFIG_ENDPOINT}"

info "Checking Azure CLI login and subscription..."
if ! az account show >/dev/null 2>&1; then
  error "Not logged in. Please run 'az login'."
  exit 1
fi
success "Azure CLI is logged in."

buildMode="$(select_build_mode)"
success "Build mode: ${buildMode}"

info "Loading App Configuration settings (label=${label})..."
containerRegistryName="$(require_config_value "CONTAINER_REGISTRY_NAME")"
containerRegistryLoginServer="$(require_config_value "CONTAINER_REGISTRY_LOGIN_SERVER")"
resourceGroupName="$(require_config_value "AZURE_RESOURCE_GROUP")"
appName="$(require_config_value "$appConfigKey")"
subscriptionId="$(get_config_value "SUBSCRIPTION_ID" || az account show --only-show-errors --query id -o tsv 2>&1 | select_cli_value)"
resourceToken="$(get_config_value "RESOURCE_TOKEN" || true)"

success "All App Configuration values retrieved:"
echo "   containerRegistryName = $containerRegistryName"
echo "   containerRegistryLoginServer = $containerRegistryLoginServer"
echo "   resourceGroupName = $resourceGroupName"
echo "   appName = $appName"
echo

if [[ "$buildMode" == "local" ]]; then
  info "Logging into ACR (${containerRegistryName} in ${resourceGroupName})..."
  az acr login --name "$containerRegistryName" --resource-group "$resourceGroupName"
  success "Logged into ACR."
else
  success "Using remote ACR build; local Docker login is not required."
fi

tag="$(determine_tag)"
success "Using image tag: ${tag}"
imageRef="${containerRegistryLoginServer}/azure-gpt-rag/${imageRepository}:${tag}"

if [[ "$buildMode" == "local" ]]; then
  info "Building Docker image..."
  docker build --platform linux/amd64 -t "$imageRef" .

  info "Pushing image..."
  docker push "$imageRef"
  success "Image pushed."
else
  info "Building image remotely via az acr build..."
  acr_build_args=(acr build --registry "$containerRegistryName" --image "azure-gpt-rag/${imageRepository}:${tag}" --file Dockerfile)
  if [[ -n "${ACR_TASK_AGENT_POOL:-}" ]]; then
    az acr agentpool show --registry "$containerRegistryName" --name "$ACR_TASK_AGENT_POOL" --resource-group "$resourceGroupName" --only-show-errors >/dev/null
    acr_build_args+=(--agent-pool "$ACR_TASK_AGENT_POOL")
  fi
  acr_build_args+=(.)
  az "${acr_build_args[@]}"
  success "Remote build completed."
fi

info "Updating container app registry..."
set_containerapp_registry "$appName"
success "Container app registry updated."

info "Updating container app image..."
latestRevision="$(az containerapp update \
  --name "$appName" \
  --resource-group "$resourceGroupName" \
  --image "$imageRef" \
  --only-show-errors \
  --query properties.latestRevisionName -o tsv 2>&1 | select_cli_value)"
success "Container app updated."
if [[ -n "${latestRevision//[[:space:]]/}" && "$latestRevision" != "null" ]]; then
  success "Latest revision: ${latestRevision}"
fi

info "Ensuring container app ingress target port is 8080..."
az containerapp ingress update \
  --name "$appName" \
  --resource-group "$resourceGroupName" \
  --target-port 8080 \
  --only-show-errors
success "Ingress target port updated."

info "Verifying container app image..."
verify_containerapp_image "$appName" "$imageRef"
success "Container app image verified."
