import os
import logging

from typing import Dict, Any
from azure.identity import ChainedTokenCredential, ManagedIdentityCredential, AzureCliCredential
from azure.identity.aio import ChainedTokenCredential as AsyncChainedTokenCredential, ManagedIdentityCredential as AsyncManagedIdentityCredential, AzureCliCredential as AsyncAzureCliCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.core.exceptions import AzureError
from azure.appconfiguration.provider import (
    AzureAppConfigurationKeyVaultOptions,
    load,
    SettingSelector
)

from tenacity import retry, wait_random_exponential, stop_after_attempt, RetryError

class AppConfigClient:

    credential = None
    aiocredential = None

    def __init__(self):
        """
        Initializes the App Configuration client with multi-layer authentication fallback.
        
        Authentication Strategy:
        1. Reads AZURE_CLIENT_ID from environment (if present) to specify a User-Assigned Managed Identity
        2. Creates ChainedTokenCredential with fallback order:
           a) ManagedIdentityCredential - for production (VMs, Container Apps, App Services)
              - Uses client_id if provided (User-Assigned MI)
              - Uses default system identity if client_id is empty (System-Assigned MI)
           b) AzureCliCredential - for local development (uses 'az login' credentials)
        
        Configuration Loading Priority:
        1. Connects to Azure App Configuration using the credential chain
        2. Loads keys with labels in order: 'gpt-rag-ingestion', 'gpt-rag', no-label
        3. Falls back to connection string if credential auth fails
        4. Falls back to direct os.environ reads if all Azure connections fail
        
        Environment Variables Required for Bootstrap:
        - APP_CONFIG_ENDPOINT: Azure App Configuration endpoint (required)
        - AZURE_CLIENT_ID: Client ID for User-Assigned Managed Identity (optional)
        - AZURE_APPCONFIG_CONNECTION_STRING: Fallback connection string (optional)
        """
        # Read client_id for Managed Identity authentication (optional - empty string is valid)
        # Azure automatically injects this for User-Assigned MI; remains empty for System-Assigned MI
        self.client_id = os.environ.get('AZURE_CLIENT_ID', "")
        
        # Control flag for allowing direct environment variable fallback in get_value()
        self.allow_env_vars = False
        if "allow_environment_variables" in os.environ:
            self.allow_env_vars = bool(os.environ["allow_environment_variables"])

        # Required: Azure App Configuration endpoint for remote configuration
        endpoint = os.getenv("APP_CONFIG_ENDPOINT")
        if not endpoint:
            raise EnvironmentError("APP_CONFIG_ENDPOINT must be set")

        # Build credential chain: prefer Managed Identity (production), fallback to CLI (dev)
        self.credential = ChainedTokenCredential(
            ManagedIdentityCredential(client_id=self.client_id),  # Production auth
            AzureCliCredential()  # Local development auth
        )
        # Async version for async Azure SDK clients
        self.aiocredential = AsyncChainedTokenCredential(
            AsyncManagedIdentityCredential(client_id=self.client_id),
            AsyncAzureCliCredential()
        )

        # Define label selectors for configuration priority (most specific to least specific)
        app_label_selector = SettingSelector(label_filter='gpt-rag-ingestion', key_filter='*')
        base_label_selector = SettingSelector(label_filter='gpt-rag', key_filter='*')
        no_label_selector = SettingSelector(label_filter=None, key_filter='*')

        # Attempt 1: Connect to App Configuration using credential-based auth (Managed Identity or CLI)
        try:
            self.client = load(
                selects=[app_label_selector, base_label_selector, no_label_selector],
                endpoint=endpoint,
                credential=self.credential,
                key_vault_options=AzureAppConfigurationKeyVaultOptions(credential=self.credential),
            )
        except Exception as e:
            logging.error(
                "Unable to connect to Azure App Configuration via endpoint. %s",
                e,
                exc_info=True,
            )
            # Attempt 2: Fallback to connection string-based auth (less secure, for legacy scenarios)
            connection_string = os.environ.get("AZURE_APPCONFIG_CONNECTION_STRING")
            if connection_string:
                try:
                    self.client = load(
                        connection_string=connection_string,
                        key_vault_options=AzureAppConfigurationKeyVaultOptions(credential=self.credential),
                    )
                except Exception as e2:
                    logging.error(
                        "Unable to connect to Azure App Configuration via connection string. %s",
                        e2,
                        exc_info=True,
                    )
                    raise
            else:
                # Attempt 3: Last resort fallback - direct environment variable reads (no Azure dependency)
                logging.warning(
                    "AZURE_APPCONFIG_CONNECTION_STRING not set; AppConfig lookups will rely on environment variables only."
                )
                # Create a minimal shim that mimics the App Config client interface but reads from os.environ
                class _EnvOnly:
                    def __getitem__(self, key):
                        val = os.environ.get(key)
                        if val is None:
                            raise KeyError(key)
                        return val

                self.client = _EnvOnly()


    def get(self, key: str, default: Any = None, type: type = str, allow_none : bool = False) -> Any:
        return self.get_value(key, default=default, allow_none=allow_none, type=type)

    def get_value(self, key: str, default: str = None, allow_none: bool = False, type: type = str) -> str:

        if key is None:
            raise Exception('The key parameter is required for get_value().')

        value = None

        allow_env_vars = False
        if "allow_environment_variables" in os.environ:
            allow_env_vars = bool(os.environ[
                    "allow_environment_variables"
                    ])

        if allow_env_vars is True:
            value = os.environ.get(key)

        if value is None:
            try:
                # If self.client behaves like a mapping, try it; otherwise skip
                if isinstance(self.client, dict):
                    value = None  # no value from config provider stub
                else:
                    value = self.get_config_with_retry(name=key)
            except Exception:
                value = None

        if value is not None:
            if type is not None:
                if type is bool:
                    if isinstance(value, str):
                        value = value.lower() in ['true', '1', 'yes']
                else:
                    try:
                        value = type(value)
                    except ValueError as e:
                        raise Exception(f'Value for {key} could not be converted to {type.__name__}. Error: {e}')
            return value
        else:
            if default is not None or allow_none is True:
                return default
            
            raise Exception(f'The configuration variable {key} not found.')
        
    def retry_before_sleep(self, retry_state):
        # Log the outcome of each retry attempt.
        message = f"""Retrying {retry_state.fn}:
                        attempt {retry_state.attempt_number}
                        ended with: {retry_state.outcome}"""
        if retry_state.outcome.failed:
            ex = retry_state.outcome.exception()
            message += f"; Exception: {ex.__class__.__name__}: {ex}"
        if retry_state.attempt_number < 1:
            logging.info(message)
        else:
            logging.warning(message)

    @retry(
        wait=wait_random_exponential(multiplier=1, max=5),
        stop=stop_after_attempt(5),
        before_sleep=retry_before_sleep
    )
    def get_config_with_retry(self, name):
        try:
            return self.client[name]
        except RetryError:
            raise

    # Helper functions for reading environment variables
    def read_env_variable(self, var_name, default=None):
        value = self.get_value(var_name, default)
        return value.strip() if value else default

    def read_env_list(self, var_name):
        value = self.get_value(var_name, "")
        return [item.strip() for item in value.split(",") if item.strip()]

    def read_env_boolean(self, var_name, default=False):
        value = self.get_value(var_name, str(default)).strip().lower()
        return value in ['true', '1', 'yes']