import os
import logging
from azure.identity import DefaultAzureCredential
from azure.appconfiguration.provider import (
    AzureAppConfigurationKeyVaultOptions,
    load
)

from tenacity import retry, wait_random_exponential, stop_after_attempt, RetryError

class Configuration:

    credential : None

    def __init__(self):

        try:
            self.tenant_id = os.environ.get('AZURE_TENANT_ID', "*")
        except Exception as e:
            raise e

        self.credential = DefaultAzureCredential(
            additionally_allowed_tenants=self.tenant_id,
            exclude_environment_credential=True, 
            exclude_managed_identity_credential=True,
            exclude_cli_credential=True,
            exclude_powershell_credential=True,
            exclude_shared_token_cache_credential=True,
            exclude_developer_cli_credential=True,
            exclude_interactive_browser_credential=False
            )

        try:
            app_config_uri = os.environ['APP_CONFIGURATION_URI']
        except Exception as e:
            raise e

        connection_string = os.environ["AZURE_APPCONFIG_CONNECTION_STRING"]

        try:
        # Connect to Azure App Configuration using a connection string.
            self.config = load(connection_string=connection_string, key_vault_options=AzureAppConfigurationKeyVaultOptions(credential=self.credential))
        except Exception as e:
            print(e)

    # Connect to Azure App Configuration.
    #config = load(endpoint=app_config_uri, credential=credential,key_vault_options=AzureAppConfigurationKeyVaultOptions(credential=credential))

    def get_value(self, key: str, default: str = None) -> str:
        
        if key is None:
            raise Exception('The key parameter is required for get_value().')

        value = None

        allow_env_vars = False
        if "allow-environment-variables" in os.environ:
            allow_env_vars = bool(os.environ[
                    "allow-environment-variables"
                    ])

        if allow_env_vars is True:
            value = os.environ.get(key)

        if value is None:
            try:
                value = self.get_config_with_retry(name=key)
            except Exception as e:
                pass

        if value is not None:
            return value
        else:
            if default is not None:
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
            return self.config[name]
        except RetryError:
            pass

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