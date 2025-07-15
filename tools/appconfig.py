import os
from typing import Dict, Any
from azure.identity import ChainedTokenCredential, ManagedIdentityCredential, AzureCliCredential
from azure.identity.aio import ChainedTokenCredential as AsyncChainedTokenCredential, ManagedIdentityCredential as AsyncManagedIdentityCredential, AzureCliCredential as AsyncAzureCliCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.core.exceptions import AzureError

class AppConfigClient:

    credential = None
    aiocredential = None
    allow_env_vars = False

    def __init__(self):
        """
        Bulk-loads all keys labeled 'orchestrator' and 'gpt-rag' into an in-memory dict,
        giving precedence to 'orchestrator' where a key exists in both.
        """
        
        client_id = os.getenv("AZURE_CLIENT_ID")

        self.allow_env_vars = False
        
        if "allow_environment_variables" in os.environ:
            self.allow_env_vars = bool(os.environ[
                "allow_environment_variables"
                ])
       
        endpoint = os.getenv("APP_CONFIG_ENDPOINT")
        if not endpoint:
            raise EnvironmentError("APP_CONFIG_ENDPOINT must be set")

        self.credential = ChainedTokenCredential(ManagedIdentityCredential(client_id=client_id), AzureCliCredential())
        self.aiocredential = AsyncChainedTokenCredential(AsyncManagedIdentityCredential(client_id=client_id), AsyncAzureCliCredential())
        client = AzureAppConfigurationClient(base_url=endpoint, credential=self.credential)

        self._settings: Dict[str, str] = {}

        # 1) Load everything labeled “gpt-rag-ingestion”
        try:
            for setting in client.list_configuration_settings(label_filter="gpt-rag-ingestion"):
                self._settings[setting.key] = setting.value
        except AzureError as e:
            raise RuntimeError(f"Failed to bulk-load 'gpt-rag-ingestion' settings: {e}")

        # 2) Load “gpt-rag” ones only if not already present
        try:
            for setting in client.list_configuration_settings(label_filter="gpt-rag"):
                self._settings.setdefault(setting.key, setting.value)
        except AzureError as e:
            raise RuntimeError(f"Failed to bulk-load 'gpt-rag' settings: {e}")

    def get(self, key: str, default: Any = None, type: type = str) -> Any:
        """
        Returns the in-memory value for the given key.

        If the key was not found under either label, returns `default`.
        """
        value = self._settings.get(key, default)

        if self.allow_env_vars is True:
            value = os.environ.get(key)

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
