import os
from typing import Dict, Any
from azure.identity import ChainedTokenCredential, ManagedIdentityCredential, AzureCliCredential
from azure.appconfiguration import AzureAppConfigurationClient
from azure.core.exceptions import AzureError

class AppConfigClient:
    def __init__(self):
        """
        Bulk-loads all keys labeled 'gpt-rag-ingestion' and 'gpt-rag' into an in-memory dict,
        giving precedence to 'gpt-rag-ingestion' where a key exists in both.
        """
        endpoint = os.getenv("APP_CONFIG_ENDPOINT")
        if not endpoint:
            raise EnvironmentError("APP_CONFIG_ENDPOINT must be set")

        credential = ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())
        # make client available to other methods
        self.client = AzureAppConfigurationClient(base_url=endpoint, credential=credential)

        self._settings: Dict[str, str] = {}
        self._load_settings()

    def _load_settings(self):
        # 1) Load everything labeled “gpt-rag-ingestion”
        try:
            for setting in self.client.list_configuration_settings(label_filter="gpt-rag-ingestion"):
                self._settings[setting.key] = setting.value
        except AzureError as e:
            raise RuntimeError(f"Failed to bulk-load 'gpt-rag-ingestion' settings: {e}")

        # 2) Load “gpt-rag” ones only if not already present
        try:
            for setting in self.client.list_configuration_settings(label_filter="gpt-rag"):
                self._settings.setdefault(setting.key, setting.value)
        except AzureError as e:
            raise RuntimeError(f"Failed to bulk-load 'gpt-rag' settings: {e}")

    def apply_environment_settings(self) -> None:
        """
        Pushes loaded settings into os.environ.
        Keys from 'gpt-rag-ingestion' will overwrite any existing env-vars;
        keys from 'gpt-rag' will only be set if not already present.
        """
        # first, ingestion (always overwrite)
        for setting in self.client.list_configuration_settings(label_filter="gpt-rag-ingestion"):
            os.environ[setting.key] = setting.value

        # then, rag—but only if the key isn't already in os.environ
        for setting in self.client.list_configuration_settings(label_filter="gpt-rag"):
            os.environ.setdefault(setting.key, setting.value)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Returns the in-memory value for the given key.

        If the key was not found under either label, returns `default`.
        """
        return self._settings.get(key, default)
