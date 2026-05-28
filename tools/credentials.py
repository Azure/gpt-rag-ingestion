import os
from typing import Any


def get_azure_client_id(config_client: Any | None = None) -> str | None:
    """Resolve the user-assigned managed identity client ID.

    App Configuration remains authoritative when the key exists; the Container
    App environment variable is used as a fallback because it is injected by
    the platform for runtime authentication.
    """
    if config_client is not None:
        client_id = config_client.get("AZURE_CLIENT_ID", None, allow_none=True)
        if client_id:
            return client_id

    return os.environ.get("AZURE_CLIENT_ID") or None
