import logging
from azure.identity.aio import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.keyvault.secrets.aio import SecretClient as AsyncSecretClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
from dependencies import get_config

app_config_client = get_config()

class KeyVaultClient:
    """
    KeyVaultClient provides methods to retrieve secrets from an Azure Key Vault.
    """

    def __init__(self):
        self.kv_uri = app_config_client.get("KEY_VAULT_URI")
        if not self.kv_uri:
            logging.error("[keyvault] KEY_VAULT_URI environment variable not set.")
            raise ValueError("KEY_VAULT_URI environment variable not set.")
        
        # Initialize the ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential
        try:
            client_id = app_config_client.get('AZURE_CLIENT_ID', None, allow_none=True) or None

            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(client_id=client_id),
                AzureCliCredential()
            )
            logging.debug("[keyvault] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[keyvault] Failed to initialize ChainedTokenCredential: {e}")
            raise
        
        self.clients = {}  # Cache SecretClient instances if needed

    async def get_secret(self, secret_name):
        """
        Retrieves the value of a secret from Azure Key Vault.
        
        Parameters:
        secret_name (str): The name of the secret to retrieve.

        Returns:
        str: The value of the secret, or None if not found or an error occurs.
        """
        if not self.kv_uri:
            logging.error("[keyvault] Key Vault URI is not configured.")
            return None

        try:
            async with AsyncSecretClient(vault_url=self.kv_uri, credential=self.credential) as client:
                retrieved_secret = await client.get_secret(secret_name)
                logging.debug(f"[keyvault] Successfully retrieved secret '{secret_name}'.")
                return retrieved_secret.value
        except ClientAuthenticationError:
            logging.error(f"[keyvault] Authentication failed when reading '{secret_name}'. Please check your credentials.")
            return None
        except ResourceNotFoundError:
            logging.debug(f"[keyvault] Secret '{secret_name}' not found in the Key Vault.")
            return None
        except Exception as e:
            logging.error(f"[keyvault] An unexpected error occurred when reading '{secret_name}': {e}")
            return None

    async def close(self):
        """
        Closes the credential client session.
        """
        if self.credential:
            await self.credential.close()
            logging.debug("[keyvault] Credential has been closed.")
