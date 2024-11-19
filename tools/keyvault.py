import os
import logging
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient as AsyncSecretClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError

class KeyVaultClient:
    """
    KeyVaultClient provides methods to retrieve secrets from an Azure Key Vault.
    """

    def __init__(self):
        self.key_vault_name = os.getenv("AZURE_KEY_VAULT_NAME")
        if not self.key_vault_name:
            logging.warning("Environment variable AZURE_KEY_VAULT_NAME not found.")
        self.kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
        self.credential = AsyncDefaultAzureCredential()

    async def get_secret(self, secret_name):
        """
        Retrieves the value of a secret from Azure Key Vault.
        
        Parameters:
        secret_name (str): The name of the secret to retrieve.

        Returns:
        str: The value of the secret, or None if not found or an error occurs.
        """
        if not self.key_vault_name:
            logging.error(f"[keyvault] Key Vault name is not configured.")
            return None

        try:
            async with AsyncSecretClient(vault_url=self.kv_uri, credential=self.credential) as client:
                retrieved_secret = await client.get_secret(secret_name)
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
        await self.credential.close()

# Usage example
# async def main():
#     keyvault_client = KeyVaultClient()
#     secret_value = await keyvault_client.get_secret("my-secret")
#     print(secret_value)
#     await keyvault_client.close()
#
# asyncio.run(main())
