import openai
import os
import re
import logging
import tiktoken
from tenacity import retry, wait_random_exponential, stop_after_attempt  
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")   
    retrieved_secret = client.get_secret(secretName)
    return retrieved_secret.value

class TextEmbedder():
    openai.api_type = "azure"    
    openai.api_key = get_secret('azureOpenAIKey')
    openai.api_base = f"https://{os.getenv('AZURE_OPENAI_SERVICE_NAME')}.openai.azure.com/"
    openai.api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
    
    def estimate_tokens(self, text: str) -> int:
        gpt2_tokenizer = tiktoken.get_encoding("gpt2")
        return len(gpt2_tokenizer.encode(text))

    def clean_text(self, text, token_limit=8191):
        # Clean up text (e.g. line breaks, )    
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[\n\r]+', ' ', text).strip()
        # Truncate text if necessary (for, ada-002 max is 8191 tokens)    
        if self.estimate_tokens(text) > token_limit:
            logging.warning("Token limit reached exceeded maximum length, truncating...")
            while self.estimate_tokens(text) > token_limit:
                text = text[:-1]
        return text

    @retry(reraise=True, wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def embed_content(self, text, clean_text=True, use_single_precision=True):
        import time
        embedding_precision = 9 if use_single_precision else 18
        if clean_text:
            text = self.clean_text(text)
        response = openai.Embedding.create(input=text, engine=self.AZURE_OPENAI_EMBEDDING_DEPLOYMENT)

        embedding = [round(x, embedding_precision) for x in response['data'][0]['embedding']] # type: ignore
        return embedding