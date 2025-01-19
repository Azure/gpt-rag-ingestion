from openai import AzureOpenAI
import os

import os
import re
import logging
import tiktoken
import time
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

client = AzureOpenAI(api_key=get_secret('azureOpenAIKey'),
azure_endpoint=f"https://{os.getenv('AZURE_OPENAI_SERVICE_NAME')}.openai.azure.com/",
api_version=os.getenv("AZURE_OPENAI_API_VERSION"))

class TextEmbedder():
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")

    def estimate_tokens(self, text: str) -> int:
        gpt2_tokenizer = tiktoken.get_encoding("gpt2")
        return len(gpt2_tokenizer.encode(text))

    def clean_text(self, text, token_limit=8191):
            # Clean up text (e.g. line breaks)
            text = re.sub(r'\s+', ' ', text).strip()
            text = re.sub(r'[\n\r]+', ' ', text).strip()

            # Truncate text if necessary (for ada-002 max is 8191 tokens)
            if self.estimate_tokens(text) > token_limit:
                logging.warning("Token limit reached exceeded maximum length, truncating...")
                step_size = 1  # Initial step size
                iteration = 0  # Iteration counter

                while self.estimate_tokens(text) > token_limit:
                    text = text[:-step_size]
                    iteration += 1

                    # Increase step size exponentially every 5 iterations
                    if iteration % 5 == 0:
                        step_size = min(step_size * 2, 100)

            return text

    def extract_retry_seconds(self, error_message):
        match = re.search(r'retry after (\d+)', error_message)
        if match:
            return int(match.group(1))
        else:
            return 60 # default to 60 seconds in case it can't be extracted

    @retry(reraise=True, stop=stop_after_attempt(6))
    def embed_content(self, text, clean_text=True, use_single_precision=True):
        embedding_precision = 9 if use_single_precision else 18
        if clean_text:
            text = self.clean_text(text)        
        try:
            response = client.embeddings.create(input=text, model=self.AZURE_OPENAI_EMBEDDING_DEPLOYMENT)
            embedding = [round(x, embedding_precision) for x in response.data[0].embedding] # type: ignore
            return embedding            
        except Exception as e:
            error_message = str(e)
            seconds = self.extract_retry_seconds(error_message) * 2
            logging.warning(f"Embeddings model deployment rate limit exceeded, retrying in {seconds} seconds...")
            time.sleep(seconds)
            raise e # to make tenacity retry