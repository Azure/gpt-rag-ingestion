import openai
from openai import AsyncOpenAI,AsyncAzureOpenAI
import os
import re
import logging
import tiktoken
import time
import asyncio
from tenacity import retry, wait_random_exponential, stop_after_attempt  
from azure.keyvault.secrets.aio import SecretClient
from azure.identity.aio import DefaultAzureCredential

async def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")   
    retrieved_secret = await client.get_secret(secretName)
    await client.close()
    await credential.close()
    return retrieved_secret.value

class TextEmbedder:
    def __init__(self,embedding_deployment,client:AsyncAzureOpenAI):
        self.client=client
        self.embedding_deployment = embedding_deployment


    @classmethod
    async def create(cls):
        api_key = await get_secret('azureOpenAIKey')
        api_base = f"https://{os.getenv('AZURE_OPENAI_SERVICE_NAME')}.openai.azure.com/"
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        embedding_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
        client=AsyncAzureOpenAI(api_key=api_key, azure_endpoint=api_base, api_version=api_version,azure_deployment=embedding_deployment)
        
        return cls(embedding_deployment,client)
    
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
    async def embed_content(self, text, clean_text=True, use_single_precision=True):
        embedding_precision = 9 if use_single_precision else 18
        if clean_text:
            text = self.clean_text(text)
    
            response = await self.client.embeddings.create(input=text,model=self.embedding_deployment)
            embedding = [round(x, embedding_precision) for x in response.data[0].embedding] # type: ignore
        return embedding
    
    def extract_retry_seconds(self, error_message):
        match = re.search(r'retry after (\d+)', error_message)
        if match:
            return int(match.group(1))
        else:
            return 60 # default to 60 seconds in case it can't be extracted

    @retry(reraise=True, stop=stop_after_attempt(6))
    async def embed_content(self, text, clean_text=True, use_single_precision=True):
        embedding_precision = 9 if use_single_precision else 18
        if clean_text:
            text = self.clean_text(text)        
        try:
            response = await self.client.embeddings.create(input=text,model=self.embedding_deployment)
            embedding = [round(x, embedding_precision) for x in response.data[0].embedding] # type: ignore
            return embedding            
        except openai.RateLimitError as e:
            error_message = str(e)
            seconds = self.extract_retry_seconds(error_message) * 2
            logging.warning(f"Embeddings model deployment rate limit exceeded, retrying in {seconds} seconds...")
            await asyncio.sleep(seconds)
            raise e # to make tenacity retry