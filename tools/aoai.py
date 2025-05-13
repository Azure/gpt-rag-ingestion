# tools/aoai.py

import logging
import os
import time
import tiktoken
from openai import RateLimitError
from azure.identity import (
    ManagedIdentityCredential,
    AzureCliCredential,
    ChainedTokenCredential
)
from azure.core.exceptions import ClientAuthenticationError
from azure.ai.projects import AIProjectClient


class AzureOpenAIClient:
    """
    Routes all OpenAI calls through your API Management proxy,
    by first retrieving the APIM endpoint & key from your AI Foundry project.
    """
    def __init__(self, document_filename: str = ""):
        # Optional tag for log traceability
        self.document_filename = f"[{document_filename}]" if document_filename else ""

        # Maximum retries for rate-limit errors before using `retry-after`
        self.max_retries = int(os.getenv("MAX_RETRIES", 10))

        # Token limits
        self.max_embeddings_model_input_tokens = 8192
        self.max_gpt_model_input_tokens = 128_000

        # --- 1) load configuration from environment ---
        project_conn_str = os.getenv("AI_FOUNDRY_PROJECT_CONNECTION_STRING")
        if not project_conn_str:
            raise EnvironmentError(
                "Environment variable AI_FOUNDRY_PROJECT_CONNECTION_STRING is not set"
            )

        # Name of the APIM-backed OpenAI connection in AI Foundry
        self.connection_name = os.getenv("OPENAI_CONNECTION_NAME", "openai-apim-conn")

        # Deployment names for embeddings & chat models
        self.openai_api_version           = os.getenv("OPENAI_API_VERSION")
        self.openai_embeddings_deployment = os.getenv("OPENAI_EMBEDDING_DEPLOYMENT")
        self.openai_chat_deployment       = os.getenv("OPENAI_CHAT_DEPLOYMENT")

        for var, val in {
            "OPENAI_API_VERSION": self.openai_api_version,
            "OPENAI_EMBEDDING_DEPLOYMENT": self.openai_embeddings_deployment,
            "OPENAI_CHAT_DEPLOYMENT": self.openai_chat_deployment
        }.items():
            if not val:
                logging.warning(f"[aoai]{self.document_filename} {var} is not set")

        # --- 2) build a credential chain ---
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug(f"[aoai]{self.document_filename} Credential chain initialized")
        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} Credential init failed: {e}")
            raise

        # --- 3) instantiate AIProjectClient & retrieve APIM-backed OpenAI client ---
        try:
            project = AIProjectClient.from_connection_string(
                conn_str=project_conn_str,
                credential=self.credential
            )
            logging.debug(f"[aoai]{self.document_filename} Connected to AI Foundry project")

            self.client = project.inference.get_azure_openai_client(
                api_version=self.openai_api_version,
                connection_name=self.connection_name
            )
            logging.debug(
                f"[aoai]{self.document_filename} Acquired OpenAI client "
                f"via APIM connection '{self.connection_name}'"
            )
        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} Failed to get OpenAI client: {e}")
            raise

    def get_completion(self, prompt: str, image_base64: str = None,
                       max_tokens: int = 800, retry_after: bool = True) -> str:
        # Truncate and build messages...
        prompt = self._truncate_input(prompt, self.max_gpt_model_input_tokens)
        messages = [{"role": "system", "content": "You are a helpful assistant."}]
        if image_base64:
            messages.append({
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]
            })
        else:
            messages.append({"role": "user", "content": prompt})

        try:
            resp = self.client.chat.completions.create(
                model=self.openai_chat_deployment,
                messages=messages,
                max_tokens=max_tokens
            )
            return resp.choices[0].message.content

        except RateLimitError as e:
            if not retry_after:
                logging.error(f"[aoai]{self.document_filename} Rate limit exceeded: {e}")
                raise

            wait_ms = e.response.headers.get("retry-after-ms")
            if wait_ms:
                logging.info(f"[aoai]{self.document_filename} Retrying after {wait_ms}ms")
                time.sleep(int(wait_ms) / 1000)
                return self.get_completion(prompt, image_base64, max_tokens, retry_after=False)
            raise

        except ClientAuthenticationError as e:
            logging.error(f"[aoai]{self.document_filename} Authentication failed: {e}")
            raise

    def get_embeddings(self, text: str, retry_after: bool = True) -> list:
        text = self._truncate_input(text, self.max_embeddings_model_input_tokens)
        try:
            resp = self.client.embeddings.create(
                model=self.openai_embeddings_deployment,
                input=text
            )
            return resp.data[0].embedding

        except RateLimitError as e:
            if not retry_after:
                logging.error(f"[aoai]{self.document_filename} Embedding rate limit: {e}")
                raise

            wait_ms = e.response.headers.get("retry-after-ms")
            if wait_ms:
                logging.info(f"[aoai]{self.document_filename} Retrying embeddings after {wait_ms}ms")
                time.sleep(int(wait_ms) / 1000)
                return self.get_embeddings(text, retry_after=False)
            raise

        except ClientAuthenticationError as e:
            logging.error(f"[aoai]{self.document_filename} Authentication failed: {e}")
            raise

    def _truncate_input(self, text: str, max_tokens: int) -> str:
        estimator = GptTokenEstimator()
        count = estimator.estimate_tokens(text)
        if count <= max_tokens:
            return text

        logging.info(f"[aoai]{self.document_filename} Truncating from {count} to {max_tokens} tokens")
        step = 1
        while estimator.estimate_tokens(text) > max_tokens:
            text = text[:-step]
            step = min(step * 2, 100)
        return text


class GptTokenEstimator:
    """
    Estimates token count using the GPT-2 tokenizer.
    """
    GPT2_TOKENIZER = tiktoken.get_encoding("gpt2")

    def estimate_tokens(self, text: str) -> int:
        return len(self.GPT2_TOKENIZER.encode(text))
