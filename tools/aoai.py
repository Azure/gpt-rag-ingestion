# tools/aoai.py

import time
import logging
import random
import re

import openai
import tiktoken
from azure.identity import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential, get_bearer_token_provider
from dependencies import get_config

app_config_client = get_config()

class AzureOpenAIClient:
    def __init__(self, document_filename: str = ""):
        # Optional tag for log traceability
        self.document_filename = f"[{document_filename}]" if document_filename else ""

        # Load configuration from environment
        self.endpoint             = app_config_client.get("AI_FOUNDRY_ACCOUNT_ENDPOINT")        # e.g. "https://<your-resource>.openai.azure.com/"
        self.api_version          = app_config_client.get("OPENAI_API_VERSION", "2024-10-21")
        self.chat_deployment      = app_config_client.get("CHAT_DEPLOYMENT_NAME")         # deployment name in Azure OpenAI Studio
        self.embedding_deployment = app_config_client.get("EMBEDDING_DEPLOYMENT_NAME")

        # Warn if any required var is missing
        for var, val in {
            "AI_FOUNDRY_ACCOUNT_ENDPOINT": self.endpoint,
            "OPENAI_API_VERSION":    self.api_version,
            "CHAT_DEPLOYMENT_NAME":  self.chat_deployment,
            "EMBEDDING_DEPLOYMENT_NAME": self.embedding_deployment
        }.items():
            if not val:
                logging.warning(f"[aoai]{self.document_filename} {var} is not set")

        # Token limits
        self.max_gpt_tokens       = 128_000
        self.max_embed_tokens     =   8_192

        # Retry controls (wrapper-level). We intentionally keep these configurable because
        # large ingestions (e.g., spreadsheets chunked by row) can hit TPM limits.
        self.retry_max_attempts   = int(app_config_client.get("OPENAI_RETRY_MAX_ATTEMPTS", "20"))
        self.retry_base_seconds   = float(app_config_client.get("OPENAI_RETRY_BASE_SECONDS", "1"))
        self.retry_max_seconds    = float(app_config_client.get("OPENAI_RETRY_MAX_SECONDS", "60"))
        self.retry_jitter_seconds = float(app_config_client.get("OPENAI_RETRY_JITTER_SECONDS", "0.5"))

        # OpenAI SDK internal retries (disable by default to avoid multiplying waits)
        self.sdk_max_retries      = int(app_config_client.get("OPENAI_SDK_MAX_RETRIES", "0"))

        # Build token provider with preferred order: Azure CLI first, then Managed Identity (optional client_id)
        client_id = app_config_client.get("AZURE_CLIENT_ID", None, allow_none=True) or None
        token_provider = get_bearer_token_provider(
            ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id)
            ),
            "https://cognitiveservices.azure.com/.default"
        )
        logging.debug(f"[aoai]{self.document_filename} Obtained bearer token provider")

        # Instantiate Azure OpenAI client with AAD token auth
        self.client = openai.AzureOpenAI(
            azure_endpoint          = self.endpoint,
            api_version             = self.api_version,
            azure_ad_token_provider = token_provider,
            max_retries             = self.sdk_max_retries,
        )
        logging.debug(f"[aoai]{self.document_filename} AzureOpenAI client initialized with AAD token provider")
        # Token estimator for truncation
        self.token_estimator = GptTokenEstimator()

    def _extract_retry_after_seconds(self, exc: Exception) -> float | None:
        """Best-effort parsing of Retry-After from SDK exception headers or message."""
        headers = None
        # openai exceptions sometimes expose .headers, sometimes .response.headers
        try:
            headers = getattr(exc, "headers", None)
        except Exception:
            headers = None

        if not headers:
            try:
                resp = getattr(exc, "response", None)
                headers = getattr(resp, "headers", None) if resp is not None else None
            except Exception:
                headers = None

        if headers:
            # Case-insensitive lookup
            for key in ("retry-after", "Retry-After", "RETRY-AFTER"):
                if key in headers:
                    try:
                        return float(headers[key])
                    except Exception:
                        pass

        # Sometimes the service embeds a human-readable hint in the error message
        msg = str(exc)
        match = re.search(r"retry after\s+(\d+(?:\.\d+)?)\s+seconds", msg, flags=re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return None
        return None

    def _sleep_for_retry(self, *, attempt: int, retry_after: float | None, op_name: str) -> None:
        # Exponential backoff with cap; if server gives Retry-After we respect it (minimum).
        exp = min(self.retry_max_seconds, self.retry_base_seconds * (2 ** max(attempt, 0)))
        wait = max(exp, retry_after or 0)
        if self.retry_jitter_seconds > 0:
            wait += random.uniform(0, self.retry_jitter_seconds)
        logging.info(
            f"[aoai]{self.document_filename} {op_name} rate-limited; sleeping {wait:.2f}s (attempt {attempt + 1}/{self.retry_max_attempts})"
        )
        time.sleep(wait)

    def get_completion(
        self,
        prompt: str,
        max_tokens: int = 800,
        retry_after: bool = True
    ) -> str:
        # Truncate prompt if over token limit
        prompt_trunc = self._truncate_input(prompt, self.max_gpt_tokens)

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user",   "content": prompt_trunc}
        ]

        attempt = 0
        while True:
            try:
                resp = self.client.chat.completions.create(
                    model      = self.chat_deployment,
                    messages   = messages,
                    max_tokens = max_tokens
                )
                return resp.choices[0].message.content

            except openai.RateLimitError as e:
                if not retry_after or attempt >= self.retry_max_attempts:
                    logging.error(f"[aoai]{self.document_filename} RateLimitError in get_completion: {e}")
                    raise
                ra = self._extract_retry_after_seconds(e)
                self._sleep_for_retry(attempt=attempt, retry_after=ra, op_name="chat.completions")
                attempt += 1
                continue

            except openai.APIStatusError as e:
                # Some SDK versions may surface 429 as APIStatusError.
                status = getattr(e, "status_code", None)
                if retry_after and status == 429 and attempt < self.retry_max_attempts:
                    ra = self._extract_retry_after_seconds(e)
                    self._sleep_for_retry(attempt=attempt, retry_after=ra, op_name="chat.completions")
                    attempt += 1
                    continue
                logging.error(f"[aoai]{self.document_filename} APIStatusError in get_completion: {e}")
                raise

            except openai.OpenAIError as e:
                logging.error(f"[aoai]{self.document_filename} OpenAIError in get_completion: {e}")
                raise

            except Exception as e:
                logging.error(f"[aoai]{self.document_filename} Unexpected error in get_completion: {e}")
                raise

    def get_embeddings(self, text: str, retry_after: bool = True) -> list:
        text_trunc = self._truncate_input(text, self.max_embed_tokens)

        attempt = 0
        while True:
            try:
                resp = self.client.embeddings.create(
                    model = self.embedding_deployment,
                    input = text_trunc
                )
                return resp.data[0].embedding

            except openai.RateLimitError as e:
                if not retry_after or attempt >= self.retry_max_attempts:
                    logging.error(f"[aoai]{self.document_filename} RateLimitError in get_embeddings: {e}")
                    raise
                ra = self._extract_retry_after_seconds(e)
                self._sleep_for_retry(attempt=attempt, retry_after=ra, op_name="embeddings")
                attempt += 1
                continue

            except openai.APIStatusError as e:
                status = getattr(e, "status_code", None)
                if retry_after and status == 429 and attempt < self.retry_max_attempts:
                    ra = self._extract_retry_after_seconds(e)
                    self._sleep_for_retry(attempt=attempt, retry_after=ra, op_name="embeddings")
                    attempt += 1
                    continue
                logging.error(f"[aoai]{self.document_filename} APIStatusError in get_embeddings: {e}")
                raise

            except openai.OpenAIError as e:
                logging.error(f"[aoai]{self.document_filename} OpenAIError in get_embeddings: {e}")
                raise

            except Exception as e:
                logging.error(f"[aoai]{self.document_filename} Unexpected error in get_embeddings: {e}")
                raise

    def _truncate_input(self, text: str, max_tokens: int) -> str:
        count = self.token_estimator.estimate_tokens(text)
        if count <= max_tokens:
            return text

        logging.info(f"[aoai]{self.document_filename} Truncating input from {count} to {max_tokens} tokens")
        step = 1
        truncated = text
        while self.token_estimator.estimate_tokens(truncated) > max_tokens and truncated:
            truncated = truncated[:-step]
            step = min(step * 2, 100)
        return truncated

class GptTokenEstimator:
    """
    Estimates token counts for a specified OpenAI model using tiktoken.
    """
    def __init__(self, model_name: str = "text-embedding-3-large"):
        # encoding_for_model picks the right BPE for the model:
        self.encoding = tiktoken.encoding_for_model(model_name)

    def estimate_tokens(self, text: str) -> int:
        # Exact count of BPE tokens, no fallbacks needed:
        return len(self.encoding.encode(text))