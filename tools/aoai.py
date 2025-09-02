# AzureOpenAIClient.py

import logging
import os
import tiktoken
import time
from openai import AzureOpenAI, RateLimitError
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential, get_bearer_token_provider
from azure.core.exceptions import ClientAuthenticationError

class AzureOpenAIClient:
    """
    AzureOpenAIClient uses the OpenAI SDK's built-in retry mechanism with exponential backoff.
    The number of retries is controlled by the MAX_RETRIES environment variable.
    Delays between retries start at 0.5 seconds, doubling up to 8 seconds.
    If a rate limit error occurs after retries, the client will retry once more after the retry-after-ms header duration (if the header is present).
    """
    def __init__(self, document_filename=""):
        """
        Initializes the AzureOpenAI client.

        Parameters:
        document_filename (str, optional): Additional attribute for improved log traceability.
        """        
        self.max_retries = 10  # Maximum number of retries for rate limit errors
        self.max_embeddings_model_input_tokens = 8192
        self.max_gpt_model_input_tokens = 128000  # this is gpt4o max input, if using gpt35turbo use 16385

        self.document_filename = f"[{document_filename}]" if document_filename else ""
        self.openai_service_name = os.getenv('AZURE_OPENAI_SERVICE_NAME')
        self.openai_api_base = f"https://{self.openai_service_name}.openai.azure.com"
        self.openai_api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        self.openai_embeddings_deployment = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT')
        
        # Log a warning if any environment variable is empty
        env_vars = {
            'AZURE_OPENAI_SERVICE_NAME': self.openai_service_name,
            'AZURE_OPENAI_API_VERSION': self.openai_api_version,
            'AZURE_OPENAI_EMBEDDING_DEPLOYMENT': self.openai_embeddings_deployment,
        }
        
        for var_name, var_value in env_vars.items():
            if not var_value:
                logging.warning(f'[aoai]{self.document_filename} Environment variable {var_name} is not set.')

        # Initialize the ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug(f"[aoai]{self.document_filename} Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} Failed to initialize ChainedTokenCredential: {e}")
            raise

        # Initialize the bearer token provider
        try:
            self.token_provider = get_bearer_token_provider(
                self.credential, 
                "https://cognitiveservices.azure.com/.default"
            )
            logging.debug(f"[aoai]{self.document_filename} Initialized bearer token provider.")
        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} Failed to initialize bearer token provider: {e}")
            raise

        # Initialize the AzureOpenAI client
        try:
            self.client = AzureOpenAI(
                api_version=self.openai_api_version,
                azure_endpoint=self.openai_api_base,
                azure_ad_token_provider=self.token_provider,
                max_retries=self.max_retries
            )
            logging.debug(f"[aoai]{self.document_filename} Initialized AzureOpenAI client.")
        except ClientAuthenticationError as e:
            logging.error(f"[aoai]{self.document_filename} Authentication failed during AzureOpenAI client initialization: {e}")
            raise
        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} Failed to initialize AzureOpenAI client: {e}")
            raise


    def get_embeddings(self, text, retry_after=True):
        """
        Generates embeddings for the given text using the Azure OpenAI service.

        Args:
            text (str): The input text to generate embeddings for.
            retry_after (bool, optional): Flag to determine if the method should retry after rate limiting. Defaults to True.

        Returns:
            list: The generated embeddings.
        """
        one_liner_text = text.replace('\n', ' ')
        logging.debug(f"[aoai]{self.document_filename} Getting embeddings for text: {one_liner_text[:100]}")        
        
        # Truncate in case it is larger than the maximum input tokens
        text = self._truncate_input(text, self.max_embeddings_model_input_tokens)

        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.openai_embeddings_deployment
            )
            embeddings = response.data[0].embedding
            logging.debug(f"[aoai]{self.document_filename} Embeddings received successfully.")
            return embeddings
        
        except RateLimitError as e:
            if not retry_after:
                logging.error(f"[aoai]{self.document_filename} get_embeddings: Rate limit error occurred after retries: {e}")
                raise

            retry_after_ms = e.response.headers.get('retry-after-ms')
            if retry_after_ms:
                retry_after_ms = int(retry_after_ms)
                logging.info(f"[aoai]{self.document_filename} get_embeddings: Reached rate limit, retrying after {retry_after_ms} ms")
                time.sleep(retry_after_ms / 1000)
                return self.get_embeddings(text, retry_after=False)
            else:
                logging.error(f"[aoai]{self.document_filename} get_embeddings: Rate limit error occurred, no 'retry-after-ms' provided: {e}")
                raise

        except ClientAuthenticationError as e:
            logging.error(f"[aoai]{self.document_filename} get_embeddings: Authentication failed: {e}")
            raise

        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} get_embeddings: An unexpected error occurred: {e}")
            raise

    def _truncate_input(self, text, max_tokens):
        """
        Truncates the input text to ensure it does not exceed the maximum number of tokens.

        Args:
            text (str): The input text to truncate.
            max_tokens (int): The maximum number of tokens allowed.

        Returns:
            str: The truncated text.
        """
        input_tokens = GptTokenEstimator().estimate_tokens(text)
        if input_tokens > max_tokens:
            logging.info(f"[aoai]{self.document_filename} Input size {input_tokens} exceeded maximum token limit {max_tokens}, truncating...")
            step_size = 1  # Initial step size
            iteration = 0  # Iteration counter

            while GptTokenEstimator().estimate_tokens(text) > max_tokens:
                text = text[:-step_size]
                iteration += 1

                # Increase step size exponentially every 5 iterations
                if iteration % 5 == 0:
                    step_size = min(step_size * 2, 100)

        return text    

class GptTokenEstimator:
    GPT2_TOKENIZER = tiktoken.get_encoding("gpt2")

    def estimate_tokens(self, text: str) -> int:
        """
        Estimates the number of tokens in the given text using the GPT-2 tokenizer.

        Args:
            text (str): The input text.

        Returns:
            int: The estimated number of tokens.
        """
        return len(self.GPT2_TOKENIZER.encode(text))