import logging
import os
import tiktoken
import time
from openai import AzureOpenAI, RateLimitError
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

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
        self.max_retries = 10 # Maximum number of retries for rate limit errors
        self.max_embeddings_model_input_tokens = 8192
        self.max_gpt_model_input_tokens = 128000 # this is gpt4o max input, if using gpt35turbo use 16385

        self.document_filename = f"[{document_filename}]" if document_filename else ""
        self.openai_service_name = os.getenv('AZURE_OPENAI_SERVICE_NAME')
        self.openai_api_base = f"https://{self.openai_service_name}.openai.azure.com"
        self.openai_api_version = os.getenv('AZURE_OPENAI_API_VERSION')
        self.openai_embeddings_deployment = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT')
        self.openai_gpt_deployment = os.getenv('AZURE_OPENAI_CHATGPT_DEPLOYMENT')

        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )

        self.client = AzureOpenAI(
            api_version=self.openai_api_version,
            azure_endpoint=self.openai_api_base,
            azure_ad_token_provider=token_provider,
            max_retries=self.max_retries
        )

    def get_completion(self, prompt, max_tokens=800, retry_after=True):
        one_liner_prompt = prompt.replace('\n', ' ')
        logging.info(f"[aoai]{self.document_filename} Getting completion for prompt: {one_liner_prompt[:100]}")

        # truncate prompt if needed
        prompt = self._truncate_input(prompt, self.max_gpt_model_input_tokens)

        try:
            input_messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": f"{prompt}"}
            ]

            response = self.client.chat.completions.create(
                messages=input_messages,
                model=self.openai_gpt_deployment,
                temperature=0.7,
                top_p=0.95,
                max_tokens=max_tokens
            )

            completion = response.choices[0].message.content

            return completion

        except RateLimitError as e:
            retry_after_ms = e.response.headers.get('retry-after-ms')
            if retry_after_ms:
                retry_after_ms = int(retry_after_ms)
                logging.info(f"[aoai]{self.document_filename} get_completion: Reached rate limit, retrying after {retry_after_ms} ms")
                time.sleep(retry_after_ms / 1000)
                return self.get_completion(self, prompt, retry_after=False)
            else:
                logging.error(f"[aoai]{self.document_filename} get_completion: Rate limit error occurred, no 'retry-after-ms' provided: {e}")
                raise

        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} get_completion: An unexpected error occurred: {e}")
            raise

    def get_embeddings(self, text, retry_after=True):
        one_liner_text = text.replace('\n', ' ')
        logging.info(f"[aoai]{self.document_filename} Getting embeddings for text: {one_liner_text[:100]}")        
        
        # truncate in case it is larger than the maximum input tokens
        text = self._truncate_input(text, self.max_embeddings_model_input_tokens)

        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.openai_embeddings_deployment
            )
            embeddings = response.data[0].embedding
            return embeddings
        
        except RateLimitError as e:
            retry_after_ms = e.response.headers.get('retry-after-ms')
            if retry_after_ms:
                retry_after_ms = int(retry_after_ms)
                logging.info(f"[aoai]{self.document_filename} get_completion: Reached rate limit, retrying after {retry_after_ms} ms")
                time.sleep(retry_after_ms / 1000)
                return self.get_completion(self, prompt, retry_after=False)
            else:
                logging.error(f"[aoai]{self.document_filename} get_completion: Rate limit error occurred, no 'retry-after-ms' provided: {e}")
                raise

        except Exception as e:
            logging.error(f"[aoai]{self.document_filename} get_embedding: An unexpected error occurred: {e}")
            raise

    def _truncate_input(self, text, max_tokens):
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

class GptTokenEstimator():
    GPT2_TOKENIZER = tiktoken.get_encoding("gpt2")

    def estimate_tokens(self, text: str) -> int:
        return len(self.GPT2_TOKENIZER.encode(text))
