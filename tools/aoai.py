import logging
import os
import tiktoken
import time
from openai import AzureOpenAI, RateLimitError
from azure.identity import DefaultAzureCredential, get_bearer_token_provider

MAX_RETRIES = 3 # Maximum number of retries for rate limit errors
MAX_EMBEDDINGS_MODEL_INPUT_TOKENS = 8192
MAX_GPT_MODEL_INPUT_TOKENS = 128000 # this is gpt4o max input, if using gpt35turbo use 16385

class AzureOpenAIClient:
    """
    AzureOpenAIClient uses the OpenAI SDK's built-in retry mechanism with exponential backoff.
    The number of retries is controlled by the MAX_RETRIES environment variable.
    Delays between retries start at 0.5 seconds, doubling up to 8 seconds.
    If a rate limit error occurs after retries, the client will retry once more after the retry-after-ms header duration.
    """
    def __init__(self):
        self.openai_service_name = os.getenv('AZURE_OPENAI_SERVICE_NAME')
        self.openai_api_base = f"https://{self.openai_service_name}.openai.azure.com"
        self.openai_api_version = os.getenv('AZURE_OPENAI_API_VERSION')

        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )

        self.client = AzureOpenAI(
            api_version=self.openai_api_version,
            azure_endpoint=self.openai_api_base,
            azure_ad_token_provider=token_provider,
            max_retries=MAX_RETRIES
        )

    def get_completion(self, prompt, max_tokens=800, retry_after=True):
        one_liner_prompt = prompt.replace('\n', ' ')
        logging.info(f"[aoai] Getting completion for prompt: {one_liner_prompt[:100]}")
        self.openai_deployment = os.getenv('AZURE_OPENAI_CHATGPT_DEPLOYMENT')

        # truncate prompt if needed
        prompt = self._truncate_input(prompt, MAX_GPT_MODEL_INPUT_TOKENS)

        try:
            input_messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": f"{prompt}"}
            ]

            response = self.client.chat.completions.create(
                messages=input_messages,
                model=self.openai_deployment,
                temperature=0.7,
                top_p=0.95,
                max_tokens=max_tokens
            )

            completion = response.choices[0].message.content

            return completion

        except RateLimitError as e:
            logging.error(f"[aoai] get_completion: Rate limit error occurred: {e}")
            raise

        except RateLimitError as e:
            if retry_after:
                retry_after_ms = int(e.response.headers['retry-after-ms'])
                logging.info(f"[aoai] get_completion: Reached rate limit retrying after {retry_after_ms} ms")
                time.sleep(e.retry_after / 1000)
                return self.get_completion(self, prompt, retry_after=False)
            else:
                logging.error(f"[aoai] get_completion: Rate limit error occurred: {e}")
                raise

        except Exception as e:
            logging.error(f"[aoai] get_completion: An unexpected error occurred: {e}")
            raise

    def get_embeddings(self, text, retry_after=True):
        """
        Retrieves embeddings for a given text using the Azure OpenAI service.

        This method obtains embeddings for a given input text by interacting with the Azure OpenAI service.
        It first estimates the number of tokens in the input text and, if the text exceeds the maximum allowed
        token limit for the embedding model, it summarizes the text to fit within the limit. The method then
        attempts to create the embeddings using the Azure OpenAI deployment specified in the environment variables.

        Args:
            text (str): The input text for which embeddings are to be generated.
            retry_after (bool, optional): If True (default), the method will retry obtaining embeddings after a 
                                        specified wait time if a rate limit error occurs. If False, it will raise 
                                        the rate limit error without retrying.

        Returns:
            list: A list of embedding vectors representing the input text.

        Raises:
            RateLimitError: If a rate limit error occurs and `retry_after` is set to False, or if the error persists after retrying.
            Exception: If any other unexpected error occurs during the embedding generation process.

        Logging:
            Logs the process of obtaining embeddings, including summarizing the text if it exceeds the token limit, 
            handling rate limit errors, and any unexpected errors.
        """

        one_liner_text = text.replace('\n', ' ')
        logging.info(f"[aoai] Getting embeddings for a text: {one_liner_text[:100]}")
        self.openai_deployment = os.getenv('AZURE_OPENAI_EMBEDDING_DEPLOYMENT')

        # summarize in case it is larger than the maximum input tokens
        num_tokens = GptTokenEstimator().estimate_tokens(text)
        if num_tokens > MAX_EMBEDDINGS_MODEL_INPUT_TOKENS:
                prompt = f"Rewrite the text to be coherent and meaningful, reducing it to {MAX_EMBEDDINGS_MODEL_INPUT_TOKENS} tokens: {text}"
                text = self.get_completion(prompt)
                logging.warning(f"[aoai] get_embeddings: rewriting text to fit in {MAX_EMBEDDINGS_MODEL_INPUT_TOKENS} tokens")

        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.openai_deployment
            )
            embeddings = response.data[0].embedding
            return embeddings
        
        except RateLimitError as e:
            if retry_after:
                retry_after_ms = int(e.response.headers['retry-after-ms'])
                logging.info(f"[aoai] get_embedding: Reached rate limit retrying after {retry_after_ms} ms")
                time.sleep(e.retry_after / 1000)
                return self.get_embeddings(self, text, retry_after=False)
            else:
                logging.error(f"[aoai] get_embedding: Rate limit error occurred: {e}")
                raise

        except Exception as e:
            logging.error(f"[aoai] get_embedding: An unexpected error occurred: {e}")
            raise

    def _truncate_input(self, text, max_tokens):
        """
        Truncates the input to maximum number of tokens.
        """
        input_tokens = GptTokenEstimator().estimate_tokens(text)
        if input_tokens > max_tokens:
            logging.warning(f"[aoai] Input size {input_tokens} exceeded maximum token limit {max_tokens}, truncating...")
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
    