import logging
import requests
from abc import ABC
from azure.core.credentials import TokenCredential
from rich.progress import Progress
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

logger = logging.getLogger("scripts")


class MediaDescriber(ABC):

    def describe_image(self, image_bytes) -> str:
        raise NotImplementedError  # pragma: no cover


class ContentUnderstandingDescriber:
    CU_API_VERSION = "2024-12-01-preview"

    analyzer_schema = {
        "analyzerId": "image_analyzer",
        "name": "Image understanding",
        "description": "Extract detailed structured information from images extracted from documents.",
        "baseAnalyzerId": "prebuilt-image",
        "scenario": "image",
        "config": {"returnDetails": False},
        "fieldSchema": {
            "name": "ImageInformation",
            "descriptions": "Description of image.",
            "fields": {
                "Description": {
                    "type": "string",
                    "description": "Description of the image. If the image has a title, start with the title. Include a 2-sentence summary. If the image is a chart, diagram, or table, include the underlying data in an HTML table tag, with accurate numbers. If the image is a chart, describe any axis or legends. The only allowed HTML tags are the table/thead/tr/td/tbody tags.",
                },
            },
        },
    }

    def __init__(self, endpoint: str, credential: TokenCredential):
        self.endpoint = endpoint
        self.credential = credential

    def poll_api(self, poll_url, headers):
        @retry(stop=stop_after_attempt(60), wait=wait_fixed(2), retry=retry_if_exception_type(ValueError))
        def poll():
            response = requests.get(poll_url, headers=headers)
            response.raise_for_status()
            response_json = response.json()
            if response_json["status"] == "Failed":
                raise Exception("Failed")
            if response_json["status"] == "Running":
                raise ValueError("Running")
            return response_json

        return poll()

    def create_analyzer(self):
        logger.info("Creating analyzer '%s'...", self.analyzer_schema["analyzerId"])

        token = self.credential.get_token("https://cognitiveservices.azure.com/.default")
        headers = {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}
        params = {"api-version": self.CU_API_VERSION}
        analyzer_id = self.analyzer_schema["analyzerId"]
        cu_endpoint = f"{self.endpoint}/contentunderstanding/analyzers/{analyzer_id}"

        response = requests.put(
            url=cu_endpoint,
            params=params,
            headers=headers,
            json=self.analyzer_schema
        )

        if response.status_code == 409:
            logger.info("Analyzer '%s' already exists.", analyzer_id)
            return
        elif response.status_code != 201:
            raise Exception("Error creating analyzer", response.text)
        
        poll_url = response.headers.get("Operation-Location")
        with Progress() as progress:
            progress.add_task("Creating analyzer...", total=None, start=False)
            self.poll_api(poll_url, headers)

    def describe_image(self, image_bytes: bytes) -> str:
        logger.info("Sending image to Azure Content Understanding service...")
        token = self.credential.get_token("https://cognitiveservices.azure.com/.default")
        headers = {"Authorization": "Bearer " + token.token}
        params = {"api-version": self.CU_API_VERSION}
        analyzer_name = self.analyzer_schema["analyzerId"]

        response = requests.post(
            url=f"{self.endpoint}/contentunderstanding/analyzers/{analyzer_name}:analyze",
            params=params,
            headers=headers,
            data=image_bytes,
        )
        response.raise_for_status()
        poll_url = response.headers["Operation-Location"]

        with Progress() as progress:
            progress.add_task("Processing...", total=None, start=False)
            results = self.poll_api(poll_url, headers)

        fields = results["result"]["contents"][0]["fields"]
        return fields["Description"]["valueString"]