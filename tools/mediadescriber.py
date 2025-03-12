import logging
import os
import requests
from abc import ABC
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

    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key

    def poll_api(self, poll_url, headers):
        @retry(
            stop=stop_after_attempt(60),
            wait=wait_fixed(2),
            retry=retry_if_exception_type(ValueError),
        )
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

        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key,
            "Content-Type": "application/json",
        }
        params = {"api-version": self.CU_API_VERSION}
        analyzer_id = self.analyzer_schema["analyzerId"]
        cu_endpoint = f"{self.endpoint}/contentunderstanding/analyzers/{analyzer_id}"

        response = requests.put(
            url=cu_endpoint, params=params, headers=headers, json=self.analyzer_schema
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

        # Map of common image file extensions to MIME types
        content_types = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".bmp": "image/bmp",
            ".tiff": "image/tiff",
        }

        # Default to jpeg if format can't be determined
        content_type = "image/jpeg"

        # Try to detect format from image bytes
        if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            content_type = "image/png"
        elif image_bytes.startswith(b"\xff\xd8"):
            content_type = "image/jpeg"

        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key,
            "Content-Type": content_type,
        }
        params = {"api-version": self.CU_API_VERSION}
        analyzer_name = self.analyzer_schema["analyzerId"]

        url = f"{self.endpoint.rstrip('/')}/contentunderstanding/analyzers/{analyzer_name}:analyze"

        response = requests.post(
            url=url,
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


if __name__ == "__main__":
    describer = ContentUnderstandingDescriber(
        endpoint=os.getenv("AZ_COMPUTER_VISION_ENDPOINT"),
        api_key=os.getenv("AZ_COMPUTER_VISION_KEY"),
    )
    describer.create_analyzer()

    # Provide a link with sas token of the image here 
    image_url = ""
    try:
        # Download the image from URL with proper headers
        logger.info(f"Downloading image from: {image_url}")
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "image/jpeg,image/png,image/*",
        }
        response = requests.get(image_url, stream=True, headers=headers)
        response.raise_for_status()

        # Check content type from response
        content_type = response.headers.get("Content-Type", "")
        logger.info(f"Image Content-Type from server: {content_type}")

        # Get image data and show first few bytes for debugging
        image_bytes = response.content
        logger.info(f"First few bytes of image: {image_bytes[:20]}")
        if not image_bytes:
            raise ValueError("No image data received")
        logger.info(f"Downloaded image size: {len(image_bytes)} bytes")

        # Process the image
        try:
            result = describer.describe_image(image_bytes)
            print("Image description:", result)
        except requests.exceptions.HTTPError as e:
            error_text = e.response.text if hasattr(e.response, "text") else str(e)
            print(f"Azure API error: {error_text}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading image: {e}")
    except ValueError as e:
        print(f"Image validation error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    # test with local file
    # with open("/path/to/local/image.png", "rb") as f:
    #     image_bytes = f.read()
    # print(describer.describe_image(image_bytes))
