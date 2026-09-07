import logging

from azure.identity.aio import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.storage.blob.aio import ContainerClient
from tools import AISearchClient

from dependencies import get_config

app_config_client = get_config()


class ImagesDeletedFilesPurger:
    """
    Identifies images in the 'documents-images' container that 
    are no longer referenced in the Azure AI Search index (via 'relatedImages').
    If an image is not present in any document's 'relatedImages', it is deleted.
    """

    def __init__(self):
        """
        Initialize with environment variables and any other configuration.
        """
        self.index_name = app_config_client.get("AZURE_SEARCH_INDEX_NAME", "ragindex")
        self.container_name = app_config_client.get("STORAGE_CONTAINER_IMAGES", "documents-images")
        self.storage_account_name = app_config_client.get("STORAGE_ACCOUNT_NAME")
        self.blob_base_url = f"https://{self.storage_account_name}.blob.core.windows.net"

        # Warn if some env vars are missing
        if not all([self.index_name, self.container_name, self.storage_account_name]):
            logging.warning("[images_deleted_files_purger] Missing or incomplete environment variables.")

        # We'll create a single AISearchClient for searching the index
        self.ai_search = AISearchClient()

    async def run(self) -> None:
        """
        Executes the purge process (asynchronously):
            1) Gathers all referenced images from the 'relatedImages' field in the search index.
            2) Lists all blobs in the container.
            3) Deletes those not referenced in step 1.
        """
        logging.info("[images_deleted_files_purger] Starting images from deleted files purging run()")

        try:
            referenced_images = await self._get_all_referenced_images()
            await self._purge_unreferenced_images(referenced_images)
        finally:
            await self.ai_search.close()

        logging.info("[images_deleted_files_purger] Completed run().")

    async def _get_all_referenced_images(self) -> set[str]:
        """
        Uses AISearchClient to retrieve 'relatedImages' from all docs in the index
        and returns a set of URLs.
        """
        logging.info("[images_deleted_files_purger] Retrieving referenced images from AI Search index...")

        referenced_images: set[str] = set()
        async for doc in self.ai_search.iter_documents(
            index_name=self.index_name, select_fields=["relatedImages"],
        ):
            images = doc.get("relatedImages")
            if images is None:
                continue
            if not isinstance(images, list):
                raise ValueError("Invalid relatedImages collection; image purge stopped.")
            for img_url in images:
                if not isinstance(img_url, str):
                    raise ValueError("Invalid relatedImages URL; image purge stopped.")
                if img_url.strip():
                    referenced_images.add(img_url.strip())

        logging.info(f"[images_deleted_files_purger] Found {len(referenced_images)} referenced images.")
        return referenced_images

    async def _purge_unreferenced_images(self, referenced_images: set[str]) -> None:
        """
        Lists all blobs in the container. For each blob, if its URL isn't in 'referenced_images', delete it.
        """
        logging.info("[images_deleted_files_purger] Starting purge of unreferenced images...")

        async with ChainedTokenCredential(
            ManagedIdentityCredential(), AzureCliCredential(),
        ) as credential:
            async with ContainerClient(
                account_url=self.blob_base_url, container_name=self.container_name, credential=credential,
            ) as container_client:
                await container_client.get_container_properties()
                async for blob in container_client.list_blobs():
                    blob_url = f"{self.blob_base_url}/{self.container_name}/{blob.name}"
                    if blob_url not in referenced_images:
                        await container_client.delete_blob(blob.name)
                        logging.info("[images_deleted_files_purger] Deleted an unreferenced image.")

        logging.info("[images_deleted_files_purger] Purge process finished.")
