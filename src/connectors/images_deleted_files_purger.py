import os
import logging

from tools import BlobContainerClient
from tools import AISearchClient 


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
        self.index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "ragindex")
        self.container_name = os.getenv("STORAGE_CONTAINER_IMAGES", "documents-images")
        self.storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
        self.blob_base_url = f"https://{self.storage_account_name}.blob.core.windows.net"

        # Warn if some env vars are missing
        if not all([self.index_name, self.container_name, self.storage_account_name]):
            logging.warning("[images_deleted_files_purger] Missing or incomplete environment variables.")

        # We'll create a single AISearchClient for searching the index
        self.ai_search = AISearchClient()

    async def run(self):
        """
        Executes the purge process (asynchronously):
            1) Gathers all referenced images from the 'relatedImages' field in the search index.
            2) Lists all blobs in the container.
            3) Deletes those not referenced in step 1.
        """
        logging.info("[images_deleted_files_purger] Starting images from deleted files purging run()")

        # 1. Collect all referenced images from Azure AI Search
        referenced_images = await self._get_all_referenced_images()

        # 2. Purge unreferenced images from the container
        await self._purge_unreferenced_images(referenced_images)

        logging.info("[images_deleted_files_purger] Completed run().")

        # Optionally close the AISearchClient
        await self.ai_search.close()

    async def _get_all_referenced_images(self) -> set:
        """
        Uses AISearchClient to retrieve 'relatedImages' from all docs in the index
        and returns a set of URLs.
        """
        logging.info("[images_deleted_files_purger] Retrieving referenced images from AI Search index...")

        referenced_images = set()
        try:
            # We'll search with wildcard '*' and ask for only 'relatedImages' field
            # top=0 or top=1000 or more, depending on how many docs you expect
            # If you have more than 1,000 docs with images, you may want to paginate or set top=0 to gather them all.
            # For a large index, you'd do multiple calls in a loop. This is a basic example:
            results = await self.ai_search.search_documents(
                index_name=self.index_name,
                search_text="*",
                select_fields=["relatedImages"],
                top=1000
            )

            # results is a dict with: { "count": int, "documents": [...], "error": optional error }
            if "documents" in results and isinstance(results["documents"], list):
                for doc in results["documents"]:
                    # doc might be a SearchDocument or dict. If dict, just do doc["relatedImages"] directly.
                    images = doc.get("relatedImages") or []
                    if isinstance(images, list):
                        for img_url in images:
                            if img_url and isinstance(img_url, str):
                                referenced_images.add(img_url.strip())

            if "error" in results and results["error"]:
                logging.error(f"[images_deleted_files_purger] Error from AISearchClient: {results['error']}")

        except Exception as e:
            logging.error(f"[images_deleted_files_purger] Error retrieving referenced images: {e}")

        logging.info(f"[images_deleted_files_purger] Found {len(referenced_images)} referenced images.")
        return referenced_images

    async def _purge_unreferenced_images(self, referenced_images: set):
        """
        Lists all blobs in the container. For each blob, if its URL isn't in 'referenced_images', delete it.
        """
        logging.info("[images_deleted_files_purger] Starting purge of unreferenced images...")

        try:
            container_client = BlobContainerClient(self.blob_base_url, self.container_name)
            # Because container_client.list_blobs() is synchronous for the azure-storage-blob library,
            # we can iterate directly. If you want, you can gather them first in a list.
            blob_list = container_client.list_blobs()

            for blob in blob_list:
                blob_url = f"{self.blob_base_url}/{self.container_name}/{blob}"
                if blob_url not in referenced_images:
                    logging.info(f"[images_deleted_files_purger] Deleting unreferenced blob: {blob_url}")
                    container_client.delete_blob(blob)

        except Exception as e:
            logging.error(f"[images_deleted_files_purger] Error purging images: {e}")

        logging.info("[images_deleted_files_purger] Purge process finished.")
