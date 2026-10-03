"""Public worker exports, loaded only when requested.

The lightweight jobs.runtime interface must not initialize worker clients
before main performs its authentication and deployment-mode checks.
"""

__all__ = [
    "ImagesDeletedFilesPurger", "NL2SQLIndexer", "NL2SQLPurger",
    "SharePointIndexer", "SharePointGraphClient", "SharePointConfig",
    "SharePointPurger", "BlobStorageDocumentIndexer",
    "BlobStorageDeletedItemsCleaner",
]


def __getattr__(name: str) -> object:
    if name == "ImagesDeletedFilesPurger":
        from .multimodal_images_purger import ImagesDeletedFilesPurger
        return ImagesDeletedFilesPurger
    if name == "NL2SQLIndexer":
        from .nl2sql_indexer import NL2SQLIndexer
        return NL2SQLIndexer
    if name == "NL2SQLPurger":
        from .nl2sql_purger import NL2SQLPurger
        return NL2SQLPurger
    if name == "SharePointIndexer":
        from .sharepoint_indexer import SharePointIndexer
        return SharePointIndexer
    if name == "SharePointGraphClient":
        from .sharepoint_graph_client import SharePointGraphClient
        return SharePointGraphClient
    if name == "SharePointConfig":
        from .sharepoint_ingestion_config import SharePointConfig
        return SharePointConfig
    if name == "SharePointPurger":
        from .sharepoint_purger import SharePointPurger
        return SharePointPurger
    if name in {"BlobStorageDocumentIndexer", "BlobStorageDeletedItemsCleaner"}:
        from .blob_storage_indexer import (
            BlobStorageDocumentIndexer,
            BlobStorageDeletedItemsCleaner,
        )
        return {
            "BlobStorageDocumentIndexer": BlobStorageDocumentIndexer,
            "BlobStorageDeletedItemsCleaner": BlobStorageDeletedItemsCleaner,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")