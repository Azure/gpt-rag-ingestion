import dataclasses
import hashlib
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set

from dependencies import get_config

LOG_SCOPE = "[sp-ingest]"

LIST_TYPE_DOCUMENT_LIBRARY = "documentLibrary"
LIST_TYPE_GENERIC_LIST = "genericList"
_VALID_LIST_TYPES = {
    LIST_TYPE_DOCUMENT_LIBRARY.lower(),
    LIST_TYPE_GENERIC_LIST.lower(),
}


def _get_config_str(name: str) -> str:
    try:
        value = get_config().get(name, "", allow_none=True)
    except Exception:
        return ""
    if value is None:
        return ""
    return str(value).strip()


def _env_or(default: str, *keys: str) -> str:
    for key in keys:
        value = _get_config_str(key)
        if value:
            return value
    return default


@dataclasses.dataclass
class SharePointConfig:
    jobs_log_container: str = "jobs"
    indexer_name: str = "sharepoint-indexer"
    search_endpoint: str = ""
    search_index_name: str = ""
    files_format: str = "pdf,docx,pptx"
    batch_size: int = 500
    max_concurrency: int = 4
    storage_account_name: str = ""
    tenant_id: str = ""
    client_id: str = ""
    client_secret_name: str = "sharepointClientSecret"

    @staticmethod
    def from_app_config() -> "SharePointConfig":
        app = get_config()
        return SharePointConfig(
            jobs_log_container=app.get("JOBS_LOG_CONTAINER", "jobs"),
            indexer_name=app.get("SP_INDEXER_NAME", app.get("SP_LISTS_INDEXER_NAME", "sharepoint-indexer")),
            search_endpoint=app.get("SEARCH_SERVICE_QUERY_ENDPOINT", ""),
            search_index_name=app.get("SEARCH_RAG_INDEX_NAME", app.get("AI_SEARCH_INDEX_NAME", "")),
            files_format=app.get("SHAREPOINT_FILES_FORMAT", "pdf,docx,pptx"),
            batch_size=int(app.get("INDEXER_BATCH_SIZE", 500)),
            max_concurrency=int(app.get("INDEXER_MAX_CONCURRENCY", 4)),
            storage_account_name=app.get("STORAGE_ACCOUNT_NAME", ""),
            tenant_id=app.get("SHAREPOINT_TENANT_ID", ""),
            client_id=app.get("SHAREPOINT_CLIENT_ID", ""),
            client_secret_name=app.get("SHAREPOINT_CLIENT_SECRET_NAME", "sharepointClientSecret"),
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dt(val: str) -> datetime:
    s = (val or "").strip()
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        logging.warning(f"{LOG_SCOPE} could not parse datetime '{val}', using epoch (no reindex)")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _get_setting_float(name: str, default: float) -> float:
    val = _get_config_str(name)
    if not val:
        return float(default)
    try:
        return float(val)
    except (ValueError, TypeError):
        logging.warning(f"{LOG_SCOPE} invalid float for %s=%r; using default %s", name, val, default)
        return float(default)


def _get_setting_int(name: str, default: int) -> int:
    val = _get_config_str(name)
    if not val:
        return int(default)
    try:
        return int(val)
    except (ValueError, TypeError):
        logging.warning(f"{LOG_SCOPE} invalid int for %s=%r; using default %s", name, val, default)
        return int(default)


def _is_strictly_newer(incoming: datetime, existing: Optional[datetime], skew: timedelta = timedelta(seconds=1)) -> bool:
    if existing is None:
        return True
    return (incoming - existing) > skew


def _chunk(items: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _sanitize_key_part(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


def _make_parent_key(*parts: str) -> str:
    return "/" + "/".join(p.strip("/") for p in parts if p is not None)


def _make_chunk_key_prefix(parent_id: str) -> str:
    base = _sanitize_key_part(parent_id.lstrip("/"))
    if len(base) > 128:
        digest = hashlib.sha1(parent_id.encode("utf-8")).hexdigest()[:10]
        base = f"{base[:100]}-{digest}"
    return f"{base}-c*"


def _make_chunk_key(parent_id: str, chunk_id: int) -> str:
    base = _sanitize_key_part(parent_id.lstrip("/"))
    if not base:
        base = "doc"
    if len(base) > 128:
        digest = hashlib.sha1(parent_id.encode("utf-8")).hexdigest()[:10]
        base = f"{base[:100]}-{digest}"
    return f"{base}-c{chunk_id:05d}"


@dataclasses.dataclass
class RunStats:
    items_discovered: int = 0
    items_candidates: int = 0
    items_indexed: int = 0
    items_skipped_nochange: int = 0
    items_failed: int = 0
    att_candidates: int = 0
    att_skipped_not_newer: int = 0
    att_skipped_ext_not_allowed: int = 0
    att_uploaded_chunks: int = 0
    body_docs_uploaded: int = 0


__all__ = [
    "SharePointConfig",
    "RunStats",
    "LOG_SCOPE",
    "LIST_TYPE_DOCUMENT_LIBRARY",
    "LIST_TYPE_GENERIC_LIST",
    "_utc_now",
    "_as_dt",
    "_get_setting_float",
    "_get_setting_int",
    "_is_strictly_newer",
    "_chunk",
    "_sanitize_key_part",
    "_make_parent_key",
    "_make_chunk_key_prefix",
    "_make_chunk_key",
]
