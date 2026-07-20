"""Fail-closed bounds and prohibited-data filtering for ingestion audit events.

This is the sanitizing, bounded adapter every audit event passes through
before it is logged for OpenTelemetry/Application Insights export. It never
raises for the caller's benefit (see ``audit.py``): callers treat sanitizer
failures as a dropped event, never as an ingestion failure.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import Any

from .audit_contract import (
    MAX_ATTRIBUTES,
    MAX_COLLECTION_ITEMS,
    MAX_DEPTH,
    MAX_EMITTED_ARRAY_ITEMS,
    MAX_EVENT_BYTES,
    MAX_METADATA_STRING,
    MAX_SANITIZER_NODES,
    MAX_SENSITIVE_STRING,
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    SENSITIVE_FIELDS,
)


REDACTED = "[REDACTED]"
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_SEPARATOR = re.compile(r"[^a-z0-9]+")
_PROHIBITED_KEY_PARTS = frozenset(
    {
        "authorization",
        "proxyauth",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "accesstoken",
        "refreshtoken",
        "idtoken",
        "bearertoken",
        "apikey",
        "clientsecret",
        "secret",
        "password",
        "credential",
        "connectionstring",
        "sharedaccesssignature",
        "sas",
        "privatekey",
        "certificatecredential",
        "certcredential",
    }
)
_PROHIBITED_VALUE_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"(?i)\bbasic\s+[A-Za-z0-9+/=]{8,}"),
    re.compile(r"(?i)\b(?:cookie|set-cookie)\s*:\s*[^\r\n]{4,}"),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    re.compile(
        r"(?i)\b(?:AccountKey|SharedAccessKey|SharedAccessSignature|ClientSecret|Password|"
        r"AccessToken|RefreshToken|IdToken)\s*=\s*[^;\s]*"
    ),
    re.compile(
        r"(?i)\b(?:Endpoint|Server|Data Source)\s*=\s*[^;]+;"
        r".*\b(?:Key|Password|Secret)\s*=\s*[^;\s]*"
    ),
    re.compile(r"(?i)(?:[?&]|^)(?:sig|se|sp|sv|srt|ss)=([^&\s]*)"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(
        r"""(?ix)
        ["']?(?:authorization|proxy[-_ ]?authorization|cookie|set[-_ ]?cookie|
        access[-_ ]?token|refresh[-_ ]?token|id[-_ ]?token|api[-_ ]?key|
        client[-_ ]?secret|password|credential|connection[-_ ]?string|
        shared[-_ ]?access[-_ ]?signature|private[-_ ]?key)["']?
        \s*[:=]\s*["']?[^"',}\s]*
        """
    ),
    re.compile(r"(?i)https?://[^/\s:@]+:[^/\s@]+@"),
)


class AuditSanitizationError(ValueError):
    """Raised when an event cannot be made safe for export."""


@dataclass(slots=True)
class SanitizedEvent:
    attributes: dict[str, Any]
    serialized: str


def normalize_key(value: str) -> str:
    return _SEPARATOR.sub("", value.casefold())


def contains_prohibited_value(value: str) -> bool:
    return any(pattern.search(value) for pattern in _PROHIBITED_VALUE_PATTERNS)


def _is_prohibited_key(key: str, additional_keys: frozenset[str]) -> bool:
    normalized = normalize_key(key)
    return any(part in normalized for part in _PROHIBITED_KEY_PARTS) or any(
        part and part in normalized for part in additional_keys
    )


class _Sanitizer:
    def __init__(self, additional_keys: frozenset[str]) -> None:
        self.additional_keys = frozenset(
            normalize_key(item[:MAX_METADATA_STRING])
            for item in additional_keys
            if item
        )
        self.omitted: list[str] = []
        self.truncated: list[str] = []
        self.redaction_applied = False
        self._seen: set[int] = set()
        self._nodes_visited = 0

    def _record(self, target: list[str], path: str) -> None:
        bounded_path = path[:MAX_METADATA_STRING]
        if bounded_path not in target and len(target) < MAX_EMITTED_ARRAY_ITEMS:
            target.append(bounded_path)

    def value(
        self,
        value: Any,
        *,
        path: str,
        depth: int,
        sensitive: bool,
    ) -> Any:
        self._nodes_visited += 1
        if self._nodes_visited > MAX_SANITIZER_NODES:
            self._record(self.omitted, path)
            return None
        if depth > MAX_DEPTH:
            self._record(self.omitted, path)
            return None

        if value is None or isinstance(value, (bool, int, float)):
            return value

        if isinstance(value, str):
            limit = MAX_SENSITIVE_STRING if sensitive else MAX_METADATA_STRING
            if len(value) > limit:
                self._record(self.truncated, path)
                value = value[:limit]
            cleaned = _CONTROL_CHARACTERS.sub("", value)
            if contains_prohibited_value(cleaned):
                self.redaction_applied = True
                return REDACTED
            return cleaned

        if isinstance(value, (Mapping, Sequence)) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            identity = id(value)
            if identity in self._seen:
                self._record(self.omitted, path)
                return None
            self._seen.add(identity)
            try:
                if isinstance(value, Mapping):
                    output: dict[str, Any] = {}
                    try:
                        items = iter(value.items())
                        bounded_items = islice(items, MAX_COLLECTION_ITEMS + 1)
                    except Exception:
                        self._record(self.omitted, path)
                        return None
                    for index, item in enumerate(bounded_items):
                        if index == MAX_COLLECTION_ITEMS:
                            self._record(self.truncated, path)
                            break
                        try:
                            raw_key, child = item
                        except (TypeError, ValueError):
                            self._record(self.omitted, f"{path}.*")
                            continue
                        if not isinstance(raw_key, str):
                            self._record(self.omitted, f"{path}.*")
                            continue
                        bounded_key = raw_key[:MAX_METADATA_STRING]
                        child_path = f"{path}.{bounded_key}"
                        if len(raw_key) > MAX_METADATA_STRING:
                            self._record(self.truncated, child_path)
                            self._record(self.omitted, child_path)
                            continue
                        if _is_prohibited_key(
                            bounded_key, self.additional_keys
                        ):
                            output[bounded_key] = REDACTED
                            self.redaction_applied = True
                            continue
                        sanitized = self.value(
                            child,
                            path=child_path,
                            depth=depth + 1,
                            sensitive=sensitive,
                        )
                        if sanitized is not None:
                            output[bounded_key] = sanitized
                    return output

                try:
                    values = islice(iter(value), MAX_EMITTED_ARRAY_ITEMS + 1)
                except Exception:
                    self._record(self.omitted, path)
                    return None
                emitted: list[Any] = []
                for index, child in enumerate(values):
                    if index == MAX_EMITTED_ARRAY_ITEMS:
                        self._record(self.truncated, path)
                        break
                    sanitized = self.value(
                        child,
                        path=f"{path}[{index}]",
                        depth=depth + 1,
                        sensitive=sensitive,
                    )
                    if sanitized is not None:
                        emitted.append(sanitized)
                return emitted
            except Exception:
                self._record(self.omitted, path)
                return None
            finally:
                self._seen.remove(identity)

        self._record(self.omitted, path)
        return None


def sanitize_event(
    event: dict[str, Any],
    *,
    additional_redacted_keys: frozenset[str],
) -> SanitizedEvent:
    sanitizer = _Sanitizer(additional_redacted_keys)
    for field_name, target in (
        ("omitted_fields", sanitizer.omitted),
        ("truncated_fields", sanitizer.truncated),
    ):
        paths = event.get(field_name, []) or []
        if isinstance(paths, Sequence) and not isinstance(
            paths, (str, bytes, bytearray)
        ):
            try:
                for path in islice(iter(paths), MAX_EMITTED_ARRAY_ITEMS):
                    if isinstance(path, str):
                        sanitizer._record(target, path)
            except Exception:
                sanitizer._record(sanitizer.omitted, field_name)
    attributes: dict[str, Any] = {}

    bounded_event_items = islice(iter(event.items()), MAX_ATTRIBUTES + 1)
    for index, (raw_key, value) in enumerate(bounded_event_items):
        if index == MAX_ATTRIBUTES:
            sanitizer._record(sanitizer.omitted, "*")
            break
        if not isinstance(raw_key, str):
            sanitizer._record(sanitizer.omitted, "*")
            continue
        key = raw_key[:MAX_METADATA_STRING]
        if len(raw_key) > MAX_METADATA_STRING:
            sanitizer._record(sanitizer.truncated, key)
            sanitizer._record(sanitizer.omitted, key)
            continue
        if key not in REQUIRED_FIELDS and key not in OPTIONAL_FIELDS:
            sanitizer._record(sanitizer.omitted, key)
            continue
        if len(attributes) >= MAX_ATTRIBUTES:
            sanitizer._record(sanitizer.omitted, key)
            continue
        # Custom redaction keys apply only inside optional nested payloads.
        # Required top-level contract identifiers must remain valid.
        if _is_prohibited_key(key, frozenset()):
            attributes[key] = REDACTED
            sanitizer.redaction_applied = True
            continue
        sanitized = sanitizer.value(
            value,
            path=key,
            depth=0,
            sensitive=key in SENSITIVE_FIELDS,
        )
        if sanitized is not None or key in REQUIRED_FIELDS:
            if key in SENSITIVE_FIELDS and isinstance(sanitized, (dict, list)):
                sanitized = json.dumps(
                    sanitized, ensure_ascii=False, separators=(",", ":"), sort_keys=True
                )
                if len(sanitized) > MAX_SENSITIVE_STRING:
                    sanitizer._record(sanitizer.truncated, key)
                    sanitized = sanitized[:MAX_SENSITIVE_STRING]
            attributes[key] = sanitized

    missing = REQUIRED_FIELDS - attributes.keys()
    if missing:
        raise AuditSanitizationError(
            "Audit event is missing required fields after sanitization."
        )

    attributes["redaction_applied"] = (
        bool(attributes.get("redaction_applied")) or sanitizer.redaction_applied
    )
    attributes["omitted_fields"] = sanitizer.omitted[:MAX_EMITTED_ARRAY_ITEMS]
    attributes["truncated_fields"] = sanitizer.truncated[:MAX_EMITTED_ARRAY_ITEMS]

    optional_drop_order = [
        key
        for key in reversed(tuple(attributes))
        if key not in REQUIRED_FIELDS
    ]
    while True:
        try:
            serialized = json.dumps(
                attributes,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise AuditSanitizationError("Audit event serialization failed.") from exc

        if len(serialized.encode("utf-8")) <= MAX_EVENT_BYTES:
            break
        if not optional_drop_order:
            raise AuditSanitizationError("Audit event exceeds the 16 KiB limit.")
        dropped = optional_drop_order.pop(0)
        attributes.pop(dropped, None)
        sanitizer._record(sanitizer.omitted, dropped)
        attributes["omitted_fields"] = sanitizer.omitted[:MAX_EMITTED_ARRAY_ITEMS]

    if contains_prohibited_value(serialized):
        raise AuditSanitizationError(
            "Audit event failed the final prohibited-value scan."
        )
    if len(attributes) > MAX_ATTRIBUTES:
        raise AuditSanitizationError("Audit event exceeds the attribute limit.")

    return SanitizedEvent(attributes=attributes, serialized=serialized)
