"""
Lazy exports for the tools package to avoid circular imports.

This module exposes common classes via lazy import so importing the package
doesn't eagerly import submodules that may depend on `dependencies`.

Usage:
	from tools import AzureOpenAIClient, BlobClient, AppConfigClient, ...
"""

from typing import Any

__all__ = [
	"AppConfigClient",
	"AzureOpenAIClient",
	"GptTokenEstimator",
	"BlobClient",
	"BlobContainerClient",
	"KeyVaultClient",
	"AISearchClient",
	"DocumentIntelligenceClient",
]


def __getattr__(name: str) -> Any:  # PEP 562 lazy attribute access
	if name == "AppConfigClient":
		from .appconfig import AppConfigClient as _AppConfigClient
		return _AppConfigClient
	if name in ("AzureOpenAIClient", "GptTokenEstimator"):
		from .aoai import (
			AzureOpenAIClient as _AzureOpenAIClient,
			GptTokenEstimator as _GptTokenEstimator,
		)
		return {
			"AzureOpenAIClient": _AzureOpenAIClient,
			"GptTokenEstimator": _GptTokenEstimator,
		}[name]
	if name in ("BlobClient", "BlobContainerClient"):
		from .blob import (
			BlobClient as _BlobClient,
			BlobContainerClient as _BlobContainerClient,
		)
		return {
			"BlobClient": _BlobClient,
			"BlobContainerClient": _BlobContainerClient,
		}[name]
	if name == "KeyVaultClient":
		from .keyvault import KeyVaultClient as _KeyVaultClient
		return _KeyVaultClient
	if name == "AISearchClient":
		from .aisearch import AISearchClient as _AISearchClient
		return _AISearchClient
	if name == "DocumentIntelligenceClient":
		from .doc_intelligence import (
			DocumentIntelligenceClient as _DocumentIntelligenceClient,
		)
		return _DocumentIntelligenceClient
	raise AttributeError(name)


def __dir__():  # help() and dir() friendliness
	return sorted(list(globals().keys()) + __all__)