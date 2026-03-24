# Changelog

All notable changes to this project will be documented in this file.  
This format follows [Keep a Changelog](https://keepachangelog.com/) and adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [v2.2.3] – 2026-03-24

### Changed
- **Default chunk overlap increased to 200 tokens**: Changed the default value of `TOKEN_OVERLAP` from `100` to `200` across all chunkers (doc_analysis, json, langchain, nl2sql, transcription), improving context continuity between chunks during document ingestion.
- **Cron fallback defaults for blob ingestion jobs**: Added cron fallback defaults when `CRON_RUN_BLOB_INDEX` and `CRON_RUN_BLOB_PURGE` are not configured: blob indexing now runs hourly (`0 * * * *`) and blob purge runs at 10 minutes past each hour (`10 * * * *`).

### Fixed
- **Multimodal image captions not generated**: The `get_completion()` method in `AzureOpenAIClient` did not accept the `image_base64` parameter passed by the multimodal chunker, causing a `TypeError` on every caption generation call. The exception was caught silently and all image captions defaulted to "No caption available." Added vision support to `get_completion()` by accepting an optional `image_base64` parameter and constructing multimodal messages (text + image) using the OpenAI vision API format when an image is provided.
- **Azure OpenAI API compatibility with newer models**: Replaced `max_tokens` with `max_completion_tokens` in the chat completions API call, fixing a 400 error (`unsupported_parameter`) when using newer models (e.g., GPT-4o) that reject the deprecated parameter.

## [v2.2.2] – 2026-02-04
### Fixed
- Fixed Docker builds on ARM-based machines by explicitly setting the target platform to `linux/amd64`, preventing Azure Container Apps deployment failures.
### Changed
- Pinned the Docker base image to `mcr.microsoft.com/devcontainers/python:3.12-bookworm` to ensure stable package verification behavior across environments.
- Bumped `aiohttp` to `3.13.3`.

## [v2.2.1] – 2026-01-19
### Fixed
- Improved reliability of large spreadsheet ingestion (which generate thousands of embedding calls prone to transient rate limits) by adding robust retry with exponential backoff for Azure OpenAI calls (handles 429/Retry-After and is configurable via `OPENAI_RETRY_*` and `OPENAI_SDK_MAX_RETRIES`).
- Standardized on the container best practice of using a non-privileged port (`8080`) instead of a privileged port (`80`), reducing the risk of runtime/permission friction and improving stability of long-running ingestion workloads.

## [v2.2.0] – 2026-01-15
### Added
- Document-level security enforcement for GPT-RAG using Azure AI Search native ACL/RBAC trimming with end-user identity propagation via `x-ms-query-source-authorization`.
	Includes permission-aware indexing metadata (`userIds`, `groupIds`, `rbacScope`), safe-by-default behavior for requests without a valid user token, and optional elevated-read debugging support.

## [v2.1.0] – 2025-12-15
### Added
- Support for SharePoint Lists
### Changed
- Improved robustness of Blob Storage indexing
- Enhanced data ingestion logging

## [v2.0.5] – 2025-10-02
### Fixed
- Fixed SharePoint ingestion re-indexing unchanged files

## [v2.0.4] – 2025-08-31
### Changed
- Standardized resource group variable as `AZURE_RESOURCE_GROUP`. [#365](https://github.com/Azure/GPT-RAG/issues/365)

## [v2.0.3] – 2025-08-18
### Added
- NL2SQL Ingestion.

## [v2.0.2] – 2025-08-17
### Fixed
- Resolved issue with using Azure Container Apps under a private endpoint in AI Search as a custom web skill.

## [v2.0.1] – 2025-08-08
### Fixed
- Corrected v2.0.0 deployment issues.

## [v2.0.0] – 2025-07-22
### Changed
- Major architecture refactor to support the vNext architecture.

## [v1.0.0] 
- Original version.
