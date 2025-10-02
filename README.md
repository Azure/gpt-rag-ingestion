<!-- 
page_type: sample
languages:
- azdeveloper
- powershell
- bicep
products:
- azure
- azure-ai-foundry
- azure-openai
- azure-ai-search
urlFragment: GPT-RAG
name: Multi-repo ChatGPT and Enterprise data with Azure OpenAI and AI Search
description: GPT-RAG core is a Retrieval-Augmented Generation pattern running in Azure, using Azure AI Search for retrieval and Azure OpenAI large language models to power ChatGPT-style and Q&A experiences.
-->
# GPT-RAG Data Ingestion

Part of the [GPT-RAG](https://github.com/Azure/gpt-rag) solution.

The **GPT-RAG Data Ingestion** service automates the processing of diverse document types—such as PDFs, images, spreadsheets, transcripts, and SharePoint files—preparing them for indexing in Azure AI Search. It uses intelligent chunking strategies tailored to each format, generates text and image embeddings, and enables rich, multimodal retrieval experies for agent-based RAG applications.

## How data ingestion works

The service performs the following steps:

* **Scan sources**: Detects new or updated content in configured sources
* **Process content**: Chunk and enrich data for retrieval
* **Index documents**: Writes processed chunks into Azure AI Search
* **Schedule execution**: Runs on a CRON-based scheduler defined by environment variables

## Supported data sources

- [Blob Storage](docs/blob_data_source.md)
- [NL2SQL Metadata](docs/nl2sql_data_source.md)
- SharePoint

## Supported formats and chunkers

The ingestion service selects a chunker based on the file extension, ensuring each document is processed with the most suitable method.

* **`.pdf` files** — Processed by the [DocAnalysisChunker](chunking/chunkers/doc_analysis_chunker.py) using the Document Intelligence API. Structured elements such as tables and sections are extracted and converted into Markdown, then segmented with LangChain splitters. When Document Intelligence API 4.0 is enabled, `.docx` and `.pptx` files are handled the same way.

* **Image files** (`.bmp`, `.png`, `.jpeg`, `.tiff`) — The [DocAnalysisChunker](chunking/chunkers/doc_analysis_chunker.py) applies OCR to extract text before chunking.
  
* **Text-based files** (`.txt`, `.md`, `.json`, `.csv`) — Processed by the [LangChainChunker](chunking/chunkers/langchain_chunker.py), which splits content into paragraphs or sections.

* **Specialized formats**:

  * `.vtt` (video transcripts) — Handled by the [TranscriptionChunker](chunking/chunkers/transcription_chunker.py), which splits content by time codes.
  * `.xlsx` (spreadsheets) — Processed by the [SpreadsheetChunker](chunking/chunkers/spreadsheet_chunker.py), chunked by rows or sheets.

## How to deploy the data ingestion service

### Prerequisites

Before deploying the application, you must provision the infrastructure as described in the [GPT-RAG](https://github.com/azure/gpt-rag) repo. This includes creating all necessary Azure resources required to support the application runtime.

<details markdown="block">
<summary>Click to view <strong>software</strong> prerequisites</summary>
<br>
The machine used to customize and or deploy the service should have:

* Azure CLI: [Install Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)
* Azure Developer CLI (optional, if using azd): [Install azd](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/install-azd)
* Git: [Download Git](https://git-scm.com/downloads)
* Python 3.12: [Download Python 3.12](https://www.python.org/downloads/release/python-3120/)
* Docker CLI: [Install Docker](https://docs.docker.com/get-docker/)
* VS Code (recommended): [Download VS Code](https://code.visualstudio.com/download)
</details>


<details markdown="block">
<summary>Click to view <strong>permissions</strong> requirements</summary>
<br>
To customize the service, your user should have the following roles:

| Resource                | Role                                | Description                                 |
| :---------------------- | :---------------------------------- | :------------------------------------------ |
| App Configuration Store | App Configuration Data Owner        | Full control over configuration settings    |
| Container Registry      | AcrPush                             | Push and pull container images              |
| AI Search Service       | Search Index Data Contributor       | Read and write index data                   |
| Storage Account         | Storage Blob Data Contributor       | Read and write blob data                    |
| Cosmos DB               | Cosmos DB Built-in Data Contributor | Read and write documents in Cosmos DB       |

To deploy the service, assign these roles to your user or service principal:

| Resource                                   | Role                             | Description           |
| :----------------------------------------- | :------------------------------- | :-------------------- |
| App Configuration Store                    | App Configuration Data Reader    | Read config           |
| Container Registry                         | AcrPush                          | Push images           |
| Azure Container App                        | Azure Container Apps Contributor | Manage Container Apps |

Ensure the deployment identity has these roles at the correct scope (subscription or resource group).

</details>

## Deployment steps

Make sure you're logged in to Azure before anything else:

```bash
az login
```

### Deploying the app with azd (recommended)

Initialize the template:
```shell
azd init -t azure/gpt-rag-ingestion 
```
> [!IMPORTANT]
> Use the **same environment name** with `azd init` as in the infrastructure deployment to keep components consistent.

Update env variables then deploy:
```shell
azd env refresh
azd deploy 
```
> [!IMPORTANT]
> Run `azd env refresh` with the **same subscription** and **resource group** used in the infrastructure deployment.

### Deploying the app with a shell script

To deploy using a script, first clone the repository, set the App Configuration endpoint, and then run the deployment script.

##### PowerShell (Windows)

```powershell
git clone https://github.com/Azure/gpt-rag-ingestion.git
$env:APP_CONFIG_ENDPOINT = "https://<your-app-config-name>.azconfig.io"
cd gpt-rag-ingestion
.\scripts\deploy.ps1
```

##### Bash (Linux/macOS)
```bash
git clone https://github.com/Azure/gpt-rag-ingestion.git
export APP_CONFIG_ENDPOINT="https://<your-app-config-name>.azconfig.io"
cd gpt-rag-ingestion
./scripts/deploy.sh
````

## Previous Releases

> [!NOTE]  
> For earlier versions, use the corresponding release in the GitHub repository (e.g., v1.0.0 for the initial version).


## 🤝 Contributing

We appreciate contributions! See [CONTRIBUTING](https://github.com/Azure/gpt-rag/blob/main/CONTRIBUTING.md) for guidelines on submitting pull requests.

## Trademarks

This project may contain trademarks or logos. Authorized use of Microsoft trademarks or logos must follow [Microsoft’s Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Modified versions must not imply sponsorship or cause confusion. Third-party trademarks are subject to their own policies.
