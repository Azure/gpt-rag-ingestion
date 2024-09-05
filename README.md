# GPT on your data Ingestion

Part of [GPT-RAG](https://github.com/Azure/gpt-rag)

## Getting started

You can provision the infrastructure and deploy the whole solution using the GPT-RAG template, as instructed at: https://aka.ms/gpt-rag.

## What if I want to redeploy just the ingestion component?

Eventually, you may want to make some adjustments to the data ingestion code and redeploy the component.

To redeploy only the ingestion component (after the initial deployment of the solution), you will need:

 - Azure Developer CLI: [Download azd for Windows](https://azdrelease.azureedge.net/azd/standalone/release/1.5.0/azd-windows-amd64.msi), [Other OS's](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/install-azd).
 - Powershell (Windows only): [Powershell](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.4#installing-the-msi-package)
 - Git: [Download Git](https://git-scm.com/downloads)
 - Python 3.11: [Download Python](https://www.python.org/downloads/release/python-3118/)

Then just clone this repository and reproduce the following commands within the gpt-rag-ingestion directory:  

```bash
azd auth login  
azd env refresh  
azd deploy  
```

> Note: when running the `azd env refresh`, use the same environment name, subscription, and region used in the initial provisioning of the infrastructure.

## Running Locally with VS Code  
   
[How can I test the data ingestion component locally in VS Code?](docs/LOCAL_DEPLOYMENT.md)

## Document Intelligence API version

To use version 4.0 of Document Intelligence, it is necessary to add the property `DOCINT_API_VERSION` with the value `2024-07-31-preview` in the function app properties. It's important to check if this version is supported in the region where the service was created. More information can be found at [this link](https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/concept-layout?view=doc-intel-4.0.0). If the property has not been defined (default behavior), the version `2023-07-31` (3.1) will be used.

## Document Chunking Process

The `document_chunking` function is responsible for breaking down documents into smaller pieces known as chunks. 

When a document is submitted, the system identifies its file extension and selects the appropriate chunker to divide it into chunks, each tailored to the specific file type.

- **For `.pdf` files**, the system leverages the [DocAnalysisChunker](chunking/chunkers/doc_analysis_chunker.py) to analyze the document using the Document Intelligence API. This analysis extracts structured elements, such as tables and sections, and converts them into Markdown format. The LangChain splitters are then applied to segment the content based on sections. If the Document Intelligence API 4.0 is enabled, `.docx` and `.pptx` files are also processed using this chunker.

- **For image files** such as `.bmp`, `.png`, `.jpeg`, and `.tiff`, the [DocAnalysisChunker](chunking/chunkers/doc_analysis_chunker.py) is employed. This chunker includes Optical Character Recognition (OCR) to extract text from the images before chunking.

- **For specialized formats**, different chunkers are used:
    - `.vtt` files (video transcriptions) are handled by the [TranscriptionChunker](chunking/chunkers/transcription_chunker.py), chunking content by time codes.
    - `.xlsx` files (spreadsheets) are processed by the [SpreadsheetChunker](chunking/chunkers/spreadsheet_chunker.py), chunking by rows or sheets.

- **For text-based files** like `.txt`, `.md`, `.json`, and `.csv`, the system uses the [LangChainChunker](chunking/chunkers/langchain_chunker.py), which uses LangChain splitters to divide the content based on logical separators such as paragraphs or sections.

This flow ensures that each document is processed with the chunker best suited for its format, leading to efficient and accurate chunking tailored to the specific file type.

> [!IMPORTANT]
> Note that the choice of chunker is determined by the format, following the guidelines provided above.

### Customization

The chunking process is flexible and can be customized. You can modify the existing chunkers or create new ones to suit your specific data processing needs, allowing for a more tailored and efficient processing pipeline.

### Supported Formats

Here are the formats supported by the chunkers. Note that the decision on which chunker will be used based on the format is described earlier.

#### Doc Analysis Chunker (Document Intelligence based)

| Extension | Doc Int API Version |
|-----------|---------------------|
| pdf       | 3.1, 4.0            |
| bmp       | 3.1, 4.0            |
| jpeg      | 3.1, 4.0            |
| png       | 3.1, 4.0            |
| tiff      | 3.1, 4.0            |
| xslx      | 4.0                 |
| docx      | 4.0                 |
| pptx      | 4.0                 |

#### LangChain Chunker

| Extension | Format              |
|-----------|---------------------|
| md        | Markdown document   |
| txt       | Plain text file     |
| html      | HTML document       |
| shtml     | Server-side HTML document |
| htm       | HTML document       |
| py        | Python script       |
| json      | JSON data file      |
| csv       | Comma-separated values file |
| xml       | XML data file       |


## References

[AI Search Enrichment Pipeline](https://learn.microsoft.com/en-us/azure/search/cognitive-search-concept-intro)

[Azure Open AI Embeddings Generator](https://github.com/Azure-Samples/azure-search-power-skills/tree/57214f6e8773029a638a8f56840ab79fd38574a2/Vector/EmbeddingGenerator)

## Contributing

We appreciate your interest in contributing to this project! Please refer to the [CONTRIBUTING.md](https://github.com/Azure/GPT-RAG/blob/main/CONTRIBUTING.md) page for detailed guidelines on how to contribute, including information about the Contributor License Agreement (CLA), code of conduct, and the process for submitting pull requests.

Thank you for your support and contributions!

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
