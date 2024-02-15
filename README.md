# GPT on your data Ingestion

Part of [GPT-RAG](https://github.com/Azure/gpt-rag)

## Getting started

You provision the infrastructure and deploy the solution initially using the GPT-RAG template, as instructed at: https://aka.ms/gpt-rag.

To redeploy only the ingestion component (after the initial deployment of the solution), you will need:

 - Azure Developer CLI: [Download azd for Windows](https://azdrelease.azureedge.net/azd/standalone/release/1.5.0/azd-windows-amd64.msi), [Other OS's](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/install-azd).
 - Powershell (Windows only): [Powershell](https://learn.microsoft.com/en-us/powershell/scripting/install/installing-powershell-on-windows?view=powershell-7.4#installing-the-msi-package)
 - Git: [Download Git](https://git-scm.com/downloads)
 - Python 3.10: [Download Python](https://www.python.org/downloads/release/python-31011/)

Then just clone this repository and reproduce the following commands within the gpt-rag-ingestion directory:  

```
azd auth login  
azd env refresh  
azd deploy  
```

> Note: when running the ```azd env refresh```, use the same environment name, subscription, and region used in the initial provisioning of the infrastructure.

## Document Intelligence API version

To use version 4.0 of Document Intelligence, it is necessary to add the property `DOCINT_API_VERSION` with the value `2023-10-31-preview` in the function app properties. It's important to check if this version is supported in the region where the service was created. More information can be found at [this link](https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/concept-layout?view=doc-intel-4.0.0). If the property has not been defined (default behavior), the version `2023-07-31` (3.1) will be used.

# Supported input formats for data ingestion


**Document Inteligence Chunking**

| Extension | Doc Int API version |
|-----------|-------------------|
| pdf       | 3.1, 4.0          |
| bmp       | 3.1, 4.0          |
| jpeg      | 3.1, 4.0          |
| png       | 3.1, 4.0          |
| tiff      | 3.1, 4.0          |
| docx      | 4.0               |
| pptx      | 4.0               |
| xlsx      | 4.0               |
| html      | 4.0               |

**Langchain text Splitters Chunking**

| Extension | Format |
|-----------|--------|
| txt       | text   |
| html      | html   |
| shtml     | html   |
| htm       | html   |
| py        | python |
| pdf       | pdf    |
| json      | json   |
| csv       | csv    |
| epub      | epub   |
| rtf       | rtf    |
| xml       | xml    |
| xlsx      | xlsx   |
| xls       | xls    |
| pptx      | pptx   |
| ppt       | ppt    |
| msg       | msg    |

Note: First, based on the file extension check if it can be processed with Document Intelligence and then chunked. If not, just use the content extracted by AI Search and attempt to perform chunking with Langchain text splitter.

## References

[Cognitive Search Enrichment Pipeline](https://learn.microsoft.com/en-us/azure/search/cognitive-search-concept-intro)

[Azure Open AI Embeddings Generator](https://github.com/Azure-Samples/azure-search-power-skills/tree/57214f6e8773029a638a8f56840ab79fd38574a2/Vector/EmbeddingGenerator)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
