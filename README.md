# gpt on your data ingestion

Part of [gpt-rag](https://github.com/Azure/gpt-rag)

## Pre-reqs

- Cognitive Search Service
- Azure Function with Python 3.10 runtime.
- Azure OpenAI Service with **text-embedding-ada-002** deployment
- Azure Storage Account with **documents** container
- Form Recognizer Service
- Python 3.10 and PIP

## Quick start

**1) Azure Cognitive Search Setup**

After having provisioned the pre-req resources rename .env.template to .env and fill endpoints and keys variables.

Then run setup.py to configure Cognitive Search elements (datasource, index, skillset and indexer) running the following commands in terminal:

```
pip3 install -r requirements.txt
python3 setup.py
```

**2) Set Azure Function Application Settings**

Update the following variables with you project settings:
```
SEARCH_SERVICE
SEARCH_API_KEY
SEARCH_ANALYZER_NAME
STORAGE_CONNECTION_STRING
AZURE_OPENAI_API_KEY
AZURE_OPENAI_API_VERSION
AZURE_OPENAI_SERVICE_NAME
AZURE_OPENAI_EMBEDDING_DEPLOYMENT
```
Update in Azure Portal: Go to ```Azure Portal > Function App > Configuration > Application Settings```.

Update locally if you want to test the function locally: Rename [local.settings.json.template](local.settings.json.template) to ```local.settings.json``` and update environment variables.

**3) Deploy Function to Azure** 

In VSCode with [Azure Function App Extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions) go to the ```Azure``` Window, reveal your Function App in the resource explorer, right-click it then select ```Deploy To Function App...```.


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
