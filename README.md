# GPT on your data Ingestion

Part of [GPT-RAG](https://github.com/Azure/gpt-rag)

## Deploy (quickstart)

Here are the steps to configure cognitive search and deploy ingestion code using the terminal.

**First Check your environment meets the requirements**

- You need **[AZ CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)** to log and run Azure commands in the command line.
- **Python 3.9+** to run the setup script. Ideally use **Python 3.10** (the same version used by the Function runtime).  
- **[Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-python#install-the-azure-functions-core-tools)** will be needeed to deploy the chunking function.

**1) Login to Azure** 

run ```az login``` to log into azure. Run ```az login -i``` if using a VM with managed identity to run the setup.

**2) Clone the repo** 

If you plan to customize the ingestion logic, create a new repo by clicking on the **Use this template** button on top of this page.

Clone the repostory locally:  ```git clone https://github.com/azure/gpt-rag-ingestion```

*If you created a new repository please update the repository URL before running the command*

**3) Deploy function to Azure** 

Enter in the cloned repo folder: ```cd gpt-rag-ingestion```

Use Azure Functions Core Tools to deploy the function: ```func azure functionapp publish FUNCTION_APP_NAME --python```

<!-- Check the function is listed after deployment: ```func azure functionapp list-functions FUNCTION_APP_NAME``` -->

*Replace FUNCTION_APP_NAME with your Ingestion Function App name before running the command*

**4) Run Azure Cognitive Search Setup**

Enter in the cloned repo folder: ```cd gpt-rag-ingestion```

Install python libraries: ```pip install -r requirements.txt --use-deprecated=legacy-resolver```

Run the setup script: ```python setup.py -s SUBSCRIPTION_ID -r RESOURCE_GROUP -f FUNCTION_APP_NAME```

*Replace SUBSCRIPTION_ID, RESOURCE_GROUP and FUNCTION_APP_NAME by the names applicable to your environment*

If you get "ERROR: Failed building wheel for tiktoken", you should install rust compiler toolchain from https://rustup.rs .

<!-- *Add -i command line argument when executing setup.py if using a VM with managed identity to run the setup.* -->

**5) Add source documents to object storage** 

Upload your documents to the *documents* folder in the storage account which name starts with *strag*.

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
