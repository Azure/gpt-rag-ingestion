# gpt on your data ingestion

Part of [gpt-rag](https://github.com/Azure/gpt-rag)

## Pre-reqs

- Cognitive Search Service
- Form Recognizer Service
- Azure Storage Account
- Python and PIP 3
- Input data in pdf or png format

## Quick start

**1) Copy input data to a folder**

Create a data folder in the project root and add input PDFs or PNGs to it.

```data/```

**2) Configure environment variables**

Rename [.env.template](.env.template) to ```.env``` and fill values accordingly to your environment.

To use vector search (```VECTOR_INDEX="True"```) your service needs to be this feature activated.

**3) Install Libraries**

```pip3 install -r ./requirements.txt```

To use vector search you need to connect to [Azure SDK Python Dev Feed](https://dev.azure.com/azure-sdk/public/_artifacts/feed/azure-sdk-for-python/connect/pip) and 
use the following line in requrements.txt

```azure-search-documents==11.4.0a20230509004```

**4) Execute ingestion script** 

In a terminal (bash) execute the following line

```./data_ingestion.sh```

## References

Azure Cognitive Search [Vector Index](https://github.com/Azure/cognitive-search-vector-pr/)



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

file: ```frontend/src/pages/layout/Layout.tsx```

```
<h4 className={styles.headerRightText}>Chat On Your Data/h4>
```

file: ```frontend/src/pages/layout/index.html```

```
<title>Chat Chat On Your Data | Demo</title>
```

**3) Logo**

Update frontend logo

file: ```frontend/src/pages/layout/Layout.tsx```

Example:
```
<Link to="/" className={styles.headerTitleContainer}>
    <img height="80px" src="https://www.yourdomain.com/yourlogo.png"></img>
    <h3 className={styles.headerTitle}></h3>
</Link>
```

**4) Home page text**

file: ```frontend/src/pages/chat/Chat.tsx```
```
                    <div className={styles.chatInput}>
                        <QuestionInput
                            clearOnSend
                            placeholder="Escriba aquí su pregunta"
                            disabled={isLoading}
                            onSend={question => makeApiRequestGpt(question)}
                        />
                    </div>
```

file: ```frontend/src/components/ClearChatButton.tsx```
```
        <div className={`${styles.container} ${className ?? ""} ${disabled && styles.disabled}`} onClick={onClick}>
            <Delete24Regular />
            <Text>{"Reiniciar conversación"}</Text>
        </div>
```

**5) Speech Synthesis**

To enable speech synthesis change speechSynthesisEnabled variable to true.

file: ```frontend/src/pages/chat/Chat.tsx```

```
const speechSynthesisEnabled = true;
```