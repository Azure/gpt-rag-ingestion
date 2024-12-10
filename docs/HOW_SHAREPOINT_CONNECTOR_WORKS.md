## How the SharePoint Connector Works

Understanding the internal workings of the SharePoint connector is essential for effective troubleshooting and optimization. This section provides a high-level overview of the connector's operations, focusing on the main processes involved in indexing and maintaining the Azure AI Search Index.

### Overview

The SharePoint connector operates through two primary processes:

1. **Indexing SharePoint Files**: Retrieves files from SharePoint, processes them, and indexes their content into the Azure AI Search Index (`ragindex`).
2. **Purging Deleted Files**: Identifies and removes files that have been deleted from SharePoint to keep the search index up-to-date.

Both processes are managed by scheduled Azure Functions that run at regular intervals, leveraging configuration settings to determine their behavior.

### 1. Indexing Process

The indexing process begins by reading configuration settings such as `SHAREPOINT_TENANT_ID`, `SHAREPOINT_CLIENT_ID`, `SHAREPOINT_SITE_DOMAIN`, and `SHAREPOINT_SITE_NAME` to establish connections with SharePoint and Azure AI Search. The connector securely retrieves the `SHAREPOINT_CLIENT_SECRET` from Azure Key Vault to authenticate with Microsoft Graph API.

Once authenticated, it connects to the specified SharePoint site and folder (`SHAREPOINT_SITE_FOLDER`) to fetch files that match the defined formats (`SHAREPOINT_FILES_FORMAT`). For each file, the connector checks if it has been modified since the last indexing operation (`SHAREPOINT_CONNECTOR_ENABLED` determines if the connector is active). New or updated files are then processed, where larger documents are split into smaller chunks to facilitate efficient indexing. These chunks, enriched with relevant metadata, are indexed into the Azure AI Search Index, enabling robust search capabilities.

### 2. Purging Deleted Files

The purging process also begins by reading the necessary configuration settings and retrieving credentials from Azure Key Vault. It scans the Azure AI Search Index to identify documents linked to SharePoint files. For each identified document, the connector verifies the existence of the corresponding SharePoint file using the provided `SHAREPOINT_TENANT_ID` and `SHAREPOINT_CLIENT_ID`. If a file no longer exists in SharePoint, its associated documents are removed from the search index. This ensures that the search index remains accurate and free from outdated or irrelevant data.

### Concurrency Control

Effective concurrency control is vital to ensure that the SharePoint connector operates efficiently without overloading system resources. The connector employs the following strategies:

- **Task Limiting**: Utilizes concurrency mechanisms (such as semaphores) to limit the number of files being processed simultaneously. This prevents excessive resource consumption and ensures stable performance.
  
- **Scheduled Triggers**: Both the indexing and purging processes are triggered by timer-based Azure Functions that run at regular intervals (e.g., every 10 minutes). This scheduling helps distribute the workload evenly over time.
  
- **Batch Processing**: When handling deletions, the connector processes documents in batches. This approach reduces the number of API calls and optimizes the performance of deletion operations.
  
- **Resource Management**: By controlling the number of concurrent operations, the connector ensures that it does not exceed the available system resources, maintaining overall system stability and responsiveness.

### Function Triggers

The SharePoint connector leverages Azure Function triggers to automate and manage its operations:

1. **Indexing Trigger**:
   - **Type**: Timer-Triggered Azure Function.
   - **Schedule**: Executes at defined intervals (e.g., every 10 minutes) and runs upon startup.
   - **Purpose**: Initiates the indexing process, ensuring that new and updated SharePoint files are regularly ingested and indexed.

2. **Purging Trigger**:
   - **Type**: Timer-Triggered Azure Function.
   - **Schedule**: Executes at defined intervals (e.g., every 10 minutes) but does not run on startup.
   - **Purpose**: Initiates the purging process, ensuring that deleted SharePoint files are promptly removed from the search index to maintain data integrity.

These triggers ensure that both indexing and purging operations occur consistently and automatically, reducing the need for manual intervention and ensuring the search index remains up-to-date.
