# Release Notes v2.3.2

## Upgrading from Earlier Versions

If you are running an older version of the data ingestion component (e.g., v2.0.6, v2.1.0, v2.2.x) and want to upgrade to v2.3.2, follow the instructions below **before running `azd deploy`**. The required steps depend on your current version. Review each section that applies to your upgrade path.

---

## Upgrading from v2.0.x or v2.1.x (versions prior to v2.2.0)

These versions predate the document-level security enforcement feature introduced in v2.2.0. The following steps are **required**:

### 1. Add RBAC Security Fields to Azure AI Search Index

Starting with v2.2.0, the ingestion pipeline writes security metadata to the search index. If your index was created before this version, you must manually add the following fields using the Azure Portal JSON editor or the Azure AI Search REST API:

```json
{
  "name": "metadata_security_user_ids",
  "type": "Collection(Edm.String)",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
},
{
  "name": "metadata_security_group_ids",
  "type": "Collection(Edm.String)",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
},
{
  "name": "metadata_security_rbac_scope",
  "type": "Edm.String",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
}
```

**How to add fields via Azure Portal:**

1. Navigate to your Azure AI Search resource.
2. Go to **Indexes** and select your index (e.g., `ragindex`).
3. Click **Edit JSON** (top toolbar).
4. In the `fields` array, add the three field definitions above.
5. Click **Save**.

> **Note:** Azure AI Search allows adding fields to an existing index, but does not allow modifying or removing fields once they exist.

### 2. Update Container Port Configuration

Starting with v2.2.1, the container uses port `8080` instead of the previously common port `80`. If your Azure Container App is configured for port 80, you must update it:

1. Navigate to your Azure Container App resource (e.g., `ca-xxxx-dataingest`).
2. Go to **Ingress** and change the **Target port** to `8080`.
3. Go to **Containers** → **Health probes** and update:
   - Liveness probe port: `8080`
   - Readiness probe port: `8080`
   - Startup probe port (if configured): `8080`
4. Save the configuration and wait for a new revision to deploy.

Alternatively, using Azure CLI:

```bash
az containerapp ingress update \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --target-port 8080
```

---

## Upgrading from v2.2.0

### 1. Update Container Port Configuration

If you are on v2.2.0, you still need to update the container port from `80` to `8080` (introduced in v2.2.1). Follow the steps in the previous section.

### 2. RBAC Role Assignment for Elevated Read

Starting with v2.2.5, the ingestion service uses elevated-read operations to query the index without permission filtering (required when `permissionFilterOption` is enabled). The managed identity running the Container App must have the **Search Index Data Contributor** role on the Azure AI Search resource.

```bash
az role assignment create \
  --assignee <managed-identity-object-id> \
  --role "Search Index Data Contributor" \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Search/searchServices/<search-service>
```

> The `Search Index Data Contributor` role includes the `elevatedOperations/read` RBAC data action required for the `x-ms-enable-elevated-read` header.

---

## Upgrading from v2.2.1, v2.2.2, v2.2.3, or v2.2.4

### 1. RBAC Role Assignment for Elevated Read

As noted above, v2.2.5 introduced elevated-read headers. Ensure the **Search Index Data Contributor** role is assigned to the Container App managed identity.

### 2. (Optional) Configure Vision Deployment

If you use multimodal processing and your primary chat model does not support vision (e.g., `gpt-5-nano`), configure the `VISION_DEPLOYMENT_NAME` setting in Azure App Configuration to point to a vision-capable model (e.g., `gpt-4o-mini`). This was introduced in v2.2.4.

---

## Upgrading from v2.2.5

### 1. Verify Azure AI Foundry Account

Starting with v2.3.0, the default document analysis path uses **Azure AI Foundry Content Understanding** (`prebuilt-layout`) instead of Document Intelligence, reducing costs by ~69% per page. Ensure you have:

- An Azure AI Foundry account configured.
- The `AI_FOUNDRY_ACCOUNT_ENDPOINT` setting in App Configuration.

If you prefer to continue using Document Intelligence, set `USE_DOCUMENT_INTELLIGENCE=true` in App Configuration.

---

## Resource Recommendations for Processing Large Files

Large document processing (e.g., 100+ page PDFs, large spreadsheets) can be memory-intensive. The following container resource configuration is recommended:

| Container App       | CPU    | Memory |
|---------------------|--------|--------|
| Data Ingestion      | 1.0    | 3 GB   |
| Orchestrator        | 0.5    | 1 GB   |
| Frontend            | 0.5    | 1 GB   |

If you are on a shared **workload profile** with limited CPU capacity (e.g., 4 CPUs total), ensure the sum of all container CPU allocations does not exceed the profile limit.

To update container resources via CLI:

```bash
az containerapp update \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --cpu 1.0 \
  --memory 3Gi
```

---

## Post-Deployment Verification

After deployment, verify the running version:

```bash
az containerapp show \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --query "properties.template.containers[0].image" \
  -o tsv
```

The image tag corresponds to the Git commit SHA. You can map it to a release by checking the repository tags:

```bash
git log --oneline --decorate v2.3.2
```

To validate the ingestion pipeline:

1. Upload a small test file to the documents container.
2. Monitor the ingestion logs via the admin dashboard (`/dashboard`) or Container App logs.
3. Verify the document appears in the search index.

---

## Summary by Source Version

| Current Version | Port Change | Index Fields | RBAC Role | AI Foundry |
|-----------------|-------------|--------------|-----------|------------|
| v2.0.x          | Required    | Required     | Required  | Recommended |
| v2.1.x          | Required    | Required     | Required  | Recommended |
| v2.2.0          | Required    | ✓            | Required  | Recommended |
| v2.2.1–v2.2.4   | ✓           | ✓            | Required  | Recommended |
| v2.2.5          | ✓           | ✓            | ✓         | Recommended |
