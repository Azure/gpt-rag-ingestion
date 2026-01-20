import logging
from azure.cosmos.aio import CosmosClient
from dependencies import get_config

class CosmosDBClient:
    """
    CosmosDBClient uses the Cosmos SDK's.
    """

    def __init__(self):
        """
        Initializes the Cosmos DB client with credentials and endpoint.
        """
        # ==== Load all config parameters in one place ====
        self.cfg = get_config()
        self.database_account_name = self.cfg.get("DATABASE_ACCOUNT_NAME")
        self.database_name = self.cfg.get("DATABASE_NAME")
        self.db_uri = f"https://{self.database_account_name}.documents.azure.com:443/"
        # ==== End config block ====

    async def list_documents(self, container_name) -> list:
        """
        Lists all documents from the given container.
        """
        async with CosmosClient(self.db_uri, credential=self.cfg.aiocredential) as db_client:
            db = db_client.get_database_client(database=self.database_name)
            container = db.get_container_client(container_name)

            # Correct usage without the outdated argument
            query = "SELECT * FROM c"
            items_iterable = container.query_items(query=query, partition_key=None)

            documents = []
            async for item in items_iterable:
                documents.append(item)

            return documents

    async def get_document(self, container, key) -> dict: 
        async with CosmosClient(self.db_uri, credential=self.cfg.aiocredential) as db_client:
            db = db_client.get_database_client(database=self.database_name)
            container = db.get_container_client(container)
            try:
                document = await container.read_item(item=key, partition_key=key)
                logging.info(f"[cosmosdb] document {key} retrieved.")
            except Exception as e:
                document = None
                logging.info(f"[cosmosdb] document {key} does not exist.")
            return document

    async def create_document(self, container, key, body=None) -> dict: 
        async with CosmosClient(self.db_uri, credential=self.cfg.aiocredential) as db_client:
            db = db_client.get_database_client(database=self.database_name)
            container = db.get_container_client(container)
            try:
                if body is None:
                    body = {"id": key}
                else:
                    body["id"] = key  # ensure the document id is set
                document = await container.create_item(body=body)                    
                logging.info(f"[cosmosdb] document {key} created.")
            except Exception as e:
                document = None
                logging.info(f"[cosmosdb] error creating document {key}. Error: {e}")
            return document
        
    async def update_document(self, container, document) -> dict: 
        async with CosmosClient(self.db_uri, credential=self.cfg.aiocredential) as db_client:
            db = db_client.get_database_client(database=self.database_name)
            container = db.get_container_client(container)
            try:
                document = await container.replace_item(item=document["id"], body=document)
                logging.info(f"[cosmosdb] document updated.")
            except Exception as e:
                document = None
                logging.warning(f"[cosmosdb] could not update document: {e}", exc_info=True)
            return document