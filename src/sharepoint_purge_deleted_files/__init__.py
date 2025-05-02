import logging
import json
# import asyncio
import time
import datetime
from json import JSONEncoder

import jsonschema
import azure.functions as func
from azurefunctions.extensions.http.fastapi import Request, StreamingResponse, JSONResponse

from chunking import DocumentChunker
from connectors import SharepointFilesIndexer, SharepointDeletedFilesPurger
from connectors import ImagesDeletedFilesPurger
from tools import BlobClient
from utils.file_utils import get_filename

from configuration import Configuration

config = Configuration()

# -------------------------------
# Azure Functions
# -------------------------------

app = func.FunctionApp()

# ---------------------------------------------
# SharePoint Connector Functions (Timer Triggered)
# ---------------------------------------------

@app.timer_trigger(schedule="0 */30 * * * *", arg_name="timer", run_on_startup=True, use_monitor=False) 
@app.function_name(name="sharepoint_purge_deleted_files")
async def sharepoint_purge_deleted_files(timer: func.TimerRequest) -> None:
    logging.debug("[sharepoint_purge_deleted_files] Started sharepoint purge deleted files function.")
    try:
        purger = SharepointDeletedFilesPurger()
        await purger.run() 
    except Exception as e:
        logging.error(f"[sharepoint_purge_deleted_files] An unexpected error occurred: {e}", exc_info=True)
