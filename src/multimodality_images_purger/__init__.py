import logging
import json
# import asyncio
import time
import datetime
from json import JSONEncoder
import dotenv

import jsonschema
import azure.functions as func
from azurefunctions.extensions.http.fastapi import Request, StreamingResponse, JSONResponse

from chunking import DocumentChunker
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
# Deleted Files Image Purger (Timer Triggered)
# ---------------------------------------------

@app.function_name(name="multimodality_images_purger")
@app.timer_trigger(schedule="0 0 0 * * *", arg_name="timer", run_on_startup=True, use_monitor=True) 
async def images_purge_timer(timer: func.TimerRequest):
    if timer.past_due:
        logging.info("[multimodality_images_purger] Timer is past due.")
    
    logging.info("[multimodality_images_purger] Timer trigger started.")

    # Purge only runs when MULTIMODAL == 'true'
    multi_var = (config.get_value("MULTIMODAL") or "").lower()
    should_run_multimodality = multi_var in ["true", "1", "yes"]

    # Only run if MULTIMODAL == true
    if not should_run_multimodality:
        logging.info("[multimodality_images_purger] MULTIMODAL != true. Skipping purge.")
        return

    try:
        purger = ImagesDeletedFilesPurger()
        await purger.run()
    except Exception as e:
        logging.error(f"[multimodality_images_purger] Error running images purge: {e}")
