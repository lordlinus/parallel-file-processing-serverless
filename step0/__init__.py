# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

import logging
from ..common import BlobStorageClient
import datetime
import os
from azure.storage.blob import BlobServiceClient


def main(rawDataPath: dict) -> list:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    logging.info(
        f"Python Step 0 function started at {utc_timestamp} with rawDataPath {rawDataPath['rawDataPath']}"
    )

    try:
        rpath = rawDataPath["rawDataPath"]
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("DataStorage")
        )
        container_client = blob_service_client.get_container_client(
            os.getenv("DataContainer")
        )
        blob_client: BlobStorageClient = BlobStorageClient(container_client)

        files = blob_client.get_csv_files(rpath)
        logging.info("Files found: %s", files)
        return files
    except Exception as e:
        logging.exception("EXCEPTION while getting list", exc_info=e)
