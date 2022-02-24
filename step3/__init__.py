# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

import datetime
import logging
import os
from dataclasses import asdict, dataclass

import pandas as pd
from azure.storage.blob import BlobServiceClient

from ..common import BlobStorageClient


@dataclass
class output:
    step3_output_filename: str
    col_count: int


def main(name: dict) -> str:
    filename = name["step2_output_filename"]
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    logging.info(
        f"Python Step 3 function started at {utc_timestamp} with input {filename}"
    )
    try:
        # Create the BlobServiceClient object which will be used to create a container client

        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("DataStorage")
        )
        container_client = blob_service_client.get_container_client(
            os.getenv("DataContainer")
        )
        output_subpath = os.getenv("Step3DataSubpath")
        output_filename = f"{output_subpath}/interim_{filename.split('/')[-1]}"
        blob_client: BlobStorageClient = BlobStorageClient(container_client)

        # Download the file from the blob storage
        input_data = blob_client.download_blob_content(filename)

        # Take a sample of the data and save it to a CSV file for next step
        csv_file = pd.read_csv(input_data)
        sample = csv_file.sample(n=10)
        col_count = sample.shape[1]
        blob_client.upload_pd_dataframe(sample, output_filename)

        # Return the output of the function
        return asdict(
            output(step3_output_filename=output_filename, col_count=col_count)
        )
    except Exception as e:
        logging.exception("EXCEPTION while running Step3", exc_info=e)
