# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.


import datetime
import logging
import os
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd
from azure.storage.blob import BlobServiceClient

from ..common import BlobStorageClient


@dataclass
class output:
    step1_output_filename: str
    col_count: int
    row_count: int


def main(filename: str) -> output:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    logging.info(
        f"Python Step 1 function started at {utc_timestamp} with filename {filename}"
    )

    try:
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("DataStorage")
        )
        container_client = blob_service_client.get_container_client(
            os.getenv("DataContainer")
        )
        output_subpath = os.getenv("Step1DataSubpath")
        output_filename = f"{output_subpath}/data_{filename.split('/')[-1]}"
        blob_client: BlobStorageClient = BlobStorageClient(container_client)

        # Download the file from the blob storage
        input_data = blob_client.download_blob_content(filename)

        # Read the file into a pandas dataframe and do "something" to it.
        #  In this example we are generating 100,000 random numbers and saving them to a CSV file.
        csv_file = pd.read_csv(input_data)
        col_count = csv_file.shape[1]
        row_count = csv_file.shape[0]
        rand_data_step_1 = pd.DataFrame(
            np.random.randint(0, 99999, size=(1_00_000, col_count)),
            columns=csv_file.columns,
        )
        blob_client.upload_pd_dataframe(rand_data_step_1, output_filename)

        # Return the output of the function
        return asdict(
            output(step1_output_filename=output_filename, col_count=col_count, row_count=row_count)
        )
    except Exception as e:
        logging.exception("EXCEPTION while running Step1", exc_info=e)
