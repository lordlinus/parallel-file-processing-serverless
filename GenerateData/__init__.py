# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.

import datetime
import logging
import os

import pandas as pd
from azure.storage.blob import BlobServiceClient
from mimesis import Address, Datetime, Person
from mimesis.enums import Gender

from ..common import BlobStorageClient


def create_rows_mimesis(num=1):
    person = Person("en")
    addess = Address()
    datetime = Datetime()
    output = [
        {
            "pii_name": person.full_name(),
            "pii_email": person.email(),
            "pii_mobile": person.telephone(),
            "attribute1": person.age(),
            "attribute2": person.blood_type(),
            "attribute3": addess.city(),
            "attribute4": addess.state(),
            "attribute5": datetime.datetime().isoformat(),
            "attribute6": person.nationality(),
            "attribute7": addess.postal_code(),
        }
        for x in range(num)
    ]
    return output


def main(httpPostInput: dict) -> list:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    logging.info(
        f"Python GenerateData function started at {utc_timestamp} with httpPostInput {httpPostInput}"
    )

    try:
        f_path = httpPostInput["rawDataPath"]
        num_files = int(httpPostInput["numFiles"])
        num_rows = int(httpPostInput["numRows"])

        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("DataStorage")
        )
        container_client = blob_service_client.get_container_client(
            os.getenv("DataContainer")
        )
        blob_client: BlobStorageClient = BlobStorageClient(container_client)
        # Generate input data for testing
        for i in range(num_files):
            df = pd.DataFrame(create_rows_mimesis(num_rows))
            logging.info(f"Generating file {f_path}/{i}.csv")
            blob_client.upload_pd_dataframe(df, f"{f_path}/{i}.csv")
        return {"rawDataPath": f_path, "numFiles": num_files, "numRows": num_rows}
    except Exception as e:
        logging.exception("EXCEPTION while getting list", exc_info=e)
