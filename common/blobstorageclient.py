"""
Interaction with Azure Blob Storage.
"""

import re
import datetime
import logging
import time
from enum import Enum
from io import TextIOWrapper, BytesIO

import dateutil.parser as dt
from azure.storage.blob import ContainerClient, BlobClient
import pandas as pd

DEFAULT_ENCODING = "UTF-8-SIG"


class BlobStorageClient:
    """
    Class to interact with Azure Blob Storage.
    """

    def __init__(self, container_client: ContainerClient):
        self.__container_client = container_client

    def get_csv_files(self, folder: str) -> list:
        """
        Get all files in a folder.
        """
        files = []
        for blob in self.__container_client.list_blobs(name_starts_with=folder):
            if blob.name.endswith(".csv"):
                files.append(blob.name)
        return files

    def download_blob_content(self, path: str) -> TextIOWrapper:
        """
        Download a file as text from Azure Blob Storage.
        """
        blob: BlobClient = self.__container_client.get_blob_client(path)
        stream_downloader = blob.download_blob()
        encoding = stream_downloader.properties.content_settings.content_encoding
        encoding = encoding if encoding else DEFAULT_ENCODING
        contents = stream_downloader.readall()
        stream = TextIOWrapper(BytesIO(contents), encoding=encoding)
        return stream

    def upload_pd_dataframe(self, df: pd.DataFrame, path: str, metadata: dict = None):
        """
        Upload a pandas dataframe as a csv file to Azure Blob Storage.
        """
        csv_buffer = df.to_csv(index=False)
        self.upload_blob_content(csv_buffer, path, metadata=metadata)

    def upload_blob_content(self, content: str, path: str, metadata: dict = None):
        """
        Upload a file as text to Azure Blob Storage.
        """
        blob: BlobClient = self.__container_client.get_blob_client(path)
        blob.upload_blob(content, overwrite=True, metadata=metadata)

    def move_blob(self, source_path: str, target_folder: str):
        """
        Move a blob in the container to a target folder.
        """
        target_path = f"{target_folder}/{source_path}"
        target_blob: BlobClient = self.__container_client.get_blob_client(target_path)
        source_url = f"{self.__container_client.url}/{source_path}"
        target_blob.start_copy_from_url(source_url)
        self.__wait_for_copy(target_blob)

        source_blob: BlobClient = self.__container_client.get_blob_client(source_path)
        source_blob.delete_blob()

    def __wait_for_copy(self, blob: BlobClient):
        """
        Wait for the start_copy_from_url method to be completed
        as per: https://github.com/Azure/azure-sdk-for-python/issues/7043
        """
        count = 0
        props = blob.get_blob_properties()
        while props.copy.status == "pending":
            count = count + 1
            if count > 10:
                raise TimeoutError("Timed out waiting for async copy to complete.")
            time.sleep(5)
            props = blob.get_blob_properties()
        return props

    def __get_blob_status(self, blob_metadata: str) -> str:
        return None if not blob_metadata else blob_metadata.get(STATUS)

    def __get_timestamp(self, f_date: str, f_time: str) -> datetime:
        """
        Date looks like this: YYYYMMDD and time look like this: HHMM
        dt.parse requires the timestamp to be like this: "2020-03-27T08:49:30.000Z"
        """
        timestamp = f"{f_date[:4]}-{f_date[4:6]}-{f_date[6:8]}T{f_time[:2]}:{f_time[2:4]}:00.000Z"
        return dt.parse(timestamp)
