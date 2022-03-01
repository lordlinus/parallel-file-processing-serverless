import os
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pandas_profiling import ProfileReport
from plotly.subplots import make_subplots
from streamlit_pandas_profiling import st_profile_report

from blobstorageclient import BlobStorageClient

load_dotenv()

DATASTORAGE = os.getenv("DATASTORAGE")
DATACONTAINER = os.getenv("DATACONTAINER")

try:
    blob_service_client = BlobServiceClient.from_connection_string(DATASTORAGE)
    container_client = blob_service_client.get_container_client(DATACONTAINER)
    blob_client: BlobStorageClient = BlobStorageClient(container_client)
except Exception as e:
    st.error(f"EXCEPTION while connecting to Azure Blob Storage {e}")

st.set_page_config(
    page_title="Demo app for Azure",
    page_icon="https://streamlit.io/favicon.svg",
)

st.title("Self Service Analytics")
st.markdown(
    "This is an internal tool for self service data analytics - Contact XYZ for more info"
)
st.sidebar.title("Workflow selector")
st.sidebar.markdown("Select the workflow accordingly:")


@st.cache(allow_output_mutation=True)
def gen_profile_report(df, *report_args, **report_kwargs):
    return df.profile_report(*report_args, **report_kwargs)


def check_az_func(url):
    JOB_STATUS = "Running"
    while JOB_STATUS == "Running" or JOB_STATUS == "Pending":
        time.sleep(5)
        r = requests.get(url)
        # st.write(f"Job Status: {JOB_STATUS}")
        st.json(r.json())
        JOB_STATUS = r.json()["runtimeStatus"]
        if JOB_STATUS == "failed":
            st.error(f"Job failed. Please check logs")
            break
        elif JOB_STATUS == "succeeded":
            st.info(f"Job succeeded")
            st.json(r.json())


def run_azfunc(rawDataPath="raw"):
    header = {"Content-Type": "application/json"}
    r = requests.post(
        "https://process-files.azurewebsites.net/api/orchestrators/OrchestratorFunc",
        json={"rawDataPath": rawDataPath, "numFiles": "1", "numRows": "10"},
        headers=header,
    )
    # st.write(r.json()) # show the response from Azure functions
    check_az_func(url=r.json()["statusQueryGetUri"])


def gen_pandas_stats(df):
    st.write("Number of columns in the dataset: ", df.shape[1])
    st.write("Number of rows in the dataset: ", df.shape[0])
    st.write(df.head(5))
    pr = gen_profile_report(df, explorative=True)
    with st.expander("REPORT", expanded=True):
        st_profile_report(pr)


def main():
    st.sidebar.markdown("**1.** Select the **Data**:")

    data = st.sidebar.selectbox("Data", ["Uploaded Data", "Sample Data"])

    st.sidebar.markdown("**2.** Select the **Validate/Clean**:")

    validateclean = st.sidebar.selectbox(
        "Validate/Clean",
        ["Clean using Azure Functions", "Clean using Spark"],
    )
    st.sidebar.markdown("**3.** Select the **Score using ML**:")
    score = st.sidebar.selectbox("Train ML", ["Model1", "Model2", "Model3"])
    st.sidebar.markdown("**4.** Select the **Save Output**:")
    save = st.sidebar.selectbox(
        "Save", ["Save results to Azure Blob Storage", "Save to CSV locally"]
    )

    dataframe = pd.DataFrame()
    filename = ""
    st.subheader("Step - 1: Upload the data")
    if data == "Uploaded Data":
        uploaded_file = st.file_uploader("Choose a file")
        if uploaded_file is not None:
            filename = uploaded_file.name
            # Can be used wherever a "file-like" object is accepted:
            dataframe = pd.read_csv(uploaded_file)
            gen_pandas_stats(dataframe)

    elif data == "Sample Data":
        st.subheader("Sample Data generated with 100 rows")
        filename = "random_data.csv"
        dataframe = blob_client.gen_data()
        gen_pandas_stats(dataframe)

    st.subheader("Step - 2: Clean the Data")
    if st.button(label=f"Run - {validateclean}", key="run-azfunc"):
        if validateclean == "Clean using Azure Functions":
            run_azfunc()
        elif validateclean == "Clean using Azure Databricks":
            st.write("Clean using Azure Databricks")
    st.subheader("Step - 3: Choose the model to apply")
    if st.button(label=f"Train ML - {score}", key="apply-ml"):
        st.write("Training the model")
        # TODO: Add code to call Azure ML to score the data
    st.subheader("Step - 4: Choose the output")
    if st.button(label=f"Download - {save}", key="download-file"):
        if save == "Save results to Azure Blob Storage":
            try:
                blob_client.upload_pd_dataframe(
                    dataframe, path=filename, metadata={"type": "csv"}
                )
            except Exception as e:
                st.error(f"EXCEPTION while uploading file {e}")

        elif save == "Save to CSV locally":
            print(type(dataframe))
            dataframe.to_csv(f"/tmp/{filename}", index=False)
            try:
                st.download_button(
                    label="File ready. Click to download",
                    data="trees",
                    file_name=f"/tmp/{filename}",
                    mime="text/plain",
                )
            except Exception as e:
                st.error(f"EXCEPTION while saving file {e}")


if __name__ == "__main__":
    main()
