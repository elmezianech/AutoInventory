import streamlit as st
import boto3
import pandas as pd

s3_client = boto3.client('s3')
bucket_name = 'fmcg-data-lake'

# Upload test data
st.title("FMCG Data Analysis")
uploaded_file = st.file_uploader("Upload test data", type=["csv"])
if uploaded_file:
    s3_client.put_object(Bucket=bucket_name, Key=f"test/{uploaded_file.name}", Body=uploaded_file.getvalue())
    st.success(f"Uploaded {uploaded_file.name} to S3")

# Download processed results
if st.button("Download Processed Data"):
    obj = s3_client.get_object(Bucket=bucket_name, Key="processed/processed_data.csv")
    st.download_button(
        label="Download Processed Data",
        data=obj['Body'].read(),
        file_name="processed_data.csv",
        mime="text/csv",
    )
