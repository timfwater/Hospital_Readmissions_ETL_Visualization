import streamlit as st
import pandas as pd
import boto3
from io import StringIO

# === CONFIGURATION ===
bucket_name = "glue-hospital-data"
prefix = "final_merged_output/"
file_extension = ".csv"

# === GET LATEST CSV FROM S3 ===
def get_latest_csv_key(bucket, prefix, extension=".csv"):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(extension)
    ]
    if not files:
        raise FileNotFoundError("No CSV files found in S3 prefix.")
    latest = max(files, key=lambda x: x["LastModified"])
    return latest["Key"]

# === LOAD CSV FROM S3 ===
def load_csv_from_s3(bucket, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_content))
    return df

# === STREAMLIT APP ===
st.title("📊 Hospital Readmissions Dashboard")

try:
    latest_key = get_latest_csv_key(bucket_name, prefix, file_extension)
    df = load_csv_from_s3(bucket_name, latest_key)
    st.success(f"Loaded data from: `{latest_key}`")

    st.subheader("🔍 Data Preview")
    st.dataframe(df.head())

    st.subheader("📊 Column Info")
    st.write(df.dtypes)

    # Optional Filters
    if "provider_name" in df.columns:
        selected_provider = st.selectbox("Filter by Provider", df["provider_name"].unique())
        st.dataframe(df[df["provider_name"] == selected_provider])

    if "readmission_rate" in df.columns:
        st.subheader("📈 Readmission Rate Distribution")
        st.bar_chart(df["readmission_rate"])

except Exception as e:
    st.error(f"Error loading or displaying data: {e}")
