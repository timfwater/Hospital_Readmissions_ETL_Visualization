import os
import io
import boto3
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

# --- Secrets & AWS creds support for Streamlit Cloud ---
if "aws_access_key_id" in st.secrets:
    os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["aws_access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["aws_secret_access_key"]
    if "aws_region" in st.secrets:
        os.environ["AWS_DEFAULT_REGION"] = st.secrets["aws_region"]

# --- Load environment variables (local dev convenience) ---
# Try repo root first, then fargate_deployment/config.env
if os.path.exists("config.env"):
    load_dotenv(dotenv_path="config.env")
elif os.path.exists("fargate_deployment/config.env"):
    load_dotenv(dotenv_path="fargate_deployment/config.env")

# --- Streamlit Page Config ---
st.set_page_config(page_title="Hospital Readmissions Dashboard", layout="wide")

# --- Config ---
DEFAULT_BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
PREFIX = os.getenv("FINAL_OUTPUT_PREFIX", "final_merged_output/")

# --- Load data from S3 using boto3 ---
@st.cache_data(ttl=300)  # auto-refresh every 5 minutes
def load_latest_csv(bucket_name: str, prefix: str):
    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

    # Paginate to be safe if there are many files
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    csv_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and obj["Size"] > 0:
                csv_files.append(obj)

    if not csv_files:
        return pd.DataFrame(), None

    latest_file = max(csv_files, key=lambda x: x["LastModified"])
    latest_key = latest_file["Key"]

    obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df, latest_key

# --- Controls ---
st.sidebar.header("Settings")
bucket_name = st.sidebar.text_input("S3 Bucket", value=DEFAULT_BUCKET)
prefix = st.sidebar.text_input("S3 Prefix", value=PREFIX)
if st.sidebar.button("🔄 Refresh data now"):
    load_latest_csv.clear()

# --- Load data ---
try:
    df, key = load_latest_csv(bucket_name, prefix)
except Exception as e:
    st.error(f"Failed to load data from S3: {e}")
    st.stop()

if df.empty:
    st.warning("No data available in S3.")
    st.stop()

st.caption(f"Loaded from **s3://{bucket_name}/{key}**")

# --- Define dynamic options ---
group_options = ["Hosp Ownership", "Hosp Rating"]
metric_options = [
    "R Rate Diff", "Excess Readm Ratio", "Exp Readm Rate",
    "Pred Readm Rate", "Discharges", "Readmissions"
]

# --- Sidebar Filters ---
st.sidebar.header("🔎 Filter the Data")
states = st.sidebar.multiselect("Select State(s):", sorted(df["state"].dropna().unique()))
readm_types = st.sidebar.multiselect("Select Readmission Type(s):", sorted(df["Readm Type"].dropna().unique()))
group_by = st.sidebar.selectbox("Group By:", group_options)
kpi_metric = st.sidebar.selectbox("KPI Metric:", metric_options)

# --- Apply Filters ---
filtered_df = df.copy()
if states:
    filtered_df = filtered_df[filtered_df["state"].isin(states)]
if readm_types:
    filtered_df = filtered_df[filtered_df["Readm Type"].isin(readm_types)]

# --- Main Dashboard ---
st.title("🏥 Hospital Readmissions Executive Dashboard")

# --- Top KPIs ---
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric(f"Avg {kpi_metric}", f"{filtered_df[kpi_metric].mean():.2f}" if not filtered_df.empty else "N/A")
kpi2.metric("Total Hospitals", filtered_df["provider_id"].nunique() if "provider_id" in filtered_df.columns else "N/A")
kpi3.metric("Total States", filtered_df["state"].nunique() if "state" in filtered_df.columns else "N/A")

# --- Grouped Bar Chart ---
st.subheader(f"📊 Average {kpi_metric} by {group_by}")
if group_by in filtered_df.columns and kpi_metric in filtered_df.columns:
    bar_data = filtered_df.groupby(group_by, dropna=True)[kpi_metric].mean().reset_index()
    bar_fig = px.bar(bar_data, x=group_by, y=kpi_metric, color=group_by, title=f"{kpi_metric} by {group_by}")
    st.plotly_chart(bar_fig, use_container_width=True)

# --- Histogram ---
st.subheader(f"📉 Distribution of {kpi_metric}")
if kpi_metric in filtered_df.columns:
    hist_fig = px.histogram(filtered_df, x=kpi_metric, nbins=20)
    st.plotly_chart(hist_fig, use_container_width=True)

# --- Choropleth Map ---
st.subheader(f"🗺️ {kpi_metric} by State")
if "state" in filtered_df.columns and kpi_metric in filtered_df.columns:
    state_avg = filtered_df.groupby("state", dropna=True)[kpi_metric].mean().reset_index()
    map_fig = px.choropleth(
        state_avg,
        locations="state",
        locationmode="USA-states",
        color=kpi_metric,
        scope="usa",
        color_continuous_scale="Reds",
        title=f"Average {kpi_metric} by State"
    )
    st.plotly_chart(map_fig, use_container_width=True)

# --- Display Table ---
st.subheader("🧾 Full Filtered Data")
st.dataframe(filtered_df.reset_index(drop=True))
