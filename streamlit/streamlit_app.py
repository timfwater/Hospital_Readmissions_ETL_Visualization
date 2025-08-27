# streamlit/streamlit_app.py
import os
import io
from datetime import timezone
import boto3
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

# --- Streamlit page config ---
st.set_page_config(page_title="Hospital Readmissions Dashboard", layout="wide")

# --- Secrets & AWS creds support for Streamlit Cloud ---
if "aws_access_key_id" in st.secrets:
    os.environ["AWS_ACCESS_KEY_ID"] = st.secrets["aws_access_key_id"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = st.secrets["aws_secret_access_key"]
    if "aws_region" in st.secrets:
        os.environ["AWS_DEFAULT_REGION"] = st.secrets["aws_region"]

# --- Load env for local dev (try root, then fargate_deployment/) ---
if os.path.exists("config.env"):
    load_dotenv("config.env")
elif os.path.exists("fargate_deployment/config.env"):
    load_dotenv("fargate_deployment/config.env")

# --- App config defaults ---
DEFAULT_BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
DEFAULT_PREFIX = os.getenv("FINAL_OUTPUT_PREFIX", "final_merged_output/")

# ---------- Helpers ----------
def fmt_num(x):
    """Nice number formatting for KPIs."""
    try:
        if pd.isna(x):
            return "N/A"
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

@st.cache_data(ttl=300)  # auto-refresh every 5 minutes
def load_latest_csv(bucket_name: str, prefix: str):
    """Return (df, key, last_modified) of latest non-empty CSV under prefix."""
    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    csv_objs = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv") and obj["Size"] > 0:
                csv_objs.append(obj)

    if not csv_objs:
        return pd.DataFrame(), None, None

    latest_obj = max(csv_objs, key=lambda o: o["LastModified"])
    key = latest_obj["Key"]
    lastmod = latest_obj["LastModified"]

    body = s3.get_object(Bucket=bucket_name, Key=key)["Body"].read()
    df = pd.read_csv(io.BytesIO(body))
    return df, key, lastmod

# ---------- Sidebar / Controls ----------
st.sidebar.header("Settings")
bucket_name = st.sidebar.text_input("S3 Bucket", value=DEFAULT_BUCKET)
prefix = st.sidebar.text_input("S3 Prefix", value=DEFAULT_PREFIX)

# Manual refresh button (clears cache)
if st.sidebar.button("🔄 Refresh data now"):
    load_latest_csv.clear()

# ---------- Load data ----------
try:
    df, key, lastmod = load_latest_csv(bucket_name, prefix)
except Exception as e:
    st.error(f"Failed to load data from S3:\n\n{e}")
    st.stop()

if df.empty:
    st.warning("No data available in S3. Ensure your pipeline wrote a CSV to the configured prefix.")
    st.stop()

# Header + provenance
st.title("🏥 Hospital Readmissions Executive Dashboard")
if key and lastmod:
    st.caption(
        f"Loaded **s3://{bucket_name}/{key}** • "
        f"Last updated **{lastmod.astimezone(timezone.utc):%Y-%m-%d %H:%M UTC}**"
    )

# ---------- Filters ----------
group_options = ["Hosp Ownership", "Hosp Rating"]
metric_options = [
    "R Rate Diff", "Excess Readm Ratio", "Exp Readm Rate",
    "Pred Readm Rate", "Discharges", "Readmissions"
]

st.sidebar.header("🔎 Filter the Data")
states = st.sidebar.multiselect("Select State(s):", sorted(df["state"].dropna().unique()))
readm_types = st.sidebar.multiselect("Select Readmission Type(s):", sorted(df["Readm Type"].dropna().unique()))
group_by = st.sidebar.selectbox("Group By:", group_options)
kpi_metric = st.sidebar.selectbox("KPI Metric:", metric_options)

# Apply filters
filtered_df = df.copy()
if states:
    filtered_df = filtered_df[filtered_df["state"].isin(states)]
if readm_types:
    filtered_df = filtered_df[filtered_df["Readm Type"].isin(readm_types)]

# ---------- KPIs ----------
k1, k2, k3 = st.columns(3)
k1.metric(f"Avg {kpi_metric}", fmt_num(filtered_df[kpi_metric].mean()) if kpi_metric in filtered_df else "N/A")
k2.metric("Total Hospitals", int(filtered_df["provider_id"].nunique()) if "provider_id" in filtered_df else "N/A")
k3.metric("Total States", int(filtered_df["state"].nunique()) if "state" in filtered_df else "N/A")

# ---------- Charts ----------
# Grouped bar
st.subheader(f"📊 Average {kpi_metric} by {group_by}")
if group_by in filtered_df.columns and kpi_metric in filtered_df.columns:
    bar_data = (
        filtered_df.groupby(group_by, dropna=True)[kpi_metric]
        .mean()
        .reset_index()
        .sort_values(by=kpi_metric, ascending=False)
    )
    bar_fig = px.bar(
        bar_data, x=group_by, y=kpi_metric, color=group_by,
        title=f"{kpi_metric} by {group_by}",
        hover_data={group_by: True, kpi_metric: ':.2f'}
    )
    st.plotly_chart(bar_fig, use_container_width=True)

# Histogram
st.subheader(f"📉 Distribution of {kpi_metric}")
if kpi_metric in filtered_df.columns:
    hist_fig = px.histogram(
        filtered_df, x=kpi_metric, nbins=20,
        title=f"Distribution of {kpi_metric}"
    )
    st.plotly_chart(hist_fig, use_container_width=True)

# Choropleth
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
        title=f"Average {kpi_metric} by State",
        hover_data={"state": True, kpi_metric: ':.2f'}
    )
    st.plotly_chart(map_fig, use_container_width=True)

# ---------- Table ----------
st.subheader("🧾 Full Filtered Data")
st.dataframe(filtered_df.reset_index(drop=True))
