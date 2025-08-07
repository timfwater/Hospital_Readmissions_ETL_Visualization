import os
import io
import boto3
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

# --- Load environment variables from config.env ---
load_dotenv(dotenv_path="config.env")

# --- Streamlit Page Config ---
st.set_page_config(page_title="Hospital Readmissions Dashboard", layout="wide")

# --- Load data from S3 using boto3 ---
@st.cache_data
def load_data():
    bucket_name = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
    prefix = "final_merged_output/"

    s3 = boto3.client("s3")

    # Get list of all CSV files
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv") and obj["Size"] > 0
    ]

    if not csv_files:
        st.error("No CSV files found in S3.")
        return pd.DataFrame()

    # Sort by LastModified to get most recent file
    latest_file = sorted(csv_files, key=lambda x: x["LastModified"], reverse=True)[0]
    latest_key = latest_file["Key"]

    obj = s3.get_object(Bucket=bucket_name, Key=latest_key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

# --- Load data and handle errors ---
df = load_data()
if df.empty:
    st.warning("No data available for display.")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("🔎 Filter the data")
states = st.sidebar.multiselect("Select State(s):", sorted(df["state"].dropna().unique()))
drg_codes = st.sidebar.multiselect("Select DRG(s):", sorted(df["drg_code"].dropna().unique()))
hospitals = st.sidebar.multiselect("Select Hospital(s):", sorted(df["hospital_name"].dropna().unique()))

# --- Apply Filters ---
filtered_df = df.copy()
if states:
    filtered_df = filtered_df[filtered_df["state"].isin(states)]
if drg_codes:
    filtered_df = filtered_df[filtered_df["drg_code"].isin(drg_codes)]
if hospitals:
    filtered_df = filtered_df[filtered_df["hospital_name"].isin(hospitals)]

# --- KPI Metrics ---
st.title("🏥 Hospital Readmissions Dashboard")
kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Average Readmission Rate", f"{filtered_df['readmission_rate'].mean():.2f}%" if not filtered_df.empty else "N/A")
kpi2.metric("Total Hospitals", filtered_df["hospital_name"].nunique())
kpi3.metric("Total DRG Codes", filtered_df["drg_code"].nunique())

# --- Choropleth Map ---
st.subheader("📍 Readmission Rates by State")
if "state" in filtered_df.columns and not filtered_df.empty:
    state_map = px.choropleth(
        filtered_df,
        locations="state",
        locationmode="USA-states",
        color="readmission_rate",
        scope="usa",
        color_continuous_scale="Reds",
        title="Average Readmission Rate by State"
    )
    st.plotly_chart(state_map, use_container_width=True)

# --- Distribution Chart ---
st.subheader("📊 Readmission Rate Distribution")
if not filtered_df.empty:
    dist_fig = px.histogram(filtered_df, x="readmission_rate", nbins=20, title="Distribution of Readmission Rates")
    st.plotly_chart(dist_fig, use_container_width=True)

# --- Display Data Table ---
st.subheader("🧾 Filtered Data")
st.dataframe(filtered_df.reset_index(drop=True))
