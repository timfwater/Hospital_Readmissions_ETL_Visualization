# streamlit/streamlit_app.py
import os, io
from datetime import timezone
import boto3, pandas as pd, streamlit as st, plotly.express as px

st.set_page_config(page_title="Hospital Readmissions Dashboard", layout="wide")

# ---------------- Credentials (safe for local + Cloud) ----------------
# Prefer environment (works with ~/.aws/credentials or env vars).
aws_key    = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# If secrets exist on Streamlit Cloud, override per-key (each access wrapped).
try:
    _s = st.secrets  # may raise if no secrets configured (local run)
    try:    aws_key    = _s["aws_access_key_id"]
    except: pass
    try:    aws_secret = _s["aws_secret_access_key"]
    except: pass
    try:    aws_region = _s["aws_region"]
    except: pass
except Exception:
    pass  # no secrets file locally = fine

if aws_key and aws_secret:
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
os.environ.setdefault("AWS_DEFAULT_REGION", aws_region)

# ---------------- Data location (same as your pipeline) ----------------
BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
PREFIX = os.getenv("FINAL_OUTPUT_PREFIX", "final_merged_output/")

# ---------------- Hard schema (EXACT column names you want) ------------
EXPECTED = [
    "State",
    "Provider ID",
    "Readmission Type",
    "Number of Discharges",
    "Number of Readmissions",
    "Excess Readmission Ratio",
    "Predicted Readmission Rate",
    "Expected Readmission Rate",
    "Hospital Ownership",
    "Hospital Overall Rating",
    "Readmission Rate Difference",
]

# Convenience aliases (exact strings to avoid typos below)
COL_STATE   = "State"
COL_PID     = "Provider ID"
COL_RTYPE   = "Readmission Type"
COL_DISCH   = "Number of Discharges"
COL_READM   = "Number of Readmissions"
COL_EXCESS  = "Excess Readmission Ratio"
COL_PRED    = "Predicted Readmission Rate"
COL_EXP     = "Expected Readmission Rate"
COL_OWNER   = "Hospital Ownership"
COL_RATING  = "Hospital Overall Rating"
COL_DIFF    = "Readmission Rate Difference"

def fmt_num(x):
    try:
        if pd.isna(x): return "N/A"
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def load_latest_from_s3():
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX)
    objs = []
    for p in pages:
        for o in p.get("Contents", []):
            if o["Key"].lower().endswith(".csv") and o["Size"] > 0:
                objs.append(o)
    if not objs:
        return pd.DataFrame(), None, None
    latest = max(objs, key=lambda o: o["LastModified"])
    key, lastmod = latest["Key"], latest["LastModified"]
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    # engine="python" allows sep=None (sniffs commas/semicolons/tabs)
    df = pd.read_csv(io.BytesIO(body), encoding="utf-8-sig", sep=None, engine="python")
    return df, key, lastmod

# ---------------- App ----------------
st.title("🏥 Hospital Readmissions Executive Dashboard")

# Tip for Cloud users with private buckets
if not (aws_key and aws_secret):
    st.info("If running on Streamlit Cloud with a private S3 bucket, add read-only AWS keys under **Settings → Secrets**.")

try:
    df, key, lastmod = load_latest_from_s3()
except Exception as e:
    st.error(f"Could not load data from s3://{BUCKET}/{PREFIX}\n\n{e}")
    st.stop()

if df.empty:
    st.warning("No CSVs found under the expected S3 prefix.")
    st.stop()

# Strict schema check (exact names; no aliasing)
missing = [c for c in EXPECTED if c not in df.columns]
if missing:
    st.error(
        "The dataset does not match the required schema the app expects.\n\n"
        f"**Missing columns:** {missing}\n\n"
        f"**Found columns:** {list(df.columns)}\n\n"
        "Either update your gold layer to emit the required headers exactly as listed, "
        "or modify EXPECTED in this file to your final chosen standard."
    )
    st.stop()

if key and lastmod:
    st.caption(f"Loaded **s3://{BUCKET}/{key}** • Last updated **{lastmod.astimezone(timezone.utc):%Y-%m-%d %H:%M UTC}**")

# ---------------- Controls ----------------
group_options = [COL_OWNER, COL_RATING]
metric_options = [COL_DIFF, COL_EXCESS, COL_EXP, COL_PRED, COL_DISCH, COL_READM]

st.sidebar.header("Filters")
states = st.sidebar.multiselect("State(s)", sorted(df[COL_STATE].dropna().unique()))
readm_types = st.sidebar.multiselect("Readmission Type(s)", sorted(df[COL_RTYPE].dropna().unique()))
group_by = st.sidebar.selectbox("Group by", group_options)
kpi_metric = st.sidebar.selectbox("KPI metric", metric_options)

fdf = df.copy()
if states:
    fdf = fdf[fdf[COL_STATE].isin(states)]
if readm_types:
    fdf = fdf[fdf[COL_RTYPE].isin(readm_types)]

# ---------------- KPIs ----------------
c1, c2, c3 = st.columns(3)
c1.metric(f"Avg {kpi_metric}", fmt_num(fdf[kpi_metric].mean()))
c2.metric("Total Hospitals", int(fdf[COL_PID].nunique()))
c3.metric("Total States", int(fdf[COL_STATE].nunique()))

# ---------------- Charts ----------------
st.subheader(f"📊 Average {kpi_metric} by {group_by}")
bar_data = (
    fdf.groupby(group_by, dropna=True)[kpi_metric]
    .mean()
    .reset_index()
    .sort_values(by=kpi_metric, ascending=False)
)
st.plotly_chart(px.bar(bar_data, x=group_by, y=kpi_metric, color=group_by,
                       title=f"{kpi_metric} by {group_by}"),
                use_container_width=True)

st.subheader(f"📉 Distribution of {kpi_metric}")
st.plotly_chart(px.histogram(fdf, x=kpi_metric, nbins=20, title=f"Distribution of {kpi_metric}"),
                use_container_width=True)

# Choropleth (expects 2-letter state abbreviations)
st.subheader(f"🗺️ {kpi_metric} by State")
state_vals = fdf[COL_STATE].astype(str).str.upper().str.strip()
if state_vals.str.fullmatch(r"[A-Z]{2}").all():
    map_df = pd.DataFrame({COL_STATE: state_vals, kpi_metric: fdf[kpi_metric]})
    state_avg = map_df.groupby(COL_STATE, dropna=True)[kpi_metric].mean().reset_index()
    st.plotly_chart(px.choropleth(
        state_avg, locations=COL_STATE, locationmode="USA-states",
        color=kpi_metric, scope="usa", title=f"Average {kpi_metric} by State"),
        use_container_width=True
    )
else:
    st.info("Map skipped: 'State' must contain 2-letter state abbreviations for the USA choropleth.")

# ---------------- Table ----------------
st.subheader("🧾 Full Data")
st.dataframe(fdf.reset_index(drop=True))
