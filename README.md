# 🏥 Hospital Readmissions --- ETL & Visualization

## 📌 Overview

This project implements an end-to-end, AWS-based ETL and analytics
pipeline for U.S. hospital readmissions data. It converts raw CMS source
files into optimized Parquet datasets, registers schemas in AWS
Glue/Athena, produces a curated **Gold dataset**, and publishes an
analytics snapshot that powers a Streamlit dashboard.

**End‑to‑end goal:** deliver a one‑page, executive‑level dashboard of
U.S. hospital readmissions backed by a reproducible cloud pipeline.

------------------------------------------------------------------------

## ⚙️ Architecture (High Level)

-   **Ingestion:** CMS General Hospital Information (JSON) and
    Readmissions Reduction Program (CSV)
-   **Transformation:** PySpark jobs write partitioned Parquet datasets
    to S3 (**Silver layer**)
-   **Cataloging:** AWS Glue crawlers / Athena DDL register external
    tables
-   **Gold Layer:** Merge and clean using Provider ID; publish
    partitioned Gold dataset (by state)
-   **Serving:** CSV snapshot powers Streamlit dashboard
-   **Orchestration:** ECS Fargate runs `run_full_pipeline.py` for
    one‑click execution

------------------------------------------------------------------------

## 📊 Source Datasets

-   **General Hospital Information (JSON):** location, ownership,
    ratings, metadata\
-   **Readmissions Reduction Program (CSV):** measure‑specific
    readmission metrics

**Join key:** Provider ID

------------------------------------------------------------------------

## 🚀 How to Run

### ✅ Option A --- One‑click Fargate deployment

\`\`\`bash ./fargate_deployment/deploy_to_fargate.sh
./run_fargate_task.sh \`\`\`

### ✅ Option B --- Local execution against AWS

\`\`\`bash python run_full_pipeline.py \`\`\`

> Requires properly configured AWS credentials and `config.env`

------------------------------------------------------------------------

## 📈 Key Results & Insights

-   Regional variation: lower excess readmissions in North/Midwest;
    higher in parts of the South
-   Higher CMS star ratings typically correlate with fewer readmissions
-   Physician‑owned hospitals often outperform federal/proprietary
    hospitals (varies by state)

------------------------------------------------------------------------

## 🛠️ Engineering & Implementation Notes

-   Scripts are **EMR‑compatible** (S3A filesystem + default credential
    provider chain)
-   Parquet + Glue schemas enable **efficient Athena scans**
-   Gold dataset is **partitioned by state** for query performance and
    cost reduction
-   CSV snapshot temporarily uses `coalesce(1)` for convenience

------------------------------------------------------------------------

## 🔍 Validation & Data Quality

-   Row‑count validation after each stage (Raw → Silver → Gold)
-   Schema checks for required columns and Provider ID integrity
-   Null‑rate checks on key measures
-   State‑level sanity‑check aggregates

------------------------------------------------------------------------

## 🔐 Security & Cost Considerations

-   Least‑privilege IAM policies for ECS task role and S3 access
-   Parquet + partition pruning minimize Athena scan cost
-   Auto‑terminating compute patterns recommended for EMR/Spark
    workloads

------------------------------------------------------------------------

## 🧭 Troubleshooting Tips

-   **Zero rows in Athena?** Ensure crawler/DDL ran after Parquet writes
-   **ECS task can't pull images?** Enable public IP or ensure NAT
    access
-   **Streamlit errors on missing columns?** Refresh Gold snapshot

------------------------------------------------------------------------

## 📂 Repository Structure

├── Dockerfile
├── Hospital_Readmissions_Executive_Dashboard.twbx   # Tableau dashboard (legacy)
├── README.md
├── athena_create_tables.sql                         # Glue/Athena DDLs
├── data/                                            # Sample/landing data
├── fargate_deployment/                              # Deployment automation
│   ├── config.env
│   ├── deploy_to_fargate.sh
│   ├── ecs-trust-policy.json
│   ├── final-task-def.json
│   ├── generate_task_def.py
│   ├── s3-access-policy.json
│   ├── setup_iam.sh
│   └── task-def-template.json
├── requirements.txt
├── run_fargate_task.sh
├── run_full_pipeline.py
├── scripts/
│   ├── 01_gen_info_json_to_parquet.py
│   ├── 02_readmissions_csv_to_parquet.py
│   └── 03_merge_from_database.py
└── streamlit/
    └── streamlit_app.py

------------------------------------------------------------------------

## 🌐 Live Resources

-   **Streamlit Dashboard:**
    hospitalreadmissionsetlvisualization-ehd6voqwz3eiuuqefpq6mm.streamlit.app
-   **Project Website:** wbst-bkt.s3.amazonaws.com/index_ETL.html
