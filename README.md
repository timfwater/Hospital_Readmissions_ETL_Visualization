# 🏥 **Hospital Readmissions — ETL & Visualization**


📌 **Overview**

This project builds a full AWS-based ETL pipeline for analyzing U.S. hospital readmissions data.
It converts raw CMS datasets into analytics-ready formats (Parquet), registers schemas in Glue/Athena for SQL queries, produces a curated Gold dataset, and publishes a CSV snapshot that powers a Streamlit dashboard.

End-to-end goal: Deliver a one-page, high-level dashboard of U.S. hospital readmissions, backed by a robust, reproducible data pipeline.

⚙️ **Architecture**

Workflow at a glance:

Orchestration – ECS/Fargate runs run_full_pipeline.py

📊 **Source Datasets**

General Hospital Info (JSON) – location, ownership, CMS star rating, etc.

Readmissions Data (CSV) – category-specific readmission metrics.

Join Key: Provider ID.

🚀 **Running the Pipeline**

One-click Fargate deploy & run:

./fargate_deployment/deploy_to_fargate.sh
./run_fargate_task.sh

Or run locally against AWS (requires config.env):

python run_full_pipeline.py

📈 **Results (Highlights)**

Regional variation: Lower excess readmissions in the North/Midwest; higher in parts of the South.

Star ratings: Hospitals with higher CMS Star Ratings tend to have fewer readmissions.

Ownership: Physician-run hospitals often outperform federal/proprietary hospitals, though effects vary.

🛠️ **Implementation Notes**

All scripts are EMR-friendly (S3A + AWS default credentials).

Silver → Parquet + Glue schemas = schema stability & efficient Athena scans.

Gold dataset is partitioned by state for query performance.

CSV snapshot uses coalesce(1) for convenience; future scaling may use multi-file reads.

```
📂 Repo Structure

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
```
**Streamlit Dashboard:** 
https://hospitalreadmissionsetlvisualization-ehd6voqwz3eiuuqefpq6mm.streamlit.app/
**Website:** 
https://wbst-bkt.s3.amazonaws.com/index_ETL.html