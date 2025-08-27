# Hospital Readmissions ETL & Dashboard

**Live app:** https://hospitalreadmissionsetlvisualization-ehd6voqwz3eiuuqefpq6mm.streamlit.app/

## What this project shows
- ECS/Fargate orchestrates
- EMR (Spark) transforms raw CMS data → Silver (Parquet) → Gold (partitioned Parquet)
- Athena/Glue provides SQL over S3
- Streamlit dashboard visualizes a CSV snapshot (auto-updated)

## Architecture
1. Fargate runs `run_full_pipeline.py`
2. Scripts sync to S3, EMR runs Spark steps (08/27/25)
3. Gold written to `s3://glue-hospital-data/athena/gold/merged/` (partitioned by state)
4. CSV snapshot to `s3://glue-hospital-data/final_merged_output/`
5. Streamlit app reads the latest CSV

## Run the pipeline (one way)
```bash
./fargate_deployment/deploy_to_fargate.sh
./run_fargate_task.sh
