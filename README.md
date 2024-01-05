# Waterman_Projects
Data science work in healthcare and other domains

Diabetes Predictions (6 files):

Script_1_gen_info_json_to_csv.py: Converts source General Information file from JSON to CSV format

Script_2_merged_recreated_hosp.py: Converts source Readmissions Information file from Parquet to CSV format

Script_3_readm_parq_to_csv.py: Merges the two source files on "Provider ID", and engineers/cleans the data for BI Dashboard
   
Hospital_Readmissions_Executive_Dashboard.twbx: Provides an interactive BI Dashboard displaying KPIs across Hospital categories

ETL Diagram: Provides a visual representation of the workflow

General_Information_JSON.snappy: Source General Information dataset

Readmissions_Information_Parquet.parquet: Source Readmissions Information dataset
