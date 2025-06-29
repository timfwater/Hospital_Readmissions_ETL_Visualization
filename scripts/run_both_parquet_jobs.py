import subprocess

print("🚀 Running 01_gen_info_json_to_parquet.py...")
subprocess.run(["spark-submit", "01_gen_info_json_to_parquet.py"], check=True)

print("🚀 Running 02_readmissions_csv_to_parquet.py...")
subprocess.run(["spark-submit", "02_readmissions_csv_to_parquet.py"], check=True)
