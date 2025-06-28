import subprocess

scripts = [
    "scripts/01_gen_info_json_to_csv.py",
    "scripts/02_readmissions_parquet_to_csv.py",
    "scripts/03_merge_and_clean_hospital_data.py"
]

for script in scripts:
    print(f"🔧 Running {script}...")
    result = subprocess.run(["python3", script])
    if result.returncode != 0:
        print(f"❌ Failed on {script}")
        break
print("✅ ETL Pipeline complete.")
