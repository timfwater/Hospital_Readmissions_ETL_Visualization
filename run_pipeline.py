import subprocess

scripts = [
    "scripts/Script_1_gen_info_json_to_csv.py",
    "scripts/Script_2_readm_parq_to_csv.py",
    "scripts/Script_3_merged_recreated_hosp.py"
]

for script in scripts:
    print(f"🔧 Running {script}...")
    result = subprocess.run(["python", script])
    if result.returncode != 0:
        print(f"❌ Failed on {script}")
        break
print("✅ ETL Pipeline complete.")
