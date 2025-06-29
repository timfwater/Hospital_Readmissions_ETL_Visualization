import boto3
import time

athena = boto3.client('athena', region_name='us-east-1')

DATABASE = 'hospital_readmissions'
OUTPUT = 's3://glue-hospital-data/athena_query_results/'  # Must exist

def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    return response['QueryExecutionId']

def wait_for_query(qid):
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            print(f"Query finished with status: {status}")
            break
        time.sleep(2)

# Refresh tables (re-point to updated Parquet data)
for table in ['gen_info', 'readmissions']:
    print(f"🔄 Refreshing Athena table: {table}")
    qid = run_athena_query(f"MSCK REPAIR TABLE {table}")
    wait_for_query(qid)
