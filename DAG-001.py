# import all modules, libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# variables section
PROJECT_ID = "festive-dolphin-430515-n1"
DATASET_NAME_1 = "raw_data"
DATASET_NAME_2 = "final_data"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_final"
LOCATION = "US"
INSERT_ROWS_QUERY = f"""
CREATE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT 
  e.EmployeeID,
  CONCAT(e.FirstName,".",e.LastName) AS FullName,
  e.Email,
  e.Salary,
  e.JoinDate,
  d.DepartmentID,
  d.DepartmentName,
  CAST(e.Salary AS INTEGER)*0.01 as Tax
FROM  
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID is not null
"""


ARGS = {
    "owner" : "Onkar Shitole",
    "start_date" : datetime(2024,9,11),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}


# define the dag
with DAG(
    "DAG-001A",
    schedule_interval = "35 11 * * 3",
    default_args = ARGS
) as dag:
    
    
# define tasks : 
    task_1 = GCSToBigQueryOperator(
        task_id="emp_task",
        bucket="ora-migration_01",
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_2 = GCSToBigQueryOperator(
        task_id="dep_task",
        bucket="ora-migration_01",
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
        task_id="emp_dep_task",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

# define dependency
(task_1,task_2) >> task_3