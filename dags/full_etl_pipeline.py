from datetime import datetime
import os
import time

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

ENV_FILE = "/home/azureuser/airflow/.databricks_env"
JOB_ID = 705785191129716  # Replace with your workspace's job ID if different


def load_credentials():
    """Load Databricks settings from a protected file outside the repository."""
    with open(ENV_FILE, encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


def databricks_headers():
    load_credentials()
    return {
        "Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}",
    }


def trigger_databricks():
    """Trigger the existing Databricks job and return its run ID."""
    load_credentials()
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=databricks_headers(),
        json={"job_id": JOB_ID},
        timeout=30,
    )
    response.raise_for_status()
    run_id = response.json()["run_id"]
    print(f"Triggered Databricks job; run_id={run_id}")
    return run_id


def wait_for_databricks(ti):
    """Poll the Databricks run and fail this task if the job fails."""
    load_credentials()
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    run_id = ti.xcom_pull(task_ids="trigger_databricks")
    if not run_id:
        raise RuntimeError("No Databricks run ID found in XCom.")

    deadline = time.monotonic() + 4 * 60 * 60
    while time.monotonic() < deadline:
        response = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=databricks_headers(),
            params={"run_id": run_id},
            timeout=30,
        )
        response.raise_for_status()
        state = response.json().get("state", {})
        lifecycle = state.get("life_cycle_state")
        result = state.get("result_state")
        print(f"Databricks lifecycle={lifecycle}, result={result}")

        if lifecycle == "TERMINATED":
            if result == "SUCCESS":
                print("Databricks job succeeded.")
                return
            raise RuntimeError(f"Databricks job did not succeed: {state}")

        if lifecycle in {"INTERNAL_ERROR", "SKIPPED"}:
            raise RuntimeError(f"Databricks run stopped unexpectedly: {state}")

        time.sleep(30)

    raise TimeoutError(f"Timed out waiting for Databricks run {run_id}")


with DAG(
    dag_id="full_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "azure", "snowflake", "etl"],
    doc_md="Manually triggered sales ETL orchestration through Databricks.",
) as dag:
    trigger = PythonOperator(
        task_id="trigger_databricks",
        python_callable=trigger_databricks,
    )

    wait = PythonOperator(
        task_id="wait_for_databricks",
        python_callable=wait_for_databricks,
        execution_timeout=__import__("datetime").timedelta(hours=4, minutes=5),
    )

    validate = BashOperator(
        task_id="validate_data",
        bash_command="bash /home/azureuser/airflow/scripts/task3_validate.sh ",
    )

    finish = BashOperator(
        task_id="finish_pipeline",
        bash_command="bash /home/azureuser/airflow/scripts/task4_finish.sh ",
    )

    trigger >> wait >> validate >> finish
