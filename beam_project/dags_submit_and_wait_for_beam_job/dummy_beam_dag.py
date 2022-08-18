import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator,
)
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowConfiguration,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobStatusSensor,
)

DAG_ID = "dag-submit-and-wait-for-beam-job-completion"
env = os.environ.get("AIRFLOW_VAR_ENV")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 4, 2),
    "end_date": datetime(2022, 4, 3),
    "depends_on_past": False,
}

SERVICE_ACCOUNT = os.environ.get("AIRFLOW_VAR_SERVICE_ACCOUNT")
GCP_PROJECT_ID = "umg-de"

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule_interval="@once",
        max_active_runs=12,
        catchup=True,
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    submit_beam_job = BeamRunPythonPipelineOperator(
        task_id="submit_beam_job",
        py_file=os.path.join(
            "src",
            "dag_submit_and_wait_for_beam_job_completion",
            "dummy_beam_job.py",
        ),
        runner="DataflowRunner",
        pipeline_options={
            "sleep": 120,
            "service_account_email": SERVICE_ACCOUNT,
            "worker_machine_type": "n1-standard-2",
            "disk_size_gb": "100",
            "num_workers": "1",
        },
        dataflow_config=DataflowConfiguration(
            job_name=f"{env}_sleeping_beam_job",
            project_id=GCP_PROJECT_ID,
            location="us-central1",
            wait_until_finished=False,
        ),
        do_xcom_push=True,
    )

    wait_for_beam_job = DataflowJobStatusSensor(
        task_id="wait_for_beam_job",
        job_id=f"{{{{task_instance.xcom_pull('{submit_beam_job.task_id}')['dataflow_job_id']}}}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        location=submit_beam_job.dataflow_config.location,
        poke_interval=10,
        timeout=60 * 5,
        mode="reschedule",
        retries=0,
    )

    start >> submit_beam_job >> wait_for_beam_job >> end
