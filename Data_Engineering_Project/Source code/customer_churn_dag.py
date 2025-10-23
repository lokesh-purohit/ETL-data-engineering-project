from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor


def _glue_client():
    
    aws = AwsGenericHook(aws_conn_id="aws_s3_conn")
    session = aws.get_session(region_name="us-west-2")
    return session.client("glue")



def launch_glue_job(job_name: str, **_):
    
    glue = _glue_client()
    glue.start_job_run(JobName=job_name)


def fetch_latest_run_id(**_):
    
    time.sleep(8)  # small delay so the run appears in get_job_runs
    glue = _glue_client()
    resp = glue.get_job_runs(JobName="s3_upload_to_redshift_gluejob")
    return resp["JobRuns"][0]["Id"]



default_args = dict(
    owner="airflow",
    depends_on_past=False,
    start_date=datetime(2023, 8, 1),
    email=["myemail@domain.com"],
    email_on_failure=False,
    email_on_retry=False,
    retries=2,
    retry_delay=timedelta(seconds=15),
)

with DAG(
    dag_id="my_dag",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    trigger_glue = PythonOperator(
        task_id="tsk_glue_job_trigger",
        python_callable=launch_glue_job,
        op_kwargs={"job_name": "s3_upload_to_redshift_gluejob"},
    )

    get_run_id = PythonOperator(
        task_id="tsk_grab_glue_job_run_id",
        python_callable=fetch_latest_run_id,
    )

    wait_for_glue = GlueJobSensor(
        task_id="tsk_is_glue_job_finish_running",
        job_name="s3_upload_to_redshift_gluejob",
        run_id='{{ ti.xcom_pull("tsk_grab_glue_job_run_id") }}',
        verbose=True,            # stream Glue logs into Airflow logs
        aws_conn_id="aws_s3_conn",
        poke_interval=60,
        timeout=3600,
    )

    trigger_glue >> get_run_id >> wait_for_glue