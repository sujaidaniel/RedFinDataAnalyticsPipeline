from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

# Default arguments for the DAG
default_args = {
    'owner': 'PipelineAdmin',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 26),
    'email': ['data_pipeline_alerts@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# EMR cluster configuration
cluster_config = {
    "Name": "real_estate_data_pipeline_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://real-estate-data/emr-logs/",
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-123abc456def",
        "Ec2KeyName": "emr-access-key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# Steps for data extraction and transformation
extraction_steps = [
    {
        "Name": "Extract Real Estate Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": [
                "s3://real-estate-data/scripts/data_ingestion.sh",
            ],
        },
    },
]

transformation_steps = [
    {
        "Name": "Transform Real Estate Data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://real-estate-data/scripts/data_transformation.py",
            ],
        },
    },
]

# Define the DAG
with DAG(
    'real_estate_data_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    # Define tasks
    start_pipeline = DummyOperator(task_id="start_pipeline")

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=cluster_config,
    )

    monitor_cluster_creation = EmrJobFlowSensor(
        task_id="monitor_cluster_creation",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        target_states={"WAITING"},
        timeout=3600,
        poke_interval=10,
    )

    add_extraction_steps = EmrAddStepsOperator(
        task_id="add_extraction_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        steps=extraction_steps,
    )

    monitor_extraction = EmrStepSensor(
        task_id="monitor_extraction",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_extraction_steps')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
    )

    add_transformation_steps = EmrAddStepsOperator(
        task_id="add_transformation_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        steps=transformation_steps,
    )

    monitor_transformation = EmrStepSensor(
        task_id="monitor_transformation",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_transformation_steps')[0] }}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    )

    monitor_cluster_termination = EmrJobFlowSensor(
        task_id="monitor_cluster_termination",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        target_states={"TERMINATED"},
        timeout=3600,
        poke_interval=10,
    )

    end_pipeline = DummyOperator(task_id="end_pipeline")

    # Define task dependencies
    (
        start_pipeline
        >> create_cluster
        >> monitor_cluster_creation
        >> add_extraction_steps
        >> monitor_extraction
        >> add_transformation_steps
        >> monitor_transformation
        >> terminate_cluster
        >> monitor_cluster_termination
        >> end_pipeline
    )
