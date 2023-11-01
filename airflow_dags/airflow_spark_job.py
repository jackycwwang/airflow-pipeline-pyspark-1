from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_success': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("gcp_dataproc_spark_job",
          default_args=default_args,
          description="A DAG to run Spark job on Dataproc",
          schedule_interval=timedelta(days=1),
          start_date=days_ago(1),
          tags=['project 1'],
          )

# Configure cluster parameters
CLUSTER_NAME = 'spark-cluster-project-1'
PROJECT_ID = 'jacky-demo'       # this is project ID, not project NAME
REGION = 'us-east1'

# configuration for cluster creation
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',    # primary disk type
            'boot_disk_size_gb': 50
        }
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 32
        }
    },
    'software_config': {
        'image_version': '2.1-debian11'
    }
}
create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

pyspark_job = {
    'main_python_file_uri': 'gs://landing-zone-1/emp_batch_job.py'
}
submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id="submit_pyspark_job",
    # the entry point pyspark file to be submitted as the driver program
    main=pyspark_job['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',    # ensures cluster deletion even if the spark job fails
    dag=dag,
)


create_cluster >> submit_pyspark_job >> delete_cluster
