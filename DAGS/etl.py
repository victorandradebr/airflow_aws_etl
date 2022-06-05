from emr_config import EMR_CONFIG

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator # Cria o cluster
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor # Observa e só avança se o cluster for criado com sucesso
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator # Rodar o script
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor # Observa e só avança se o job tiver rodado com sucesso
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator # Desliga o cluster

from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator # para rodar o glue crawler
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor # Observa e só avança se o status for sucesso

from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    'owner': 'Victor Andrade',
    'depends_on_past': False,
    'retries': 2
}

with DAG(
    dag_id="victor-andrade-etl-aws",
    tags=['etl', 'aws', 'dataengineer'],
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    concurrency=3,
    max_active_runs=1,
    catchup=False
) as dag:

################################################################# DUMMY AUX #################################################################################

    task_dummy = DummyOperator(
        task_id='task_dummy'
    )

################################################################# CRIA CLUSTER E OBSERVA A CRIAÇÃO #################################################################################

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CONFIG,
        aws_conn_id="aws",
        emr_conn_id="emr",
        region_name='us-east-1'
    )

    emr_create_sensor = EmrJobFlowSensor(
        task_id='monitoring_emr_cluster_creation',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        target_states=['WAITING'],
        failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
        aws_conn_id="aws"
    )

################################################################ RODA O PRIMEIRO SCRIPT E OBSERVA O STATUS #################################################################################

    steps_landing_to_processing = [{
        "Name": "LANDING_TO_PROCESSING",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
             "Args": [
                'spark-submit',
                's3://emr-tf-data-codes/landing_to_processing.py'
             ]
        }
    }] 

    task_landing_to_processing = EmrAddStepsOperator(
        task_id='task_landing_to_processing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=steps_landing_to_processing,
        aws_conn_id='aws',
        dag=dag
    )

    step_checker_landing_to_processing = EmrStepSensor(
        task_id=f'watch_task_landing_to_processing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='task_landing_to_processing', key='return_value')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
        aws_conn_id="aws",
        dag=dag
    )

################################################################ RODA O SEGUNDO SCRIPT E OBSERVA O STATUS #################################################################################

    steps_processing_to_curated = [{
        "Name": "PROCESSING_TO_CURATED",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
             "Args": [
                'spark-submit',
                's3://emr-tf-data-codes/processing_to_curated.py'
             ]
        }
    }]    


    task_processing_to_curated = EmrAddStepsOperator(
        task_id='task_processing_to_curated',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=steps_processing_to_curated,
        aws_conn_id='aws',
        dag=dag
    )

    step_checker_processing_to_curated = EmrStepSensor(
        task_id=f'watch_task_processing_to_curated',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='task_processing_to_curated', key='return_value')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['CANCELLED', 'FAILED', 'INTERRUPTED'],
        aws_conn_id="aws",
        dag=dag
    )

################################################################# DESTRÓI CLUSTER #################################################################################

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        trigger_rule="all_done",
        aws_conn_id="aws"
    )

################################################################# RODA O CRAWLER E OBSERVA O STATUS #################################################################################

    glue_crawler = GlueCrawlerOperator(
        task_id='glue_crawler_curated',
        config={"Name": "crawler_etl_data"},
        aws_conn_id='aws',
        poll_interval=10
    )

############################################################ RODA QUERIES ATHENA ####################################################################################


    task_media_compras_athena = AthenaOperator(
        task_id='query_media_compras_athena',
        query="""
            create table if not exists media_preco_diario as 
            select 
                id,
                avg(preco) as media_preco
            from 
                delivered
            group by 
                id
            order by
                media_preco desc
        """,
        database='database_etl_data',
        output_location='s3://athena-tf-data/',
        aws_conn_id='aws'
    )


    task_total_preco_athena = AthenaOperator(
        task_id='query_total_preco_athena',
        query="""
            create table if not exists total_preco_per_name as 
            select 
                    nome,
                    sum(preco) as total_preco
            from 
                    delivered
            group by
                    nome
            order by
                total_preco desc
        """,
        database='database_etl_data',
        output_location='s3://athena-tf-data/',
        aws_conn_id='aws'
    )
    
############################################################ DEFINIÇÃO DE SEQUÊNCIA DAS TASKS DA DAG ################################################################

    (
        create_emr_cluster >> emr_create_sensor >>

        task_landing_to_processing >> step_checker_landing_to_processing >>
        
        task_processing_to_curated >> step_checker_processing_to_curated >>

        [terminate_emr_cluster, glue_crawler] >> task_dummy >>

        [task_media_compras_athena, task_total_preco_athena]

    )