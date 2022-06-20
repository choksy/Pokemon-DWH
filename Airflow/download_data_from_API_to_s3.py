from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from utils.classes import PokemonType, Move, Species, Pokemon
from utils.functions import _fetch_api, INDIVIDUAL_SNOWPIPE_FILES_FOLDER, _print

with DAG(
    dag_id='download_data_from_API_to_s3',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['de_school', 'antropova', 'final_project']
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=_print,
        op_args=["Gotta Catch 'Em All!"]
    )

    success = PythonOperator(
        task_id='success',
        python_callable=_print,
        op_args=["Gotcha!"]
    )

    failed = PythonOperator(
        task_id='failed',
        python_callable=_print,
        op_args=["Bad luck =("],
        trigger_rule=TriggerRule.ALL_FAILED
    )

    with TaskGroup(group_id="API") as task_group:

        for endpoint in [PokemonType, Move, Species, Pokemon]:

            fetch_API = PythonOperator(
                task_id=f"fetch_API_{endpoint.name}",
                pool="Antropova_API_pool",
                priority_weight=endpoint.priority_weight,
                python_callable=_fetch_api,
                op_args=[endpoint, f"{INDIVIDUAL_SNOWPIPE_FILES_FOLDER}{endpoint.name}.json"]
            )
            start >> fetch_API >> [success, failed]
