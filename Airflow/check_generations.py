from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from utils.classes import Generation
from utils.functions import _check_generations, _print

with DAG(
    dag_id='check_generations',
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['de_school', 'antropova', 'final_project']
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=_print,
        op_args=["Start"]
    )

    success = PythonOperator(
        task_id='success',
        python_callable=_print,
        op_args=["Success"]
    )

    failed = PythonOperator(
        task_id='failed',
        python_callable=_print,
        op_args=["Failed"],
        trigger_rule=TriggerRule.ALL_FAILED
    )

    with TaskGroup(group_id="API") as task_group:

        check_generations = PythonOperator(
            task_id='check_generations',
            pool="Antropova_API_pool",
            priority_weight=Generation.priority_weight,
            python_callable=_check_generations,
            op_args=['{{ execution_date }}']
        )

    start >> check_generations >> [success, failed]
