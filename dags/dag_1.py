"""
#### 测试 DAG1

- task1 打印日期。
- task2 等待5秒钟。
- task3 测试传参。参数格式如下：

    ```
    {
        "a": 2,
        "b": 3,
        "action": "mul"
    }
    ```
"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils.calculate import add, mul, div


def cal(ds, **kwargs):
    print("ds: ", ds)
    print(kwargs)
    a = kwargs.get("params", {}).get("a")
    b = kwargs.get("params", {}).get("b")
    action = kwargs.get("params", {}).get("action")
    func = dict(
        add=add,
        mul=mul,
        div=div,
    ).get(action)
    if not func:
        print(f"action={action} not supported. Must be one of [add, mul, div]")
        return None
    return func(a, b)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    "test-dag-1",
    default_args=default_args,
    description="Dag-1 for test.",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(0, hour=1, minute=1),
    tags=["example", "test"],
) as dag:
    dag.doc_md = __doc__
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t3 = PythonOperator(
        task_id="calculate-test",
        python_callable=cal,
    )

[t1, t2] >> t3
